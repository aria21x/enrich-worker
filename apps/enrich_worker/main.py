import asyncio
import json
import logging
from datetime import datetime, timezone

from core.utils.logging import setup_logging
from core.enrichers.enhanced_tx import EnhancedTxClient
from core.parsers.tx_normalizer import normalize_enhanced_tx
from core.db.postgres import execute, fetch_one
from core.db.redis import enqueue_wallet_for_scoring
from core.intelligence.entity_resolver import sync_label_book_to_db, touch_wallet_entity
from core.intelligence.wallet_profiler import upsert_wallet_trade
from core.intelligence.cluster_builder import update_edges_for_wallet, cluster_count_for_token
from core.intelligence.funder_tracker import maybe_record_funder, maybe_backfill_funder
from core.intelligence.launch_detector import update_launch_state
from core.intelligence.unknown_programs import record_unknown_programs
from core.intelligence.deployer_tracker import maybe_record_deployer
from core.services.replay_service import get_unprocessed_signatures, mark_processed, release_signature
from core.config.settings import settings
from core.scoring.wallet_score import recompute_wallet_score

logger = logging.getLogger(__name__)


def ts_to_dt(ts):
    if ts is None:
        return None
    if isinstance(ts, datetime):
        return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
    return datetime.fromtimestamp(ts, tz=timezone.utc)


def token_buy_velocity(mint: str, ref_time=None) -> int:
    ref_time = ref_time or datetime.now(timezone.utc)
    row = fetch_one("SELECT COUNT(*) AS c FROM trades WHERE mint = %s AND side = 'buy' AND block_time BETWEEN %s - INTERVAL '10 minutes' AND %s", (mint, ref_time, ref_time))
    return int(row['c']) if row else 0


def token_unique_buyers(mint: str, ref_time=None) -> int:
    ref_time = ref_time or datetime.now(timezone.utc)
    row = fetch_one("SELECT COUNT(DISTINCT wallet_address) AS c FROM trades WHERE mint = %s AND side = 'buy' AND block_time BETWEEN %s - INTERVAL '20 minutes' AND %s", (mint, ref_time, ref_time))
    return int(row['c']) if row else 0


def compute_alert_priority(event: dict, wallet_score: float, clusters: int, velocity: int, buyers: int, launch: dict) -> float:
    return round(
        (float(event.get('confidence') or 0) * 35.0)
        + min(wallet_score, 100.0) * 0.35
        + min(clusters, 10) * 2.0
        + min(velocity, 10) * 1.5
        + min(buyers, 10) * 1.5
        + float(launch.get('launch_confidence') or 0) * 20.0,
        2,
    )


async def _process_signature(sig: str, tx: dict) -> None:
    event_time = ts_to_dt(tx.get('timestamp'))
    execute(
        "INSERT INTO raw_transactions (signature, slot, block_time, payload_json) VALUES (%s, %s, %s, %s::jsonb) ON CONFLICT (signature) DO UPDATE SET slot = COALESCE(EXCLUDED.slot, raw_transactions.slot), block_time = COALESCE(EXCLUDED.block_time, raw_transactions.block_time), payload_json = EXCLUDED.payload_json",
        (sig, tx.get('slot'), event_time, json.dumps(tx)),
    )
    events = normalize_enhanced_tx(tx)
    record_unknown_programs(tx, events)
    if not events:
        mark_processed(sig)
        return

    for event in events:
        event['block_time'] = ts_to_dt(event['block_time'])
        maybe_record_funder(event['wallet_address'], tx)
        if settings.ENABLE_ADDRESS_HISTORY_ENRICHMENT:
            await maybe_backfill_funder(event['wallet_address'])
        if event.get('venue') and event.get('venue') != 'unknown':
            touch_wallet_entity(event['wallet_address'], None, 'trader', 0.25, 'trade_seen')
        inserted = execute(
            "INSERT INTO trades (signature, wallet_address, mint, symbol, side, token_amount, usd_value, sol_value, source_program, tx_type, confidence, venue, trade_path, route_hops, block_time) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
            (event['signature'], event['wallet_address'], event['mint'], event.get('symbol'), event['side'], event['token_amount'], event['usd_value'], event['sol_value'], event['source_program'], event['tx_type'], event.get('confidence', 0), event.get('venue'), event.get('trade_path'), event.get('route_hops'), event['block_time']),
        )
        if not inserted:
            continue
        token_seen_at = event['block_time'] or datetime.now(timezone.utc)
        execute(
            """
            INSERT INTO tokens (mint, symbol, name, first_seen_at, mention_count, buy_count, sell_count, total_buy_usd, total_sell_usd, last_seen_at, updated_at, venue, first_source_program)
            VALUES (%s, %s, %s, %s, 1, %s, %s, %s, %s, %s, NOW(), %s, %s)
            ON CONFLICT (mint) DO UPDATE SET
                symbol = COALESCE(EXCLUDED.symbol, tokens.symbol),
                mention_count = tokens.mention_count + 1,
                buy_count = tokens.buy_count + EXCLUDED.buy_count,
                sell_count = tokens.sell_count + EXCLUDED.sell_count,
                total_buy_usd = tokens.total_buy_usd + EXCLUDED.total_buy_usd,
                total_sell_usd = tokens.total_sell_usd + EXCLUDED.total_sell_usd,
                unique_buyers = (SELECT COUNT(DISTINCT wallet_address) FROM trades WHERE trades.mint = EXCLUDED.mint AND side = 'buy'),
                venue = COALESCE(tokens.venue, EXCLUDED.venue),
                first_source_program = COALESCE(tokens.first_source_program, EXCLUDED.first_source_program),
                first_seen_at = LEAST(tokens.first_seen_at, EXCLUDED.first_seen_at),
                last_seen_at = GREATEST(tokens.last_seen_at, EXCLUDED.last_seen_at),
                updated_at = NOW()
            """,
            (event['mint'], event.get('symbol'), event.get('symbol'), token_seen_at, 1 if event['side'] == 'buy' else 0, 1 if event['side'] == 'sell' else 0, event['usd_value'] if event['side'] == 'buy' else 0, event['usd_value'] if event['side'] == 'sell' else 0, token_seen_at, event.get('venue'), event['source_program']),
        )
        maybe_record_deployer(event['mint'], tx, event)
        upsert_wallet_trade(event)
        update_edges_for_wallet(event['wallet_address'], event['mint'], event['block_time'])
        wallet_score = recompute_wallet_score(event['wallet_address'])
        enqueue_wallet_for_scoring(event['wallet_address'])
        launch = update_launch_state(event['mint'], event.get('venue'), event['source_program'], event['side'] == 'buy', event['block_time'])
        if event['side'] == 'buy':
            velocity = token_buy_velocity(event['mint'], event['block_time'])
            clusters = cluster_count_for_token(event['mint'], event['block_time'])
            buyers = token_unique_buyers(event['mint'], event['block_time'])
            priority = compute_alert_priority(event, wallet_score, clusters, velocity, buyers, launch)
            execute(
                "INSERT INTO alerts_queue (signature, wallet_address, mint, symbol, side, usd_value, confidence, wallet_score, cluster_count, token_buy_velocity, token_unique_buyers, launch_stage, launch_confidence, venue, priority, reason) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
                (event['signature'], event['wallet_address'], event['mint'], event.get('symbol'), event['side'], event['usd_value'], event.get('confidence', 0), wallet_score, clusters, velocity, buyers, launch['launch_stage'], launch['launch_confidence'], event.get('venue'), priority, f"buy_conf={event.get('confidence', 0):.2f};normalizer={event.get('normalizer', 'unknown')};path={event.get('trade_path', 'unknown')};venue={event.get('venue', 'unknown')}"),
            )
    mark_processed(sig)


async def main() -> None:
    setup_logging()
    sync_label_book_to_db()
    client = EnhancedTxClient()
    while True:
        rows = get_unprocessed_signatures(settings.SIGNATURE_BATCH_SIZE)
        if not rows:
            await asyncio.sleep(settings.ENRICH_WORKER_POLL_SECONDS)
            continue
        signatures = [r['signature'] for r in rows]
        try:
            txs = await client.fetch(signatures)
            tx_by_sig = {tx.get('signature'): tx for tx in txs}
        except Exception as exc:
            logger.exception('enrich fetch failed: %s', exc)
            for sig in signatures:
                release_signature(sig, str(exc))
            await asyncio.sleep(2)
            continue

        for row in rows:
            sig = row['signature']
            tx = tx_by_sig.get(sig)
            if not tx:
                release_signature(sig, 'not found')
                continue
            try:
                await _process_signature(sig, tx)
            except Exception as exc:
                logger.exception('failed processing signature %s: %s', sig, exc)
                release_signature(sig, str(exc))
        await asyncio.sleep(0)


if __name__ == '__main__':
    asyncio.run(main())
