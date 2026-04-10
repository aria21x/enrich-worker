-- Create address_entities table (the one causing your error)
CREATE TABLE IF NOT EXISTS address_entities (
    address VARCHAR(100) PRIMARY KEY,
    label VARCHAR(255),
    entity_type VARCHAR(50),
    confidence FLOAT,
    source VARCHAR(50),
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- Create indexes
CREATE INDEX idx_address_entities_label ON address_entities(label);
CREATE INDEX idx_address_entities_type ON address_entities(entity_type);

-- Create raw_signatures table
CREATE TABLE IF NOT EXISTS raw_signatures (
    id INT AUTO_INCREMENT PRIMARY KEY,
    signature VARCHAR(100) NOT NULL,
    source VARCHAR(50),
    received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed BOOLEAN DEFAULT FALSE,
    INDEX idx_signature (signature),
    INDEX idx_processed (processed)
);

-- Create enriched_transactions table
CREATE TABLE IF NOT EXISTS enriched_transactions (
    id INT AUTO_INCREMENT PRIMARY KEY,
    signature VARCHAR(100) NOT NULL,
    data JSON,
    enriched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_signature (signature)
);

-- Create wallet_scores table
CREATE TABLE IF NOT EXISTS wallet_scores (
    wallet VARCHAR(100) PRIMARY KEY,
    score FLOAT DEFAULT 0,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- Create token_state table
CREATE TABLE IF NOT EXISTS token_state (
    token_address VARCHAR(100) PRIMARY KEY,
    data JSON,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- Create alert_rules table
CREATE TABLE IF NOT EXISTS alert_rules (
    id INT AUTO_INCREMENT PRIMARY KEY,
    rule_name VARCHAR(100),
    conditions JSON,
    webhook_url VARCHAR(500),
    active BOOLEAN DEFAULT TRUE
);
