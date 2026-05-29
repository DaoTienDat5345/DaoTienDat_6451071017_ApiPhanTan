CREATE TABLE IF NOT EXISTS idempotency_keys (
    command_id VARCHAR(150) PRIMARY KEY,
    status VARCHAR(30) NOT NULL DEFAULT 'processed',
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_error TEXT
);

CREATE TABLE IF NOT EXISTS comments (
    id SERIAL PRIMARY KEY,
    event_id VARCHAR(200) UNIQUE,
    comment_id VARCHAR(200) UNIQUE,
    post_id VARCHAR(200),
    page_id VARCHAR(200),
    from_user_id VARCHAR(200),
    message TEXT,
    normalized_message TEXT,
    intent VARCHAR(50),
    sentiment VARCHAR(30),
    spam_score INTEGER DEFAULT 0,
    action VARCHAR(50),
    status VARCHAR(50) DEFAULT 'received',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS processing_logs (
    id SERIAL PRIMARY KEY,
    event_id VARCHAR(200),
    service_name VARCHAR(100),
    status VARCHAR(50),
    message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dead_letter_logs (
    id SERIAL PRIMARY KEY,
    command_id VARCHAR(150),
    event_id VARCHAR(200),
    reason TEXT,
    payload JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
