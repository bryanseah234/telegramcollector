-- init-db.sql
-- Database initialization script
-- Runs once when PostgreSQL container is first created

-- Enable pgvector extension for vector similarity search
CREATE EXTENSION IF NOT EXISTS vector;

-- ============================================
-- Table: telegram_accounts
-- ============================================
-- Tracks Telegram user accounts added to the system
CREATE TABLE IF NOT EXISTS telegram_accounts (
    id SERIAL PRIMARY KEY,
    phone_number VARCHAR(20) UNIQUE NOT NULL,
    session_file_path VARCHAR(255) NOT NULL,
    status VARCHAR(20) DEFAULT 'active' CHECK (status IN ('active', 'paused', 'error')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_active TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_accounts_status ON telegram_accounts(status);
CREATE INDEX idx_accounts_last_active ON telegram_accounts(last_active DESC);

-- ============================================
-- Table: telegram_topics
-- ============================================
-- Stores information about forum topics representing identities
CREATE TABLE IF NOT EXISTS telegram_topics (
    id SERIAL PRIMARY KEY,
    topic_id BIGINT UNIQUE NOT NULL,           -- Telegram forum topic ID
    label VARCHAR(255) DEFAULT 'Unknown Person',
    face_count INTEGER DEFAULT 0,              -- Number of face embeddings
    message_count INTEGER DEFAULT 0,           -- Number of uploaded messages
    exemplar_image_url TEXT,                   -- URL to representative image
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_topics_label ON telegram_topics(label);
CREATE INDEX idx_topics_updated_at ON telegram_topics(updated_at DESC);
CREATE INDEX idx_topics_face_count ON telegram_topics(face_count DESC);

-- ============================================
-- Table: face_embeddings
-- ============================================
-- Stores face embedding vectors with metadata
CREATE TABLE IF NOT EXISTS face_embeddings (
    id SERIAL PRIMARY KEY,
    topic_id INTEGER REFERENCES telegram_topics(id) ON DELETE CASCADE,
    embedding vector(512) NOT NULL,
    source_chat_id BIGINT NOT NULL,
    source_message_id BIGINT NOT NULL,
    frame_index INTEGER DEFAULT 0,                -- Frame index for video sources
    quality_score REAL DEFAULT 0.0 CHECK (quality_score >= 0.0 AND quality_score <= 1.0),
    is_representative BOOLEAN DEFAULT FALSE,     -- Highest quality exemplar
    detection_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_embeddings_topic ON face_embeddings(topic_id);
CREATE INDEX idx_embeddings_source ON face_embeddings(source_chat_id, source_message_id);
CREATE INDEX idx_embeddings_quality ON face_embeddings(quality_score DESC);
CREATE INDEX idx_embeddings_timestamp ON face_embeddings(detection_timestamp DESC);

-- Create vector similarity search index
-- This index dramatically speeds up nearest neighbor queries
-- The lists parameter controls index granularity
-- 1000 lists works well for databases with up to several million vectors
CREATE INDEX idx_embeddings_vector ON face_embeddings 
USING ivfflat (embedding vector_cosine_ops) 
WITH (lists = 1000);

-- ============================================
-- Table: scan_checkpoints
-- ============================================
-- Tracks scanning progress for resumable processing
CREATE TABLE IF NOT EXISTS scan_checkpoints (
    id SERIAL PRIMARY KEY,
    account_id INTEGER REFERENCES telegram_accounts(id) ON DELETE CASCADE,
    chat_id BIGINT NOT NULL,
    chat_title VARCHAR(255),
    last_processed_message_id BIGINT,
    total_messages INTEGER DEFAULT 0,
    processed_messages INTEGER DEFAULT 0,
    scan_mode VARCHAR(20) DEFAULT 'backfill' CHECK (scan_mode IN ('backfill', 'realtime')),
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(account_id, chat_id)
);

CREATE INDEX idx_checkpoints_account ON scan_checkpoints(account_id);
CREATE INDEX idx_checkpoints_mode ON scan_checkpoints(scan_mode);
CREATE INDEX idx_checkpoints_last_updated ON scan_checkpoints(last_updated DESC);

-- ============================================
-- Table: uploaded_media
-- ============================================
-- Tracks which media has been uploaded to which topics
-- Prevents duplicate uploads during restart/recovery
CREATE TABLE IF NOT EXISTS uploaded_media (
    id SERIAL PRIMARY KEY,
    topic_id INTEGER REFERENCES telegram_topics(id) ON DELETE CASCADE,
    source_message_id BIGINT NOT NULL,
    source_chat_id BIGINT NOT NULL,
    hub_message_id BIGINT NOT NULL,            -- Message ID in the hub group
    uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source_chat_id, source_message_id)  -- Prevent duplicate uploads
);

CREATE INDEX idx_uploaded_source ON uploaded_media(source_chat_id, source_message_id);
CREATE INDEX idx_uploaded_topic ON uploaded_media(topic_id);

-- ============================================
-- Table: processed_media
-- ============================================
-- Tracks file_unique_id to prevent processing forwarded/duplicate media
-- Telegram assigns the same file_unique_id to forwarded media
CREATE TABLE IF NOT EXISTS processed_media (
    id SERIAL PRIMARY KEY,
    file_unique_id VARCHAR(255) UNIQUE NOT NULL,  -- Telegram's unique file identifier
    media_type VARCHAR(20) NOT NULL,              -- 'photo', 'video', 'document'
    first_seen_chat_id BIGINT NOT NULL,
    first_seen_message_id BIGINT NOT NULL,
    faces_found INTEGER DEFAULT 0,
    topics_matched TEXT[],                        -- Array of matched topic IDs
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_processed_media_file ON processed_media(file_unique_id);
CREATE INDEX idx_processed_media_time ON processed_media(processed_at DESC);

-- ============================================
-- Table: processing_metrics
-- ============================================
-- Stores time-series metrics for dashboard display
CREATE TABLE IF NOT EXISTS processing_metrics (
    id SERIAL PRIMARY KEY,
    metric_name VARCHAR(100) NOT NULL,
    metric_value BIGINT NOT NULL,
    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_metrics_name_time ON processing_metrics(metric_name, recorded_at DESC);

-- ============================================
-- Table: processing_errors
-- ============================================
-- Logs errors for dashboard display and debugging
CREATE TABLE IF NOT EXISTS processing_errors (
    id SERIAL PRIMARY KEY,
    error_type VARCHAR(100) NOT NULL,
    error_message TEXT NOT NULL,
    error_context JSONB,
    error_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_errors_timestamp ON processing_errors(error_timestamp DESC);
CREATE INDEX idx_errors_type ON processing_errors(error_type);

-- ============================================
-- Table: processed_users
-- ============================================
-- Tracks users whose profile photos have been scanned
-- Uses photo_id to detect when a user changes their profile photo
CREATE TABLE IF NOT EXISTS processed_users (
    user_id BIGINT PRIMARY KEY,
    photo_id BIGINT NOT NULL,                         -- Telegram's photo_id, changes when user updates photo
    last_scan TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    topic_id INTEGER REFERENCES telegram_topics(id),  -- Which identity this user's face matched to
    quality_score REAL DEFAULT 0.0
);

CREATE INDEX idx_processed_users_photo ON processed_users(photo_id);
CREATE INDEX idx_processed_users_topic ON processed_users(topic_id);

-- ============================================
-- Table: health_checks
-- ============================================
-- Stores periodic health check results for dashboard display
CREATE TABLE IF NOT EXISTS health_checks (
    id SERIAL PRIMARY KEY,
    check_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    database_ok BOOLEAN DEFAULT FALSE,
    telegram_ok BOOLEAN DEFAULT FALSE,
    face_model_ok BOOLEAN DEFAULT FALSE,
    hub_access_ok BOOLEAN DEFAULT FALSE,
    queue_size INTEGER DEFAULT 0,
    workers_active INTEGER DEFAULT 0,
    error_message TEXT,
    response_time_ms INTEGER DEFAULT 0
);

CREATE INDEX idx_health_checks_time ON health_checks(check_time DESC);

-- ============================================
-- Table: excluded_chats
-- ============================================
-- Tracks chats that should NOT be scanned (user opt-out)
CREATE TABLE IF NOT EXISTS excluded_chats (
    id SERIAL PRIMARY KEY,
    account_id INTEGER REFERENCES telegram_accounts(id) ON DELETE CASCADE,
    chat_id BIGINT NOT NULL,
    chat_title VARCHAR(255),
    excluded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    reason VARCHAR(255),
    UNIQUE(account_id, chat_id)
);

CREATE INDEX idx_excluded_account ON excluded_chats(account_id);

-- ============================================
-- Maintenance Functions
-- ============================================

-- Function to clean up old metrics (keep last 30 days)
CREATE OR REPLACE FUNCTION cleanup_old_metrics()
RETURNS void AS $$
BEGIN
    DELETE FROM processing_metrics 
    WHERE recorded_at < NOW() - INTERVAL '30 days';
END;
$$ LANGUAGE plpgsql;

-- Function to update topic statistics
CREATE OR REPLACE FUNCTION update_topic_stats(p_topic_id INTEGER)
RETURNS void AS $$
BEGIN
    UPDATE telegram_topics t
    SET 
        face_count = (
            SELECT COUNT(*) FROM face_embeddings WHERE topic_id = t.id
        ),
        message_count = (
            SELECT COUNT(*) FROM uploaded_media WHERE topic_id = t.id
        ),
        updated_at = NOW()
    WHERE t.id = p_topic_id;
END;
$$ LANGUAGE plpgsql;

-- Grant necessary permissions
-- In production, you might create a separate application user
-- with limited permissions instead of using the postgres superuser
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO postgres;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO postgres;
