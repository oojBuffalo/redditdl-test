-- RedditDL SQLite Database Schema
-- Version: 1.0
-- Description: State management for RedditDL sessions

-- Sessions table stores information about scraping sessions
CREATE TABLE IF NOT EXISTS sessions (
    id TEXT PRIMARY KEY,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    config_hash TEXT NOT NULL,
    target_type TEXT NOT NULL, -- 'user', 'subreddit', 'url'
    target_value TEXT NOT NULL, -- username, subreddit name, or URL
    status TEXT NOT NULL DEFAULT 'active', -- 'active', 'completed', 'failed', 'paused'
    total_posts INTEGER DEFAULT 0,
    processed_posts INTEGER DEFAULT 0,
    successful_downloads INTEGER DEFAULT 0,
    failed_downloads INTEGER DEFAULT 0,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    metadata TEXT, -- JSON metadata for additional session info
    UNIQUE(config_hash, target_type, target_value)
);

-- Posts table stores discovered posts for each session
CREATE TABLE IF NOT EXISTS posts (
    id TEXT PRIMARY KEY, -- Reddit post ID
    session_id TEXT NOT NULL,
    discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    post_data TEXT NOT NULL, -- JSON serialized PostMetadata
    status TEXT NOT NULL DEFAULT 'pending', -- 'pending', 'processed', 'skipped', 'failed'
    processing_attempts INTEGER DEFAULT 0,
    last_attempt_at TIMESTAMP,
    error_message TEXT,
    FOREIGN KEY (session_id) REFERENCES sessions(id) ON DELETE CASCADE
);

-- Downloads table tracks individual file downloads
CREATE TABLE IF NOT EXISTS downloads (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    post_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    url TEXT NOT NULL,
    local_path TEXT,
    filename TEXT,
    file_size INTEGER,
    status TEXT NOT NULL DEFAULT 'pending', -- 'pending', 'downloading', 'completed', 'failed'
    download_attempts INTEGER DEFAULT 0,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    error_message TEXT,
    checksum TEXT, -- For integrity verification
    FOREIGN KEY (post_id) REFERENCES posts(id) ON DELETE CASCADE,
    FOREIGN KEY (session_id) REFERENCES sessions(id) ON DELETE CASCADE
);

-- Metadata table for storing additional key-value data
CREATE TABLE IF NOT EXISTS metadata (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL,
    key TEXT NOT NULL,
    value TEXT,
    type TEXT DEFAULT 'string', -- 'string', 'json', 'number', 'boolean'
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (session_id) REFERENCES sessions(id) ON DELETE CASCADE,
    UNIQUE(session_id, key)
);

-- Indexes for performance optimization
CREATE INDEX IF NOT EXISTS idx_sessions_status ON sessions(status);
CREATE INDEX IF NOT EXISTS idx_sessions_target ON sessions(target_type, target_value);
CREATE INDEX IF NOT EXISTS idx_sessions_created_at ON sessions(created_at);

CREATE INDEX IF NOT EXISTS idx_posts_session_id ON posts(session_id);
CREATE INDEX IF NOT EXISTS idx_posts_status ON posts(status);
CREATE INDEX IF NOT EXISTS idx_posts_discovered_at ON posts(discovered_at);

CREATE INDEX IF NOT EXISTS idx_downloads_post_id ON downloads(post_id);
CREATE INDEX IF NOT EXISTS idx_downloads_session_id ON downloads(session_id);
CREATE INDEX IF NOT EXISTS idx_downloads_status ON downloads(status);
CREATE INDEX IF NOT EXISTS idx_downloads_url ON downloads(url);

CREATE INDEX IF NOT EXISTS idx_metadata_session_id ON metadata(session_id);
CREATE INDEX IF NOT EXISTS idx_metadata_key ON metadata(key);

-- Triggers to automatically update timestamps
CREATE TRIGGER IF NOT EXISTS update_sessions_timestamp 
AFTER UPDATE ON sessions
BEGIN
    UPDATE sessions SET updated_at = CURRENT_TIMESTAMP WHERE id = NEW.id;
END;

-- Trigger to update session post counts
CREATE TRIGGER IF NOT EXISTS update_session_post_count
AFTER INSERT ON posts
BEGIN
    UPDATE sessions 
    SET total_posts = (
        SELECT COUNT(*) FROM posts WHERE session_id = NEW.session_id
    )
    WHERE id = NEW.session_id;
END;

-- Trigger to update session download counts
CREATE TRIGGER IF NOT EXISTS update_session_download_count
AFTER UPDATE OF status ON downloads
WHEN NEW.status IN ('completed', 'failed')
BEGIN
    UPDATE sessions 
    SET 
        successful_downloads = (
            SELECT COUNT(*) FROM downloads 
            WHERE session_id = NEW.session_id AND status = 'completed'
        ),
        failed_downloads = (
            SELECT COUNT(*) FROM downloads 
            WHERE session_id = NEW.session_id AND status = 'failed'
        )
    WHERE id = NEW.session_id;
END;