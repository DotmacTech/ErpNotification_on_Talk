CREATE TABLE IF NOT EXISTS user_cache (
    username TEXT PRIMARY KEY,
    room_token TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS processed_webhooks (
    payload TEXT PRIMARY KEY
);
