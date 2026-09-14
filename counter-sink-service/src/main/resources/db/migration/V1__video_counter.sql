CREATE TABLE video_counter
(
    video_id   BIGINT PRIMARY KEY,
    view_count BIGINT      NOT NULL CHECK (view_count >= 0),
    version    BIGINT      NOT NULL CHECK (version = view_count),
    updated_at TIMESTAMPTZ NOT NULL
);
