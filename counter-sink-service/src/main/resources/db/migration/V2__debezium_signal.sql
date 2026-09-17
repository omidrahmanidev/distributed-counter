CREATE TABLE debezium_signal
(
    id   VARCHAR(64) PRIMARY KEY,
    type VARCHAR(32) NOT NULL,
    data VARCHAR(2048)
);
