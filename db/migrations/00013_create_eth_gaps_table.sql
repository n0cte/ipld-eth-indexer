-- +goose Up
CREATE TABLE eth.gaps (
  id SERIAL PRIMARY KEY,
  start BIGINT NOT NULL,
  stop BIGINT NOT NULL,
  validation BOOL NOT NULL DEFAULT FALSE,
  checked_out BOOL NOT NULL DEFAULT FALSE,
  UNIQUE(start, stop)
);

-- +goose Down
DROP TABLE eth.gaps;