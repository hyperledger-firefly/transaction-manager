BEGIN;
CREATE INDEX IF NOT EXISTS transactions_status_seq ON transactions(status, seq);
COMMIT;
