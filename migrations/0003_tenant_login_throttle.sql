ALTER TABLE tenants ADD COLUMN login_failures_per_minute INTEGER NULL CHECK (login_failures_per_minute > 0);
