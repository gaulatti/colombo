ALTER TABLE tenants ADD COLUMN revalidate_after_seconds INTEGER NULL CHECK (revalidate_after_seconds > 0);
