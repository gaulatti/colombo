-- Initial schema for Colombo.

CREATE TABLE IF NOT EXISTS tenants (
	id BIGSERIAL PRIMARY KEY,
	name VARCHAR(255) NOT NULL UNIQUE,
	ftp_username VARCHAR(255) NOT NULL UNIQUE,
	api_key VARCHAR(255) NOT NULL,
	validation_endpoint VARCHAR(1024) NOT NULL,
	photo_endpoint VARCHAR(1024) NOT NULL
);
