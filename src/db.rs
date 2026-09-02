use anyhow::Result;
use sqlx::{PgPool, postgres::PgPoolOptions};

use crate::{config::Config, domain::Tenant};

pub async fn connect(config: &Config) -> Result<PgPool> {
    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&config.postgres_url()?)
        .await?;
    if config.migrations_enabled {
        sqlx::migrate!("./migrations").run(&pool).await?;
    }
    Ok(pool)
}

pub async fn tenant_by_username(
    pool: &PgPool,
    username: &str,
) -> Result<Option<Tenant>, sqlx::Error> {
    sqlx::query_as::<_, Tenant>(
        "SELECT id, name, ftp_username, api_key, validation_endpoint, photo_endpoint FROM tenants WHERE ftp_username = $1"
    ).bind(username).fetch_optional(pool).await
}
