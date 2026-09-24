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
        "SELECT id, name, ftp_username, api_key, validation_endpoint, photo_endpoint, revalidate_after_seconds, login_failures_per_minute FROM tenants WHERE ftp_username = $1"
    ).bind(username).fetch_optional(pool).await
}

pub async fn tenant_by_id(pool: &PgPool, id: i64) -> Result<Option<Tenant>, sqlx::Error> {
    sqlx::query_as::<_, Tenant>(
        "SELECT id, name, ftp_username, api_key, validation_endpoint, photo_endpoint, revalidate_after_seconds, login_failures_per_minute FROM tenants WHERE id = $1"
    ).bind(id).fetch_optional(pool).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::migrate::Migrator;

    // Run only against a disposable PostgreSQL database. The first migrator
    // models a later migration landing before this feature's migration.
    #[tokio::test]
    #[ignore = "requires a disposable local PostgreSQL DATABASE_URL"]
    async fn tenant_revalidation_migration_applies_after_later_version() {
        let database_url = std::env::var("DATABASE_URL").unwrap();
        let pool = PgPoolOptions::new().connect(&database_url).await.unwrap();
        let directory = tempfile::tempdir().unwrap();
        let migration_path = directory.path();
        std::fs::copy(
            "migrations/0001_init.sql",
            migration_path.join("0001_init.sql"),
        )
        .unwrap();
        std::fs::write(migration_path.join("0003_future.sql"), "SELECT 1;").unwrap();
        Migrator::new(migration_path)
            .await
            .unwrap()
            .run(&pool)
            .await
            .unwrap();
        std::fs::copy(
            "migrations/0002_tenant_revalidation.sql",
            migration_path.join("0002_tenant_revalidation.sql"),
        )
        .unwrap();
        Migrator::new(migration_path)
            .await
            .unwrap()
            .run(&pool)
            .await
            .unwrap();
        let versions: Vec<i64> =
            sqlx::query_scalar("SELECT version FROM _sqlx_migrations ORDER BY version")
                .fetch_all(&pool)
                .await
                .unwrap();
        assert_eq!(versions, vec![1, 2, 3]);
        let column_exists: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'tenants' AND column_name = 'revalidate_after_seconds')",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(column_exists);
    }
}
