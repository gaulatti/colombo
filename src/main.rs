use std::{process::ExitCode, sync::Arc};

use anyhow::{Context, Result};
use colombo::{
    cms::CmsClient,
    config::Config,
    db, ftp,
    http::{self, AppState},
    metrics::Metrics,
    upload::UploadService,
};

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "colombo=info,libunftp=info".into()),
        )
        .json()
        .init();
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            tracing::error!(error = ?err, "Colombo stopped");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<()> {
    let config = Config::load()?;
    tracing::info!(config = ?config, "starting Colombo");
    let pool = db::connect(&config)
        .await
        .context("database startup failed")?;
    let metrics = Metrics::new(&config.build_version)?;
    let cms = CmsClient::new(metrics.clone())?;
    let uploads = UploadService::new(cms.clone(), metrics.clone());
    let state = AppState {
        pool: pool.clone(),
        cms: cms.clone(),
        uploads: uploads.clone(),
        metrics: metrics.clone(),
        metrics_token: Arc::from(config.metrics_token.clone().unwrap_or_default()),
    };
    if config.ftp_enabled {
        tokio::try_join!(
            http::serve(config.http_port, state),
            ftp::serve(config, pool, cms, uploads, metrics)
        )?;
    } else {
        http::serve(config.http_port, state).await?;
    }
    Ok(())
}
