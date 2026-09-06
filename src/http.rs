use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use axum::{
    Json, Router,
    body::Body,
    extract::{DefaultBodyLimit, Multipart, Path as AxumPath, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::Serialize;
use sqlx::PgPool;
use subtle::ConstantTimeEq;
use tokio::io::AsyncWriteExt;
use tower_http::{catch_panic::CatchPanicLayer, trace::TraceLayer};
use uuid::Uuid;

use crate::{
    cms::{CmsClient, ValidationError},
    db,
    metrics::Metrics,
    spool::UploadState,
    upload::UploadService,
};

const MAX_UPLOAD_BYTES: usize = 100 * 1024 * 1024;

#[derive(Clone)]
pub struct AppState {
    pub pool: PgPool,
    pub cms: CmsClient,
    pub uploads: Arc<UploadService>,
    pub metrics: Arc<Metrics>,
    pub metrics_token: Arc<str>,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/", get(root))
        .route("/upload", post(upload))
        .route("/uploads/{operation_id}", get(upload_status))
        .route("/actuator/health", get(health))
        .route("/actuator/prometheus", get(prometheus))
        .fallback(|| async { StatusCode::UNAUTHORIZED })
        .layer(DefaultBodyLimit::max(MAX_UPLOAD_BYTES))
        .layer(CatchPanicLayer::new())
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

async fn root() -> Response {
    Response::builder()
        .status(StatusCode::FOUND)
        .header(
            header::LOCATION,
            "https://www.youtube.com/watch?v=KieE_MLv-ZY",
        )
        .body(Body::empty())
        .expect("static redirect response")
}

pub async fn serve(port: u16, state: AppState) -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port)).await?;
    tracing::info!(port, "HTTP listener ready");
    axum::serve(listener, router(state))
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

#[derive(Serialize)]
struct Accepted {
    status: &'static str,
    assignment_id: String,
    operation_id: Uuid,
}

async fn upload(
    State(state): State<AppState>,
    headers: HeaderMap,
    mut multipart: Multipart,
) -> Response {
    let username = match required_header(&headers, "X-Colombo-Username") {
        Some(value) if !value.trim().is_empty() => value,
        _ => {
            return error(
                StatusCode::BAD_REQUEST,
                "X-Colombo-Username header is required",
            );
        }
    };
    let password = match required_header(&headers, "X-Colombo-Password") {
        Some(value) => value,
        None => {
            return error(
                StatusCode::BAD_REQUEST,
                "X-Colombo-Password header is required",
            );
        }
    };
    let tenant = match db::tenant_by_username(&state.pool, &username).await {
        Ok(Some(value)) => value,
        Ok(None) => return error(StatusCode::NOT_FOUND, "No tenant registered for username"),
        Err(err) => {
            tracing::error!(error = %err, "tenant lookup failed");
            return error(StatusCode::INTERNAL_SERVER_ERROR, "Tenant lookup failed");
        }
    };
    let session = match state
        .cms
        .validate(&tenant, &password, "validation_upload")
        .await
    {
        Ok(value) => {
            state
                .metrics
                .authentication_attempts
                .with_label_values(&["http_upload", "success"])
                .inc();
            value
        }
        Err(ValidationError::Denied) => {
            state
                .metrics
                .authentication_attempts
                .with_label_values(&["http_upload", "denied"])
                .inc();
            return error(StatusCode::UNAUTHORIZED, "Invalid credentials");
        }
        Err(err) => {
            state
                .metrics
                .authentication_attempts
                .with_label_values(&["http_upload", "unavailable"])
                .inc();
            tracing::warn!(error = %err, "HTTP upload authentication failed");
            return error(StatusCode::UNAUTHORIZED, "Invalid credentials");
        }
    };
    let mut accepted: Option<(String, PathBuf)> = None;
    loop {
        let field = match multipart.next_field().await {
            Ok(Some(value)) => value,
            Ok(None) => break,
            Err(_) => return error(StatusCode::BAD_REQUEST, "Invalid multipart request"),
        };
        if field.name() != Some("file") || accepted.is_some() {
            continue;
        }
        let original = field
            .file_name()
            .filter(|v| !v.trim().is_empty())
            .and_then(|v| Path::new(v).file_name())
            .and_then(|v| v.to_str())
            .unwrap_or("upload")
            .to_owned();
        let temp = match tempfile::Builder::new()
            .prefix("colombo-upload-")
            .tempfile()
        {
            Ok(value) => value,
            Err(err) => {
                tracing::error!(error = %err, "temp upload creation failed");
                return error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Failed to process uploaded file",
                );
            }
        };
        let path = temp
            .into_temp_path()
            .keep()
            .unwrap_or_else(|e| e.path.to_path_buf());
        let mut output = match tokio::fs::File::create(&path).await {
            Ok(value) => value,
            Err(_) => {
                return error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Failed to process uploaded file",
                );
            }
        };
        let mut total = 0usize;
        let mut field = field;
        loop {
            match field.chunk().await {
                Ok(Some(chunk)) => {
                    total += chunk.len();
                    if total > MAX_UPLOAD_BYTES || output.write_all(&chunk).await.is_err() {
                        let _ = tokio::fs::remove_file(&path).await;
                        return error(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "Failed to process uploaded file",
                        );
                    }
                }
                Ok(None) => break,
                Err(_) => {
                    let _ = tokio::fs::remove_file(&path).await;
                    return error(StatusCode::BAD_REQUEST, "Invalid multipart request");
                }
            }
        }
        if total == 0 {
            let _ = tokio::fs::remove_file(&path).await;
            return error(
                StatusCode::BAD_REQUEST,
                "file part is required and must not be empty",
            );
        }
        if output.sync_all().await.is_err() {
            let _ = tokio::fs::remove_file(&path).await;
            return error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to durably store uploaded file",
            );
        }
        drop(output);
        accepted = Some((original, path));
    }
    let Some((original, path)) = accepted else {
        return error(
            StatusCode::BAD_REQUEST,
            "file part is required and must not be empty",
        );
    };
    let assignment_id = session.assignment_id.clone();
    let receipt = match state
        .uploads
        .accept_http(session, original, path.clone())
        .await
    {
        Ok(value) => value,
        Err(err) => {
            tracing::error!(error = %err, "durable HTTP upload acceptance failed");
            let _ = tokio::fs::remove_file(&path).await;
            return error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to durably store uploaded file",
            );
        }
    };
    let _ = tokio::fs::remove_file(&path).await;
    (
        StatusCode::ACCEPTED,
        Json(Accepted {
            status: "accepted",
            assignment_id,
            operation_id: receipt.operation_id,
        }),
    )
        .into_response()
}

async fn upload_status(
    State(state): State<AppState>,
    AxumPath(operation_id): AxumPath<String>,
    headers: HeaderMap,
) -> Response {
    let Some(username) =
        required_header(&headers, "X-Colombo-Username").filter(|value| !value.trim().is_empty())
    else {
        return error(
            StatusCode::BAD_REQUEST,
            "X-Colombo-Username header is required",
        );
    };
    let Some(password) = required_header(&headers, "X-Colombo-Password") else {
        return error(
            StatusCode::BAD_REQUEST,
            "X-Colombo-Password header is required",
        );
    };
    let tenant = match db::tenant_by_username(&state.pool, &username).await {
        Ok(Some(value)) => value,
        Ok(None) => return error(StatusCode::UNAUTHORIZED, "Invalid credentials"),
        Err(err) => {
            tracing::error!(error = %err, "status tenant lookup failed");
            return error(StatusCode::INTERNAL_SERVER_ERROR, "Tenant lookup failed");
        }
    };
    let authenticated = match state
        .cms
        .validate(&tenant, &password, "validation_status")
        .await
    {
        Ok(value) => value,
        Err(ValidationError::Denied) => {
            return error(StatusCode::UNAUTHORIZED, "Invalid credentials");
        }
        Err(err) => {
            tracing::warn!(error = %err, "upload status authentication failed");
            return error(
                StatusCode::SERVICE_UNAVAILABLE,
                "Authentication unavailable",
            );
        }
    };
    let Ok(operation_id) = Uuid::parse_str(&operation_id) else {
        return error(StatusCode::NOT_FOUND, "Upload operation not found");
    };
    match state
        .uploads
        .receipt_for_assignment(operation_id, tenant.id, authenticated.assignment_id)
        .await
    {
        Ok(Some(receipt)) if receipt.state == UploadState::Expired => {
            (StatusCode::GONE, Json(receipt)).into_response()
        }
        Ok(Some(receipt)) => (StatusCode::OK, Json(receipt)).into_response(),
        Ok(None) => error(StatusCode::NOT_FOUND, "Upload operation not found"),
        Err(err) => {
            tracing::error!(error = %err, "upload receipt lookup failed");
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Upload receipt lookup failed",
            )
        }
    }
}

fn required_header(headers: &HeaderMap, name: &'static str) -> Option<String> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned)
}

#[derive(Serialize)]
struct ErrorBody<'a> {
    error: &'a str,
}
fn error(status: StatusCode, message: &str) -> Response {
    (status, Json(ErrorBody { error: message })).into_response()
}

async fn health(State(state): State<AppState>) -> Response {
    if sqlx::query_scalar::<_, i32>("SELECT 1")
        .fetch_one(&state.pool)
        .await
        .is_ok()
    {
        (StatusCode::OK, Json(serde_json::json!({"status":"UP"}))).into_response()
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"status":"DOWN"})),
        )
            .into_response()
    }
}

async fn prometheus(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let supplied = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .unwrap_or("");
    if !metrics_authorized(&state.metrics_token, supplied) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    match state.metrics.encode() {
        Ok(body) => Response::builder()
            .status(StatusCode::OK)
            .header(
                header::CONTENT_TYPE,
                "text/plain; version=0.0.4; charset=utf-8",
            )
            .body(Body::from(body))
            .unwrap(),
        Err(err) => {
            tracing::error!(error = %err, "metrics encoding failed");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

fn metrics_authorized(expected: &str, supplied: &str) -> bool {
    !expected.is_empty() && supplied.as_bytes().ct_eq(expected.as_bytes()).unwrap_u8() == 1
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn root_preserves_v1_redirect_contract() {
        let response = root().await;
        assert_eq!(response.status(), StatusCode::FOUND);
        assert_eq!(
            response.headers()[header::LOCATION],
            "https://www.youtube.com/watch?v=KieE_MLv-ZY"
        );
    }

    #[test]
    fn required_header_rejects_missing_and_invalid_values() {
        let mut headers = HeaderMap::new();
        assert!(required_header(&headers, "X-Colombo-Username").is_none());
        headers.insert("X-Colombo-Username", "photographer".parse().unwrap());
        assert_eq!(
            required_header(&headers, "X-Colombo-Username").as_deref(),
            Some("photographer")
        );
    }

    #[test]
    fn metrics_auth_fails_closed_without_a_token() {
        assert!(!metrics_authorized("", ""));
        assert!(!metrics_authorized("configured-long-token", "wrong"));
        assert!(metrics_authorized(
            "configured-long-token",
            "configured-long-token"
        ));
    }
}
