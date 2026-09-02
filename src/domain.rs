use serde::{Deserialize, Serialize};
use sqlx::FromRow;

#[derive(Clone, FromRow, Serialize)]
pub struct Tenant {
    pub id: i64,
    pub name: String,
    pub ftp_username: String,
    #[serde(skip_serializing)]
    pub api_key: String,
    pub validation_endpoint: String,
    pub photo_endpoint: String,
}

impl std::fmt::Debug for Tenant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Tenant")
            .field("id", &self.id)
            .field("name", &self.name)
            .field("ftp_username", &self.ftp_username)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UploadCredentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: String,
    pub region: String,
    pub bucket: String,
    pub key_prefix: String,
    pub expires_at: String,
    #[serde(default)]
    pub naming_policy: Option<UploadNamingPolicy>,
    #[serde(default)]
    pub sequence_endpoint: Option<String>,
}

impl std::fmt::Debug for UploadCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UploadCredentials")
            .field("region", &self.region)
            .field("bucket", &self.bucket)
            .field("key_prefix", &self.key_prefix)
            .field("expires_at", &self.expires_at)
            .finish_non_exhaustive()
    }
}

impl UploadCredentials {
    pub fn valid(&self) -> bool {
        let credentials_valid = [
            &self.access_key_id,
            &self.secret_access_key,
            &self.session_token,
            &self.region,
            &self.bucket,
            &self.key_prefix,
            &self.expires_at,
        ]
        .iter()
        .all(|v| !v.trim().is_empty());
        let naming_valid = self.naming_policy.as_ref().is_none_or(|policy| {
            policy.valid()
                && self
                    .sequence_endpoint
                    .as_deref()
                    .is_some_and(|value| !value.trim().is_empty())
        });
        credentials_valid && naming_valid
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn credentials() -> UploadCredentials {
        UploadCredentials {
            access_key_id: "a".into(),
            secret_access_key: "b".into(),
            session_token: "c".into(),
            region: "d".into(),
            bucket: "e".into(),
            key_prefix: "f".into(),
            expires_at: "g".into(),
            naming_policy: None,
            sequence_endpoint: None,
        }
    }

    #[test]
    fn credentials_require_every_base_field() {
        assert!(credentials().valid());
        let mut value = credentials();
        value.secret_access_key = " ".into();
        assert!(!value.valid());
    }

    #[test]
    fn naming_policy_requires_a_sequence_endpoint() {
        let mut value = credentials();
        value.naming_policy = Some(UploadNamingPolicy {
            version: 1,
            assignment_slug: "slug".into(),
            path: vec![],
            filename: vec![UploadNamingSegment {
                kind: "placeholder".into(),
                value: None,
                name: Some("sequence".into()),
                format: None,
                width: Some(3),
            }],
            timezone: "UTC".into(),
            capture_time_fallback: "uploadedTime".into(),
            case_mode: "preserve".into(),
        });
        assert!(!value.valid());
        value.sequence_endpoint = Some("/sequence".into());
        assert!(value.valid());
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UploadNamingPolicy {
    pub version: i32,
    pub assignment_slug: String,
    pub path: Vec<UploadNamingSegment>,
    pub filename: Vec<UploadNamingSegment>,
    pub timezone: String,
    pub capture_time_fallback: String,
    #[serde(rename = "case")]
    pub case_mode: String,
}

impl UploadNamingPolicy {
    pub fn valid(&self) -> bool {
        self.version == 1
            && !self.assignment_slug.trim().is_empty()
            && !self.filename.is_empty()
            && self
                .filename
                .iter()
                .any(|s| s.kind == "placeholder" && s.name.as_deref() == Some("sequence"))
            && !self.timezone.trim().is_empty()
            && matches!(
                self.capture_time_fallback.as_str(),
                "uploadedTime" | "reject"
            )
            && matches!(
                self.case_mode.as_str(),
                "preserve" | "lowercase" | "uppercase"
            )
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct UploadNamingSegment {
    #[serde(rename = "type")]
    pub kind: String,
    pub value: Option<String>,
    pub name: Option<String>,
    pub format: Option<String>,
    pub width: Option<usize>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ValidationResponse {
    pub assignment_id: String,
    pub upload: UploadCredentials,
}

#[derive(Clone)]
pub struct SessionData {
    pub tenant: Tenant,
    pub assignment_id: String,
    pub upload: Option<UploadCredentials>,
    pub validation_key: Option<String>,
}

impl std::fmt::Debug for SessionData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionData")
            .field("tenant", &self.tenant)
            .field("assignment_id", &self.assignment_id)
            .field("upload", &self.upload)
            .field("has_validation_key", &self.validation_key.is_some())
            .finish()
    }
}
