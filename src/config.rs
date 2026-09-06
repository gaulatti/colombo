use std::{env, net::IpAddr, path::PathBuf};

use anyhow::{Context, Result, bail};

#[derive(Clone)]
pub struct Config {
    pub http_port: u16,
    pub ftp_enabled: bool,
    pub ftp_port: u16,
    pub passive_ports: std::ops::RangeInclusive<u16>,
    pub passive_external_address: Option<String>,
    pub ftps_certificate_path: Option<PathBuf>,
    pub ftps_private_key_path: Option<PathBuf>,
    pub database_url: String,
    pub database_user: String,
    pub database_password: String,
    pub migrations_enabled: bool,
    pub master_password: Option<String>,
    pub metrics_token: Option<String>,
    pub build_version: String,
    pub spool_path: PathBuf,
    pub ftp_root: PathBuf,
}

impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config")
            .field("http_port", &self.http_port)
            .field("ftp_enabled", &self.ftp_enabled)
            .field("ftp_port", &self.ftp_port)
            .field("passive_ports", &self.passive_ports)
            .field("passive_external_address", &self.passive_external_address)
            .field("ftps_enabled", &self.ftps_certificate_path.is_some())
            .field("database_url", &redact_database_url(&self.database_url))
            .field("migrations_enabled", &self.migrations_enabled)
            .field("build_version", &self.build_version)
            .field("spool_path", &self.spool_path)
            .field("ftp_root", &self.ftp_root)
            .finish_non_exhaustive()
    }
}

impl Config {
    pub fn load() -> Result<Self> {
        let _ = dotenvy::dotenv();
        let _ = dotenvy::from_path("../.env");

        let (passive_start, passive_end) =
            parse_port_range(&value("COLOMBO_FTP_PASSIVE_PORTS", "60000-60100"))?;
        let cert = optional("COLOMBO_FTPS_CERTIFICATE_PATH").map(PathBuf::from);
        let key = optional("COLOMBO_FTPS_PRIVATE_KEY_PATH").map(PathBuf::from);
        if cert.is_some() != key.is_some() {
            bail!(
                "COLOMBO_FTPS_CERTIFICATE_PATH and COLOMBO_FTPS_PRIVATE_KEY_PATH must be set together"
            );
        }

        let metrics_token = optional("COLOMBO_METRICS_TOKEN");
        if metrics_token
            .as_deref()
            .is_some_and(|value| value.trim().len() < 16)
        {
            bail!("COLOMBO_METRICS_TOKEN must contain at least 16 non-whitespace characters");
        }

        let spool_path = optional("COLOMBO_SPOOL_PATH")
            .map(PathBuf::from)
            .unwrap_or_else(|| env::temp_dir().join("colombo-spool"));

        Ok(Self {
            http_port: parse("PORT", 8080)?,
            ftp_enabled: parse_bool_alias("COLOMBO_FTP_ENABLED", "colombo.ftp.enabled", true)?,
            ftp_port: parse("COLOMBO_FTP_PORT", 2121)?,
            passive_ports: passive_start..=passive_end,
            passive_external_address: optional("COLOMBO_FTP_PASSIVE_EXTERNAL_ADDRESS"),
            ftps_certificate_path: cert,
            ftps_private_key_path: key,
            database_url: database_url(),
            database_user: alias("DATABASE_USER", "DB_USERNAME", "colombo"),
            database_password: alias("DATABASE_PASSWORD", "DB_PASSWORD", "colombo"),
            migrations_enabled: parse_bool("FLYWAY_ENABLED", true)?,
            master_password: optional("COLOMBO_MASTER_PASSWORD")
                .or_else(|| optional("COLOMBO_DEV_PASSWORD")),
            metrics_token,
            build_version: value("COLOMBO_BUILD_VERSION", "development"),
            ftp_root: spool_path.join("ftp-incoming"),
            spool_path,
        })
    }

    pub fn postgres_url(&self) -> Result<String> {
        let raw = self
            .database_url
            .strip_prefix("jdbc:")
            .unwrap_or(&self.database_url);
        let mut parsed = url::Url::parse(raw).context("DATABASE_URL must be a PostgreSQL URL")?;
        if parsed.username().is_empty() {
            parsed
                .set_username(&self.database_user)
                .map_err(|_| anyhow::anyhow!("invalid database username"))?;
        }
        if parsed.password().is_none() {
            parsed
                .set_password(Some(&self.database_password))
                .map_err(|_| anyhow::anyhow!("invalid database password"))?;
        }
        Ok(parsed.into())
    }
}

fn database_url() -> String {
    alias(
        "DATABASE_URL",
        "DB_URL",
        "jdbc:postgresql://localhost:5432/colombo",
    )
}

fn value(name: &str, default: &str) -> String {
    optional(name).unwrap_or_else(|| default.to_owned())
}

fn alias(primary: &str, legacy: &str, default: &str) -> String {
    optional(primary)
        .or_else(|| optional(legacy))
        .unwrap_or_else(|| default.to_owned())
}

fn optional(name: &str) -> Option<String> {
    env::var(name).ok().filter(|v| !v.trim().is_empty())
}

fn parse<T>(name: &str, default: T) -> Result<T>
where
    T: std::str::FromStr + ToString,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    optional(name).map_or(Ok(default), |raw| {
        raw.parse().with_context(|| format!("invalid {name}"))
    })
}

fn parse_bool(name: &str, default: bool) -> Result<bool> {
    parse_bool_value(optional(name), name, default)
}

fn parse_bool_alias(primary: &str, legacy: &str, default: bool) -> Result<bool> {
    parse_bool_value(
        optional(primary).or_else(|| optional(legacy)),
        primary,
        default,
    )
}

fn parse_bool_value(raw: Option<String>, name: &str, default: bool) -> Result<bool> {
    match raw.as_deref().map(str::to_ascii_lowercase).as_deref() {
        None => Ok(default),
        Some("true" | "1" | "yes" | "on") => Ok(true),
        Some("false" | "0" | "no" | "off") => Ok(false),
        Some(_) => bail!("invalid boolean for {name}"),
    }
}

fn parse_port_range(raw: &str) -> Result<(u16, u16)> {
    let (start, end) = raw
        .split_once('-')
        .context("COLOMBO_FTP_PASSIVE_PORTS must be start-end")?;
    let start: u16 = start.parse().context("invalid passive port start")?;
    let end: u16 = end.parse().context("invalid passive port end")?;
    if start == 0 || start > end {
        bail!("invalid passive port range");
    }
    Ok((start, end))
}

fn redact_database_url(value: &str) -> String {
    url::Url::parse(value.strip_prefix("jdbc:").unwrap_or(value))
        .map(|mut u| {
            if u.password().is_some() {
                let _ = u.set_password(Some("***"));
            }
            u.to_string()
        })
        .unwrap_or_else(|_| "<invalid>".into())
}

pub fn parse_passive_ip(value: &str) -> Result<IpAddr> {
    value
        .parse()
        .with_context(|| "COLOMBO_FTP_PASSIVE_EXTERNAL_ADDRESS must be an IP address")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn passive_range_requires_an_ordered_nonzero_range() {
        assert_eq!(parse_port_range("60000-60100").unwrap(), (60000, 60100));
        assert!(parse_port_range("60100-60000").is_err());
        assert!(parse_port_range("0-1").is_err());
        assert!(parse_port_range("invalid").is_err());
    }

    #[test]
    fn passive_address_is_an_ip_when_requested() {
        assert!(parse_passive_ip("203.0.113.1").is_ok());
        assert!(parse_passive_ip("colombo.gaulatti.com").is_err());
    }

    #[test]
    fn database_debug_output_hides_embedded_password() {
        let redacted = redact_database_url("postgresql://user:secret@localhost/colombo");
        assert!(!redacted.contains("secret"));
        assert!(redacted.contains("***"));
    }
}
