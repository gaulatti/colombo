use std::{fs::File, io::BufReader};

use anyhow::{Context, Result, bail};
use chrono::{DateTime, NaiveDateTime, Utc};
use chrono_tz::Tz;

use crate::domain::{UploadNamingPolicy, UploadNamingSegment};

pub fn capture_time(path: &std::path::Path) -> Option<DateTime<Utc>> {
    let file = File::open(path).ok()?;
    let mut reader = BufReader::new(file);
    let exif = exif::Reader::new().read_from_container(&mut reader).ok()?;
    let value = exif
        .get_field(exif::Tag::DateTimeOriginal, exif::In::PRIMARY)?
        .display_value()
        .to_string();
    let naive = NaiveDateTime::parse_from_str(value.trim_matches('"'), "%Y-%m-%d %H:%M:%S")
        .or_else(|_| NaiveDateTime::parse_from_str(value.trim_matches('"'), "%Y:%m:%d %H:%M:%S"))
        .ok()?;
    Some(DateTime::from_naive_utc_and_offset(naive, Utc))
}

pub fn render(
    policy: &UploadNamingPolicy,
    assignment: &str,
    original: &str,
    captured: Option<DateTime<Utc>>,
    uploaded: DateTime<Utc>,
    sequence: u64,
) -> Result<String> {
    if !policy.valid() {
        bail!("Invalid upload naming policy");
    }
    let effective = match captured {
        Some(value) => value,
        None if policy.capture_time_fallback == "uploadedTime" => uploaded,
        None => bail!("Capture time is required by the naming policy"),
    };
    let zone: Tz = policy
        .timezone
        .parse()
        .context("invalid naming-policy timezone")?;
    let (stem, extension) = split_name(original);
    let context = ContextData {
        assignment_slug: &policy.assignment_slug,
        assignment_key: assignment,
        original_stem: &sanitize(stem),
        original_extension: &sanitize(extension).to_ascii_lowercase(),
        captured: effective,
        uploaded,
        zone,
        sequence,
    };
    let path = segments(&policy.path, &context)?;
    let file = segments(&policy.filename, &context)?;
    let mut target = if path.trim().is_empty() {
        file
    } else {
        format!("{}/{}", path.trim_end_matches('/'), file)
    };
    target = match policy.case_mode.as_str() {
        "lowercase" => target.to_lowercase(),
        "uppercase" => target.to_uppercase(),
        _ => target,
    };
    validate_target(&target)?;
    Ok(target)
}

struct ContextData<'a> {
    assignment_slug: &'a str,
    assignment_key: &'a str,
    original_stem: &'a str,
    original_extension: &'a str,
    captured: DateTime<Utc>,
    uploaded: DateTime<Utc>,
    zone: Tz,
    sequence: u64,
}

fn segments(values: &[UploadNamingSegment], ctx: &ContextData<'_>) -> Result<String> {
    values.iter().map(|s| segment(s, ctx)).collect()
}

fn segment(value: &UploadNamingSegment, ctx: &ContextData<'_>) -> Result<String> {
    if value.kind == "literal" {
        return Ok(value.value.clone().unwrap_or_default());
    }
    if value.kind != "placeholder" {
        bail!("Unsupported naming segment type");
    }
    Ok(
        match value
            .name
            .as_deref()
            .context("missing naming placeholder")?
        {
            "assignmentSlug" => ctx.assignment_slug.into(),
            "assignmentKey" => ctx.assignment_key.into(),
            "originalStem" => ctx.original_stem.into(),
            "originalExtension" => ctx.original_extension.into(),
            "capturedDate" | "capturedTime" => format_java(
                ctx.captured.with_timezone(&ctx.zone),
                value.format.as_deref(),
            )?,
            "uploadedDate" | "uploadedTime" => format_java(
                ctx.uploaded.with_timezone(&ctx.zone),
                value.format.as_deref(),
            )?,
            "sequence" => format!(
                "{:0width$}",
                ctx.sequence,
                width = value.width.context("sequence width missing")?
            ),
            _ => bail!("Unsupported naming placeholder"),
        },
    )
}

fn format_java<Tz2: chrono::TimeZone>(value: DateTime<Tz2>, pattern: Option<&str>) -> Result<String>
where
    Tz2::Offset: std::fmt::Display,
{
    let chrono_pattern = match pattern.context("date format missing")? {
        "yyyy" => "%Y",
        "MM" => "%m",
        "dd" => "%d",
        "yyyyMMdd" => "%Y%m%d",
        "yyyy-MM-dd" => "%Y-%m-%d",
        "HH" => "%H",
        "mm" => "%M",
        "ss" => "%S",
        "HHmmss" => "%H%M%S",
        "HHmmssSSS" => "%H%M%S%3f",
        "HH-mm-ss" => "%H-%M-%S",
        _ => bail!("Unsupported date format"),
    };
    Ok(value.format(chrono_pattern).to_string())
}

fn split_name(name: &str) -> (&str, &str) {
    match name.rsplit_once('.') {
        Some((stem, extension)) => (stem, extension),
        None => (name, "bin"),
    }
}

fn sanitize(value: &str) -> String {
    let mut output = String::new();
    for c in value.chars() {
        if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') {
            output.push(c);
        } else if !output.ends_with('-') {
            output.push('-');
        }
    }
    output.trim_matches(['.', '-']).to_owned()
}

fn validate_target(target: &str) -> Result<()> {
    if target.trim().is_empty()
        || target.starts_with('/')
        || target.contains('\\')
        || target.contains("../")
        || target.contains("/..")
        || target.contains("//")
        || target.chars().count() > 900
    {
        bail!("Rendered upload name is unsafe");
    }
    for component in target.split('/') {
        if matches!(component, "." | "..")
            || component.chars().count() > 255
            || component.chars().any(char::is_control)
        {
            bail!("Rendered upload name is unsafe");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::UploadNamingSegment;

    fn placeholder(name: &str, format: Option<&str>, width: Option<usize>) -> UploadNamingSegment {
        UploadNamingSegment {
            kind: "placeholder".into(),
            value: None,
            name: Some(name.into()),
            format: format.map(Into::into),
            width,
        }
    }
    #[test]
    fn renders_policy_and_sanitizes_original_name() {
        let policy = UploadNamingPolicy {
            version: 1,
            assignment_slug: "News".into(),
            path: vec![placeholder("assignmentSlug", None, None)],
            filename: vec![
                placeholder("originalStem", None, None),
                UploadNamingSegment {
                    kind: "literal".into(),
                    value: Some("-".into()),
                    name: None,
                    format: None,
                    width: None,
                },
                placeholder("sequence", None, Some(4)),
                UploadNamingSegment {
                    kind: "literal".into(),
                    value: Some(".".into()),
                    name: None,
                    format: None,
                    width: None,
                },
                placeholder("originalExtension", None, None),
            ],
            timezone: "UTC".into(),
            capture_time_fallback: "uploadedTime".into(),
            case_mode: "lowercase".into(),
        };
        let now = DateTime::parse_from_rfc3339("2025-01-02T03:04:05Z")
            .unwrap()
            .to_utc();
        assert_eq!(
            render(&policy, "a", "My photo.JPG", None, now, 7).unwrap(),
            "news/my-photo-0007.jpg"
        );
    }

    #[test]
    fn renders_all_supported_time_formats() {
        let now = DateTime::parse_from_rfc3339("2026-08-17T14:35:22.418Z")
            .unwrap()
            .to_utc();
        for (pattern, expected) in [
            ("yyyy", "2026"),
            ("MM", "08"),
            ("dd", "17"),
            ("yyyyMMdd", "20260817"),
            ("yyyy-MM-dd", "2026-08-17"),
            ("HH", "14"),
            ("mm", "35"),
            ("ss", "22"),
            ("HHmmss", "143522"),
            ("HHmmssSSS", "143522418"),
            ("HH-mm-ss", "14-35-22"),
        ] {
            assert_eq!(format_java(now, Some(pattern)).unwrap(), expected);
        }
        assert!(format_java(now, Some("unsupported")).is_err());
    }

    #[test]
    fn rejects_capture_requirements_and_unsafe_targets() {
        for target in ["", "/absolute", ".", "..", "a\\b", "a//b", "a/../b", "a/.."] {
            assert!(validate_target(target).is_err(), "accepted {target:?}");
        }
        assert!(validate_target(&"x".repeat(256)).is_err());
        assert!(validate_target("bad\u{1}name").is_err());
    }
}
