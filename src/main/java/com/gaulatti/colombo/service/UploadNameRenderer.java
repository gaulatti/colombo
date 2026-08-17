package com.gaulatti.colombo.service;

import com.gaulatti.colombo.ftp.UploadNamingPolicy;
import com.gaulatti.colombo.ftp.UploadNamingSegment;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Locale;
import org.springframework.stereotype.Component;

/** Renders only the typed segments already validated by the CMS contract. */
@Component
public class UploadNameRenderer {

    public String render(
            UploadNamingPolicy policy,
            String assignmentKey,
            String originalFilename,
            Instant capturedAt,
            Instant uploadedAt,
            long sequence
    ) {
        if (policy == null || !policy.isValid()) {
            throw new IllegalArgumentException("Invalid upload naming policy");
        }
        Instant effectiveCapture = capturedAt;
        if (effectiveCapture == null) {
            if ("reject".equals(policy.getCaptureTimeFallback())) {
                throw new IllegalArgumentException("Capture time is required by the naming policy");
            }
            effectiveCapture = uploadedAt;
        }

        ZoneId zone = ZoneId.of(policy.getTimezone());
        String stem = originalFilename.replaceFirst("\\.[^.]*$", "");
        String extension = originalFilename.contains(".")
                ? originalFilename.substring(originalFilename.lastIndexOf('.') + 1)
                : "bin";
        RenderContext context = new RenderContext(
                policy.getAssignmentSlug(), assignmentKey, sanitizeComponent(stem),
                sanitizeComponent(extension).toLowerCase(Locale.ROOT), effectiveCapture, uploadedAt,
                sequence, zone
        );

        String path = renderSegments(policy.getPath(), context);
        String filename = renderSegments(policy.getFilename(), context);
        String target = path.isBlank() ? filename : path.replaceAll("/+$", "") + "/" + filename;
        target = applyCase(target, policy.getCaseMode());
        validateTarget(target);
        return target;
    }

    private String renderSegments(List<UploadNamingSegment> segments, RenderContext context) {
        StringBuilder output = new StringBuilder();
        for (UploadNamingSegment segment : segments) {
            if ("literal".equals(segment.getType())) {
                output.append(segment.getValue() == null ? "" : segment.getValue());
            } else if ("placeholder".equals(segment.getType())) {
                output.append(renderPlaceholder(segment, context));
            } else {
                throw new IllegalArgumentException("Unsupported naming segment type");
            }
        }
        return output.toString();
    }

    private String renderPlaceholder(UploadNamingSegment segment, RenderContext context) {
        return switch (segment.getName()) {
            case "assignmentSlug" -> context.assignmentSlug();
            case "assignmentKey" -> context.assignmentKey();
            case "originalStem" -> context.originalStem();
            case "originalExtension" -> context.originalExtension();
            case "capturedDate", "capturedTime" -> format(context.capturedAt(), context.zone(), segment.getFormat());
            case "uploadedDate", "uploadedTime" -> format(context.uploadedAt(), context.zone(), segment.getFormat());
            case "sequence" -> String.format(Locale.ROOT, "%0" + segment.getWidth() + "d", context.sequence());
            default -> throw new IllegalArgumentException("Unsupported naming placeholder");
        };
    }

    private String format(Instant instant, ZoneId zone, String pattern) {
        return DateTimeFormatter.ofPattern(pattern, Locale.ROOT).withZone(zone).format(instant);
    }

    private String applyCase(String value, String caseMode) {
        return switch (caseMode) {
            case "lowercase" -> value.toLowerCase(Locale.ROOT);
            case "uppercase" -> value.toUpperCase(Locale.ROOT);
            default -> value;
        };
    }

    private String sanitizeComponent(String value) {
        String sanitized = value.replaceAll("[^A-Za-z0-9._-]", "-").replaceAll("-+", "-");
        return sanitized.replaceAll("^[.-]+|[.-]+$", "");
    }

    private void validateTarget(String target) {
        if (target.isBlank() || target.startsWith("/") || target.contains("\\")
                || target.contains("../") || target.contains("/..") || target.contains("//")
                || target.length() > 900) {
            throw new IllegalArgumentException("Rendered upload name is unsafe");
        }
        for (String component : target.split("/")) {
            if (component.equals(".") || component.equals("..")
                    || component.length() > 255 || component.chars().anyMatch(Character::isISOControl)) {
                throw new IllegalArgumentException("Rendered upload name is unsafe");
            }
        }
    }

    private record RenderContext(
            String assignmentSlug,
            String assignmentKey,
            String originalStem,
            String originalExtension,
            Instant capturedAt,
            Instant uploadedAt,
            long sequence,
            ZoneId zone
    ) {
    }
}
