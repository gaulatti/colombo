package com.gaulatti.colombo.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.gaulatti.colombo.ftp.UploadNamingPolicy;
import com.gaulatti.colombo.ftp.UploadNamingSegment;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class UploadNameRendererTest {
    private final UploadNameRenderer renderer = new UploadNameRenderer();
    private final Instant captured = Instant.parse("2026-08-17T14:35:22.418Z");
    private final Instant uploaded = Instant.parse("2026-08-18T01:02:03.004Z");

    @Test
    void rendersEverySupportedPlaceholderWithoutReinterpretingLiterals() {
        UploadNamingPolicy policy = policy("preserve", "uploadedTime");
        policy.setPath(List.of(
                literal("assignmentSlug-sequence"), literal("/"), placeholder("assignmentSlug"), literal("/"),
                formatted("capturedDate", "yyyy-MM-dd"), literal("/"), formatted("uploadedDate", "yyyyMMdd")
        ));
        policy.setFilename(List.of(
                placeholder("assignmentKey"), literal("-"), formatted("capturedTime", "HHmmssSSS"),
                literal("-"), formatted("uploadedTime", "HH-mm-ss"), literal("-"),
                sequence(6), literal("-"), placeholder("originalStem"), literal("."),
                placeholder("originalExtension")
        ));

        assertEquals(
                "assignmentSlug-sequence/news-desk/2026-08-17/20260818/KEY-143522418-01-02-03-000042-My-Photo.jpg",
                renderer.render(policy, "KEY", "My Photo.JPG", captured, uploaded, 42)
        );
    }

    @Test
    void supportsAllDateTimeFormatsCaseModesFallbackAndNoPath() {
        List<String> patterns = List.of("yyyy", "MM", "dd", "yyyyMMdd", "HH", "mm", "ss", "HHmmss");
        for (String pattern : patterns) {
            UploadNamingPolicy policy = policy("lowercase", "uploadedTime");
            String name = pattern.startsWith("H") || pattern.equals("mm") || pattern.equals("ss")
                    ? "capturedTime" : "capturedDate";
            policy.setPath(List.of());
            policy.setFilename(List.of(
                    formatted(name, pattern), literal("-FIXED-"), sequence(3),
                    literal("."), placeholder("originalExtension")
            ));
            renderer.render(policy, "key", "PHOTO", null, uploaded, 1);
        }

        UploadNamingPolicy uppercase = policy("uppercase", "uploadedTime");
        uppercase.setPath(List.of());
        uppercase.setFilename(List.of(
                literal("Fixed-"), placeholder("originalStem"), literal("-"), sequence(3),
                literal("."), placeholder("originalExtension")
        ));
        assertEquals("FIXED-PHOTO-001.BIN", renderer.render(uppercase, "key", "photo", captured, uploaded, 1));
    }

    @Test
    void rejectsInvalidPoliciesSegmentsPlaceholdersCaptureAndUnsafeTargets() {
        assertThrows(IllegalArgumentException.class,
                () -> renderer.render(null, "key", "photo.jpg", captured, uploaded, 1));
        UploadNamingPolicy invalid = policy("preserve", "uploadedTime");
        invalid.setVersion(2);
        assertThrows(IllegalArgumentException.class,
                () -> renderer.render(invalid, "key", "photo.jpg", captured, uploaded, 1));

        UploadNamingPolicy rejectCapture = policy("preserve", "reject");
        assertThrows(IllegalArgumentException.class,
                () -> renderer.render(rejectCapture, "key", "photo.jpg", null, uploaded, 1));

        UploadNamingPolicy badType = policy("preserve", "uploadedTime");
        badType.setFilename(List.of(new UploadNamingSegment("other", null, null, null, null)));
        assertThrows(IllegalArgumentException.class,
                () -> renderer.render(badType, "key", "photo.jpg", captured, uploaded, 1));

        UploadNamingPolicy nullLiteral = policy("preserve", "uploadedTime");
        nullLiteral.setFilename(List.of(new UploadNamingSegment("literal", null, null, null, null), literal("ok-"), sequence(3)));
        assertEquals("incoming/ok-001", renderer.render(nullLiteral, "key", "photo.jpg", captured, uploaded, 1));

        UploadNamingPolicy badPlaceholder = policy("preserve", "uploadedTime");
        badPlaceholder.setFilename(List.of(placeholder("unknown"), sequence(3)));
        assertThrows(IllegalArgumentException.class,
                () -> renderer.render(badPlaceholder, "key", "photo.jpg", captured, uploaded, 1));

        for (String unsafe : List.of("", "/absolute", ".", "..", "a\\b", "a//b", "a/../b", "a/..")) {
            assertUnsafeTarget(unsafe);
        }

        UploadNamingPolicy control = policy("preserve", "uploadedTime");
        control.setFilename(List.of(literal("bad\u0001name"), sequence(3)));
        assertThrows(IllegalArgumentException.class,
                () -> renderer.render(control, "key", "photo.jpg", captured, uploaded, 1));

        UploadNamingPolicy longComponent = policy("preserve", "uploadedTime");
        longComponent.setFilename(List.of(literal("x".repeat(256)), sequence(3)));
        assertThrows(IllegalArgumentException.class,
                () -> renderer.render(longComponent, "key", "photo.jpg", captured, uploaded, 1));

        UploadNamingPolicy longTarget = policy("preserve", "uploadedTime");
        List<UploadNamingSegment> many = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            if (i > 0) many.add(literal("/"));
            many.add(literal("x".repeat(230)));
        }
        many.add(sequence(3));
        longTarget.setFilename(many);
        assertThrows(IllegalArgumentException.class,
                () -> renderer.render(longTarget, "key", "photo.jpg", captured, uploaded, 1));
    }

    @Test
    void policyValidationCoversEveryField() {
        UploadNamingPolicy policy = policy("preserve", "uploadedTime");
        assertEquals(true, policy.isValid());
        policy.setVersion(2); assertEquals(false, policy.isValid()); policy.setVersion(1);
        policy.setAssignmentSlug(null); assertEquals(false, policy.isValid());
        policy.setAssignmentSlug(" "); assertEquals(false, policy.isValid()); policy.setAssignmentSlug("slug");
        policy.setPath(null); assertEquals(false, policy.isValid()); policy.setPath(List.of());
        policy.setFilename(null); assertEquals(false, policy.isValid());
        policy.setFilename(List.of()); assertEquals(false, policy.isValid());
        policy.setFilename(List.of(literal("x"))); assertEquals(false, policy.isValid());
        policy.setFilename(List.of(sequence(3)));
        policy.setTimezone(null); assertEquals(false, policy.isValid());
        policy.setTimezone(" "); assertEquals(false, policy.isValid()); policy.setTimezone("UTC");
        policy.setCaptureTimeFallback("other"); assertEquals(false, policy.isValid());
        policy.setCaptureTimeFallback("reject"); assertEquals(true, policy.isValid());
        policy.setCaseMode("other"); assertEquals(false, policy.isValid());
        policy.setCaseMode("lowercase"); assertEquals(true, policy.isValid());
        policy.setCaseMode("uppercase"); assertEquals(true, policy.isValid());
    }

    private UploadNamingPolicy policy(String caseMode, String fallback) {
        return new UploadNamingPolicy(1, "news-desk", List.of(literal("incoming")),
                List.of(sequence(6)), "UTC", fallback, caseMode);
    }

    private UploadNamingSegment literal(String value) {
        return new UploadNamingSegment("literal", value, null, null, null);
    }

    private UploadNamingSegment placeholder(String name) {
        return new UploadNamingSegment("placeholder", null, name, null, null);
    }

    private UploadNamingSegment formatted(String name, String format) {
        return new UploadNamingSegment("placeholder", null, name, format, null);
    }

    private UploadNamingSegment sequence(int width) {
        return new UploadNamingSegment("placeholder", null, "sequence", null, width);
    }

    private void assertUnsafeTarget(String target) {
        try {
            Method validateTarget = UploadNameRenderer.class.getDeclaredMethod("validateTarget", String.class);
            validateTarget.setAccessible(true);
            InvocationTargetException thrown = assertThrows(
                    InvocationTargetException.class,
                    () -> validateTarget.invoke(renderer, target)
            );
            assertInstanceOf(IllegalArgumentException.class, thrown.getCause());
        } catch (NoSuchMethodException exception) {
            throw new AssertionError(exception);
        }
    }
}
