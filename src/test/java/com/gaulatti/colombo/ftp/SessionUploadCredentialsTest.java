package com.gaulatti.colombo.ftp;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import java.util.List;

class SessionUploadCredentialsTest {

    @Test
    void isValidReturnsTrueWhenAllFieldsPresent() {
        SessionUploadCredentials credentials = new SessionUploadCredentials(
                "a", "b", "c", "d", "e", "f", "g"
        );

        assertTrue(credentials.isValid());
    }

    @Test
    void isValidReturnsFalseWhenAnyFieldIsMissingOrBlank() {
        assertFalse(new SessionUploadCredentials(null, "b", "c", "d", "e", "f", "g").isValid());
        assertFalse(new SessionUploadCredentials("a", " ", "c", "d", "e", "f", "g").isValid());
        assertFalse(new SessionUploadCredentials("a", "b", "", "d", "e", "f", "g").isValid());
        assertFalse(new SessionUploadCredentials("a", "b", "c", " ", "e", "f", "g").isValid());
        assertFalse(new SessionUploadCredentials("a", "b", "c", "d", null, "f", "g").isValid());
        assertFalse(new SessionUploadCredentials("a", "b", "c", "d", "e", "", "g").isValid());
        assertFalse(new SessionUploadCredentials("a", "b", "c", "d", "e", "f", " ").isValid());
    }

    @Test
    void namingPolicyRequiresValidPolicyAndSequenceEndpoint() {
        UploadNamingPolicy valid = new UploadNamingPolicy(1, "slug", List.of(),
                List.of(new UploadNamingSegment("placeholder", null, "sequence", null, 3)),
                "UTC", "uploadedTime", "preserve");
        assertFalse(new SessionUploadCredentials("a", "b", "c", "d", "e", "f", "g", valid, null).isValid());
        assertFalse(new SessionUploadCredentials("a", "b", "c", "d", "e", "f", "g", valid, " ").isValid());
        assertTrue(new SessionUploadCredentials("a", "b", "c", "d", "e", "f", "g", valid, "/sequence").isValid());
        valid.setVersion(2);
        assertFalse(new SessionUploadCredentials("a", "b", "c", "d", "e", "f", "g", valid, "/sequence").isValid());
    }
}
