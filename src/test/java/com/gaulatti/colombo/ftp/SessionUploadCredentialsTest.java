package com.gaulatti.colombo.ftp;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

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
}
