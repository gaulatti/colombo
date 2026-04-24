package com.gaulatti.colombo.ftp;

import static com.gaulatti.colombo.TestFixtures.tenant;
import static com.gaulatti.colombo.TestFixtures.validCredentials;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class SessionDataTest {

    @Test
    void dataMethodsWorkAsExpected() {
        SessionData left = new SessionData(tenant(), "assignment", validCredentials(), "validation");
        SessionData right = new SessionData(tenant(), "assignment", validCredentials(), "validation");

        assertEquals(left, right);
        assertEquals(left.hashCode(), right.hashCode());
        assertTrue(left.toString().contains("SessionData"));

        right.setAssignmentId("different");
        assertNotEquals(left, right);
    }
}
