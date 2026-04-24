package com.gaulatti.colombo.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class TenantTest {

    @Test
    void dataMethodsWorkAsExpected() {
        Tenant left = new Tenant();
        left.setId(1L);
        left.setName("A");
        left.setFtpUsername("ftp");
        left.setApiKey("api");
        left.setValidationEndpoint("v");
        left.setPhotoEndpoint("p");

        Tenant right = new Tenant();
        right.setId(1L);
        right.setName("A");
        right.setFtpUsername("ftp");
        right.setApiKey("api");
        right.setValidationEndpoint("v");
        right.setPhotoEndpoint("p");

        assertEquals(left, right);
        assertEquals(left.hashCode(), right.hashCode());
        assertTrue(left.toString().contains("Tenant"));

        right.setName("B");
        assertNotEquals(left, right);
    }
}
