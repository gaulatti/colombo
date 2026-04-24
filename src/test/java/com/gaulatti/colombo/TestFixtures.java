package com.gaulatti.colombo;

import com.gaulatti.colombo.ftp.SessionUploadCredentials;
import com.gaulatti.colombo.model.Tenant;

public final class TestFixtures {

    private TestFixtures() {
    }

    public static Tenant tenant() {
        Tenant tenant = new Tenant();
        tenant.setId(1L);
        tenant.setName("Acme");
        tenant.setFtpUsername("acme-user");
        tenant.setApiKey("api-key");
        tenant.setValidationEndpoint("https://cms.example.com/validate");
        tenant.setPhotoEndpoint("https://cms.example.com/photo");
        return tenant;
    }

    public static SessionUploadCredentials validCredentials() {
        return new SessionUploadCredentials(
                "access",
                "secret",
                "token",
                "us-east-1",
                "bucket",
                "prefix",
                "2026-01-01T00:00:00Z"
        );
    }
}
