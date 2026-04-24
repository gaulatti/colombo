package com.gaulatti.colombo.repository;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.gaulatti.colombo.model.Tenant;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class TenantRepositoryTest {

    @Autowired
    private TenantRepository tenantRepository;

    @Test
    void findByFtpUsernameReturnsMatchingTenant() {
        Tenant tenant = new Tenant();
        tenant.setName("RepoTest");
        tenant.setFtpUsername("repo-user");
        tenant.setApiKey("repo-api");
        tenant.setValidationEndpoint("https://example.com/validate");
        tenant.setPhotoEndpoint("https://example.com/photo");

        tenantRepository.save(tenant);

        assertTrue(tenantRepository.findByFtpUsername("repo-user").isPresent());
        assertEquals("RepoTest", tenantRepository.findByFtpUsername("repo-user").orElseThrow().getName());
        assertFalse(tenantRepository.findByFtpUsername("missing").isPresent());
    }
}
