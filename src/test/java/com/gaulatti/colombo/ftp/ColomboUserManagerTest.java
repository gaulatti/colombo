package com.gaulatti.colombo.ftp;

import static com.gaulatti.colombo.TestFixtures.tenant;
import static com.gaulatti.colombo.TestFixtures.validCredentials;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.gaulatti.colombo.model.Tenant;
import com.gaulatti.colombo.repository.TenantRepository;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ftpserver.ftplet.Authentication;
import org.apache.ftpserver.ftplet.AuthenticationFailedException;
import org.apache.ftpserver.ftplet.FtpException;
import org.apache.ftpserver.ftplet.User;
import org.apache.ftpserver.usermanager.UsernamePasswordAuthentication;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestTemplate;

@ExtendWith(MockitoExtension.class)
class ColomboUserManagerTest {

    @Mock
    private TenantRepository tenantRepository;

    @Mock
    private RestTemplate restTemplate;

    private ConcurrentHashMap<String, SessionData> sessions;
    private Tenant tenant;

    @BeforeEach
    void setUp() {
        sessions = new ConcurrentHashMap<>();
        tenant = tenant();
    }

    @AfterEach
    void tearDown() {
        System.clearProperty("COLOMBO_MASTER_PASSWORD");
        System.clearProperty("COLOMBO_DEV_PASSWORD");
    }

    @Test
    void basicUserManagerMethodsReturnExpectedDefaults() {
        ColomboUserManager manager = new ColomboUserManager(tenantRepository, restTemplate, sessions, "");

        assertEquals("", manager.getAdminName());
        assertEquals(0, manager.getAllUserNames().length);
        assertTrue(!manager.isAdmin("any"));
        manager.delete("ignored");
        manager.save(null);
    }

    @Test
    void getUserByNameReturnsMappedUser() throws Exception {
        when(tenantRepository.findByFtpUsername("acme-user")).thenReturn(Optional.of(tenant));
        ColomboUserManager manager = new ColomboUserManager(tenantRepository, restTemplate, sessions, "");

        User user = manager.getUserByName("acme-user");

        assertEquals("acme-user", user.getName());
        assertTrue(manager.doesExist("acme-user"));
    }

    @Test
    void getUserByNameThrowsWhenTenantMissing() {
        when(tenantRepository.findByFtpUsername("missing")).thenReturn(Optional.empty());
        ColomboUserManager manager = new ColomboUserManager(tenantRepository, restTemplate, sessions, "");

        assertThrows(FtpException.class, () -> manager.getUserByName("missing"));
        assertTrue(!manager.doesExist("missing"));
    }

    @Test
    void authenticateRejectsUnsupportedAuthenticationType() {
        ColomboUserManager manager = new ColomboUserManager(tenantRepository, restTemplate, sessions, "");
        Authentication authentication = new Authentication() {
        };

        assertThrows(AuthenticationFailedException.class, () -> manager.authenticate(authentication));
    }

    @Test
    void authenticateRejectsUnknownUser() {
        when(tenantRepository.findByFtpUsername("unknown")).thenReturn(Optional.empty());
        ColomboUserManager manager = new ColomboUserManager(tenantRepository, restTemplate, sessions, "");

        assertThrows(AuthenticationFailedException.class,
                () -> manager.authenticate(new UsernamePasswordAuthentication("unknown", "pw")));
    }

    @Test
    void authenticateUsesConfiguredMasterPassword() throws Exception {
        when(tenantRepository.findByFtpUsername("acme-user")).thenReturn(Optional.of(tenant));
        ColomboUserManager manager = new ColomboUserManager(tenantRepository, restTemplate, sessions, "master");

        User user = manager.authenticate(new UsernamePasswordAuthentication("acme-user", "master"));

        assertEquals("acme-user", user.getName());
        assertNotNull(sessions.get("acme-user"));
        assertTrue(sessions.get("acme-user").getAssignmentId().startsWith("support-assignment-"));
    }

        @Test
        void authenticateDoesNotBypassWhenMasterPasswordDoesNotMatch() {
        when(tenantRepository.findByFtpUsername("acme-user")).thenReturn(Optional.of(tenant));
        when(restTemplate.exchange(eq(tenant.getValidationEndpoint()), any(), any(), eq(Map.class)))
            .thenThrow(HttpClientErrorException.create(
                HttpStatus.UNAUTHORIZED,
                "Unauthorized",
                HttpHeaders.EMPTY,
                new byte[0],
                StandardCharsets.UTF_8
            ));

        ColomboUserManager manager = new ColomboUserManager(tenantRepository, restTemplate, sessions, "master");

        assertThrows(AuthenticationFailedException.class,
            () -> manager.authenticate(new UsernamePasswordAuthentication("acme-user", "not-master")));
        }

    @Test
    void authenticateDoesNotBypassWhenConfiguredMasterIsBlank() throws Exception {
        when(tenantRepository.findByFtpUsername("acme-user")).thenReturn(Optional.of(tenant));
        when(restTemplate.exchange(eq(tenant.getValidationEndpoint()), any(), any(), eq(Map.class)))
                .thenReturn(new ResponseEntity<>(validValidationBody(), HttpStatus.OK));

        ColomboUserManager manager = new ColomboUserManager(tenantRepository, restTemplate, sessions, "   ");

        User user = manager.authenticate(new UsernamePasswordAuthentication("acme-user", "pw"));
        assertEquals("acme-user", user.getName());
        assertEquals("assignment-1", sessions.get("acme-user").getAssignmentId());
    }

    @Test
    void authenticateUsesSystemPropertyMasterPasswordFallback() throws Exception {
        System.setProperty("COLOMBO_MASTER_PASSWORD", "fallback");
        when(tenantRepository.findByFtpUsername("acme-user")).thenReturn(Optional.of(tenant));
        ColomboUserManager manager = new ColomboUserManager(tenantRepository, restTemplate, sessions, " ");

        User user = manager.authenticate(new UsernamePasswordAuthentication("acme-user", "fallback"));

        assertEquals("acme-user", user.getName());
        assertNotNull(sessions.get("acme-user"));
    }

    @Test
    void authenticateStoresSessionOnSuccessfulValidation() throws Exception {
        when(tenantRepository.findByFtpUsername("acme-user")).thenReturn(Optional.of(tenant));
        when(restTemplate.exchange(eq(tenant.getValidationEndpoint()), any(), any(), eq(Map.class)))
                .thenReturn(new ResponseEntity<>(validValidationBody(), HttpStatus.OK));

        ColomboUserManager manager = new ColomboUserManager(tenantRepository, restTemplate, sessions, "");
        User user = manager.authenticate(new UsernamePasswordAuthentication("acme-user", "pw"));

        assertEquals("acme-user", user.getName());
        SessionData sessionData = sessions.get("acme-user");
        assertNotNull(sessionData);
        assertEquals("assignment-1", sessionData.getAssignmentId());
        assertEquals("pw", sessionData.getValidationKey());
    }

    @Test
    void authenticateDeniesOnInvalidResponseBody() {
        when(tenantRepository.findByFtpUsername("acme-user")).thenReturn(Optional.of(tenant));
        Map<String, Object> invalid = new HashMap<>();
        invalid.put("assignmentId", "assignment-1");
        invalid.put("upload", "not-a-map");
        when(restTemplate.exchange(eq(tenant.getValidationEndpoint()), any(), any(), eq(Map.class)))
                .thenReturn(new ResponseEntity<>(invalid, HttpStatus.OK));

        ColomboUserManager manager = new ColomboUserManager(tenantRepository, restTemplate, sessions, "");

        assertThrows(AuthenticationFailedException.class,
                () -> manager.authenticate(new UsernamePasswordAuthentication("acme-user", "pw")));
    }

    @Test
    void authenticateDeniesOn4xxAndClearsSession() {
        sessions.put("acme-user", new SessionData(tenant, "old", validCredentials(), "old"));
        when(tenantRepository.findByFtpUsername("acme-user")).thenReturn(Optional.of(tenant));
        when(restTemplate.exchange(eq(tenant.getValidationEndpoint()), any(), any(), eq(Map.class)))
                .thenThrow(HttpClientErrorException.create(
                        HttpStatus.UNAUTHORIZED,
                        "Unauthorized",
                        HttpHeaders.EMPTY,
                        new byte[0],
                        StandardCharsets.UTF_8
                ));

        ColomboUserManager manager = new ColomboUserManager(tenantRepository, restTemplate, sessions, "");

        assertThrows(AuthenticationFailedException.class,
                () -> manager.authenticate(new UsernamePasswordAuthentication("acme-user", "bad")));
        assertTrue(!sessions.containsKey("acme-user"));
    }

    @Test
    void authenticateFailsOnTransportErrors() {
        when(tenantRepository.findByFtpUsername("acme-user")).thenReturn(Optional.of(tenant));
        when(restTemplate.exchange(eq(tenant.getValidationEndpoint()), any(), any(), eq(Map.class)))
                .thenThrow(new RuntimeException("down"));

        ColomboUserManager manager = new ColomboUserManager(tenantRepository, restTemplate, sessions, "");

        assertThrows(AuthenticationFailedException.class,
                () -> manager.authenticate(new UsernamePasswordAuthentication("acme-user", "pw")));
    }

    @Test
    void authenticateHandlesNullPasswordAndNullCmsBody() {
        when(tenantRepository.findByFtpUsername("acme-user")).thenReturn(Optional.of(tenant));
        when(restTemplate.exchange(eq(tenant.getValidationEndpoint()), any(), any(), eq(Map.class)))
                .thenReturn(new ResponseEntity<>((Map<String, Object>) null, HttpStatus.OK));

        ColomboUserManager manager = new ColomboUserManager(tenantRepository, restTemplate, sessions, "");

        assertThrows(AuthenticationFailedException.class,
                () -> manager.authenticate(new UsernamePasswordAuthentication("acme-user", null)));
    }

    @Test
    void refreshSessionReturnsNotFoundWhenMissing() {
        ColomboUserManager manager = new ColomboUserManager(tenantRepository, restTemplate, sessions, "");
        assertEquals(ColomboUserManager.RefreshResult.NOT_FOUND, manager.refreshSessionFromValidation("missing"));
    }

    @Test
    void refreshSessionDeniesWhenRefreshContextMissing() {
        sessions.put("acme-user", new SessionData(null, "assignment", validCredentials(), ""));
        ColomboUserManager manager = new ColomboUserManager(tenantRepository, restTemplate, sessions, "");

        assertEquals(ColomboUserManager.RefreshResult.DENIED, manager.refreshSessionFromValidation("acme-user"));
        assertTrue(!sessions.containsKey("acme-user"));
    }

    @Test
    void refreshSessionDeniesWhenValidationKeyIsNull() {
        sessions.put("acme-user", new SessionData(tenant, "assignment", validCredentials(), null));
        ColomboUserManager manager = new ColomboUserManager(tenantRepository, restTemplate, sessions, "");

        assertEquals(ColomboUserManager.RefreshResult.DENIED, manager.refreshSessionFromValidation("acme-user"));
        assertTrue(!sessions.containsKey("acme-user"));
    }

    @Test
    void refreshSessionDeniesWhenValidationKeyIsBlank() {
        sessions.put("acme-user", new SessionData(tenant, "assignment", validCredentials(), "   "));
        ColomboUserManager manager = new ColomboUserManager(tenantRepository, restTemplate, sessions, "");

        assertEquals(ColomboUserManager.RefreshResult.DENIED, manager.refreshSessionFromValidation("acme-user"));
        assertTrue(!sessions.containsKey("acme-user"));
    }

    @Test
    void refreshSessionSucceedsWithValidCmsResponse() {
        SessionData existing = new SessionData(tenant, "assignment", validCredentials(), "refresh-key");
        sessions.put("acme-user", existing);
        when(restTemplate.exchange(eq(tenant.getValidationEndpoint()), any(), any(), eq(Map.class)))
                .thenReturn(new ResponseEntity<>(validValidationBody(), HttpStatus.OK));

        ColomboUserManager manager = new ColomboUserManager(tenantRepository, restTemplate, sessions, "");

        assertEquals(ColomboUserManager.RefreshResult.REFRESHED, manager.refreshSessionFromValidation("acme-user"));
        assertEquals("assignment-1", sessions.get("acme-user").getAssignmentId());
    }

    @Test
    void refreshSessionDeniesOn4xx() {
        sessions.put("acme-user", new SessionData(tenant, "assignment", validCredentials(), "refresh-key"));
        when(restTemplate.exchange(eq(tenant.getValidationEndpoint()), any(), any(), eq(Map.class)))
                .thenThrow(HttpClientErrorException.create(
                        HttpStatus.UNAUTHORIZED,
                        "Unauthorized",
                        HttpHeaders.EMPTY,
                        new byte[0],
                        StandardCharsets.UTF_8
                ));

        ColomboUserManager manager = new ColomboUserManager(tenantRepository, restTemplate, sessions, "");

        assertEquals(ColomboUserManager.RefreshResult.DENIED, manager.refreshSessionFromValidation("acme-user"));
        assertTrue(!sessions.containsKey("acme-user"));
    }

    @Test
    void refreshSessionReturnsErrorOn5xxAndRuntimeExceptions() {
        sessions.put("acme-user", new SessionData(tenant, "assignment", validCredentials(), "refresh-key"));
        when(restTemplate.exchange(eq(tenant.getValidationEndpoint()), any(), any(), eq(Map.class)))
                .thenThrow(HttpServerErrorException.create(
                        HttpStatus.BAD_GATEWAY,
                        "Bad Gateway",
                        HttpHeaders.EMPTY,
                        new byte[0],
                        StandardCharsets.UTF_8
                ));

        ColomboUserManager manager = new ColomboUserManager(tenantRepository, restTemplate, sessions, "");

        assertEquals(ColomboUserManager.RefreshResult.ERROR, manager.refreshSessionFromValidation("acme-user"));

        when(restTemplate.exchange(eq(tenant.getValidationEndpoint()), any(), any(), eq(Map.class)))
                .thenThrow(new RuntimeException("network"));
        assertEquals(ColomboUserManager.RefreshResult.ERROR, manager.refreshSessionFromValidation("acme-user"));
    }

    @Test
    void refreshSessionHandlesNonOkCmsResponses() {
        sessions.put("acme-user", new SessionData(tenant, "assignment", validCredentials(), "refresh-key"));
        when(restTemplate.exchange(eq(tenant.getValidationEndpoint()), any(), any(), eq(Map.class)))
                .thenReturn(new ResponseEntity<>(Map.of("error", "nope"), HttpStatus.UNAUTHORIZED));

        ColomboUserManager manager = new ColomboUserManager(tenantRepository, restTemplate, sessions, "");

        assertEquals(ColomboUserManager.RefreshResult.DENIED, manager.refreshSessionFromValidation("acme-user"));

        sessions.put("acme-user", new SessionData(tenant, "assignment", validCredentials(), "refresh-key"));
        when(restTemplate.exchange(eq(tenant.getValidationEndpoint()), any(), any(), eq(Map.class)))
                .thenReturn(new ResponseEntity<>(Map.of("error", "upstream"), HttpStatus.BAD_GATEWAY));

        assertEquals(ColomboUserManager.RefreshResult.ERROR, manager.refreshSessionFromValidation("acme-user"));
    }

    @Test
    void evictSessionRemovesEntry() {
        sessions.put("acme-user", new SessionData(tenant, "assignment", validCredentials(), "key"));
        ColomboUserManager manager = new ColomboUserManager(tenantRepository, restTemplate, sessions, "");

        manager.evictSession("acme-user", "test");

        assertTrue(!sessions.containsKey("acme-user"));
    }

    @Test
    void authenticateHandlesNullFieldsInResponse() {
        when(tenantRepository.findByFtpUsername("acme-user")).thenReturn(Optional.of(tenant));
        Map<String, Object> invalid = new HashMap<>();
        invalid.put("assignmentId", null);
        invalid.put("upload", Map.of());
        when(restTemplate.exchange(eq(tenant.getValidationEndpoint()), any(), any(), eq(Map.class)))
                .thenReturn(new ResponseEntity<>(invalid, HttpStatus.OK));

        ColomboUserManager manager = new ColomboUserManager(tenantRepository, restTemplate, sessions, "");
        assertThrows(AuthenticationFailedException.class,
                () -> manager.authenticate(new UsernamePasswordAuthentication("acme-user", "pw")));

        verify(tenantRepository).findByFtpUsername("acme-user");
    }

        @Test
        void authenticateDeniesOnBlankAssignmentIdAndInvalidUploadPayload() {
        when(tenantRepository.findByFtpUsername("acme-user")).thenReturn(Optional.of(tenant));

        Map<String, Object> uploadMissingFields = new HashMap<>();
        uploadMissingFields.put("accessKeyId", "access");

        Map<String, Object> invalid = new HashMap<>();
        invalid.put("assignmentId", "   ");
        invalid.put("upload", uploadMissingFields);

        when(restTemplate.exchange(eq(tenant.getValidationEndpoint()), any(), any(), eq(Map.class)))
            .thenReturn(new ResponseEntity<>(invalid, HttpStatus.OK));

        ColomboUserManager manager = new ColomboUserManager(tenantRepository, restTemplate, sessions, "");
        assertThrows(AuthenticationFailedException.class,
            () -> manager.authenticate(new UsernamePasswordAuthentication("acme-user", "pw")));
        }

        @Test
        void authenticateDeniesOnBlankAssignmentIdEvenWithValidUploadPayload() {
        when(tenantRepository.findByFtpUsername("acme-user")).thenReturn(Optional.of(tenant));

        Map<String, Object> invalid = new HashMap<>(validValidationBody());
        invalid.put("assignmentId", " ");

        when(restTemplate.exchange(eq(tenant.getValidationEndpoint()), any(), any(), eq(Map.class)))
            .thenReturn(new ResponseEntity<>(invalid, HttpStatus.OK));

        ColomboUserManager manager = new ColomboUserManager(tenantRepository, restTemplate, sessions, "");
        assertThrows(AuthenticationFailedException.class,
            () -> manager.authenticate(new UsernamePasswordAuthentication("acme-user", "pw")));
        }

        @Test
        void authenticateDeniesOnNonBlankAssignmentWithInvalidUploadCredentials() {
        when(tenantRepository.findByFtpUsername("acme-user")).thenReturn(Optional.of(tenant));

        Map<String, Object> uploadMissingFields = new HashMap<>();
        uploadMissingFields.put("accessKeyId", "access");

        Map<String, Object> invalid = new HashMap<>();
        invalid.put("assignmentId", "assignment-1");
        invalid.put("upload", uploadMissingFields);

        when(restTemplate.exchange(eq(tenant.getValidationEndpoint()), any(), any(), eq(Map.class)))
            .thenReturn(new ResponseEntity<>(invalid, HttpStatus.OK));

        ColomboUserManager manager = new ColomboUserManager(tenantRepository, restTemplate, sessions, "");
        assertThrows(AuthenticationFailedException.class,
            () -> manager.authenticate(new UsernamePasswordAuthentication("acme-user", "pw")));
        }

    @Test
    void masterPasswordBypassHelperCoversAllPredicateBranches() throws Exception {
        ColomboUserManager manager = new ColomboUserManager(tenantRepository, restTemplate, sessions, "");
        var method = ColomboUserManager.class.getDeclaredMethod("isMasterPasswordBypass", String.class, String.class);
        method.setAccessible(true);

        assertTrue(!(boolean) method.invoke(manager, null, "pw"));
        assertTrue(!(boolean) method.invoke(manager, "   ", "pw"));
        assertTrue(!(boolean) method.invoke(manager, "master", "pw"));
        assertTrue((boolean) method.invoke(manager, "master", "master"));
    }

    private Map<String, Object> validValidationBody() {
        Map<String, Object> upload = new HashMap<>();
        upload.put("accessKeyId", "access");
        upload.put("secretAccessKey", "secret");
        upload.put("sessionToken", "token");
        upload.put("region", "us-east-1");
        upload.put("bucket", "bucket");
        upload.put("keyPrefix", "prefix");
        upload.put("expiresAt", "2026-01-01T00:00:00Z");

        Map<String, Object> body = new HashMap<>();
        body.put("assignmentId", "assignment-1");
        body.put("upload", upload);
        return body;
    }
}
