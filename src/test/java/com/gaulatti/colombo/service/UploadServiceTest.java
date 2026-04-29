package com.gaulatti.colombo.service;

import static com.gaulatti.colombo.TestFixtures.tenant;
import static com.gaulatti.colombo.TestFixtures.validCredentials;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.gaulatti.colombo.ftp.ColomboUserManager;
import com.gaulatti.colombo.ftp.SessionData;
import com.gaulatti.colombo.ftp.SessionUploadCredentials;
import com.gaulatti.colombo.model.Tenant;
import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestTemplate;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

@ExtendWith(MockitoExtension.class)
class UploadServiceTest {

    @Mock
    private RestTemplate restTemplate;

    @Mock
    private ColomboUserManager userManager;

    private ConcurrentHashMap<String, SessionData> sessions;
    private UploadService uploadService;
    private Tenant tenant;

    @BeforeEach
    void setUp() {
        sessions = new ConcurrentHashMap<>();
        uploadService = new UploadService(sessions, restTemplate, userManager);
        tenant = tenant();
    }

    // ─────────────────────────── uploadToS3WithRefresh ────────────────────────────

    @Test
    void uploadToS3WithRefreshReturnsFailureWhenNoSession() throws Exception {
        File file = Files.createTempFile("colombo", ".jpg").toFile();

        UploadService.UploadResult result = uploadService.uploadToS3WithRefresh("missing", "file.jpg", file);
        assertFalse(result.success());
    }

    @Test
    void uploadToS3WithRefreshReturnsFailureWhenSessionHasNullTenant() throws Exception {
        sessions.put("bad", new SessionData(null, "assignment", validCredentials(), "key"));
        File file = Files.createTempFile("colombo", ".jpg").toFile();

        UploadService.UploadResult result = uploadService.uploadToS3WithRefresh("bad", "file.jpg", file);
        assertFalse(result.success());
    }

    @Test
    void uploadToS3WithRefreshReturnsFailureWhenCredentialsAreInvalid() throws Exception {
        sessions.put("acme-user", new SessionData(tenant, "assignment", null, "key"));
        File file = Files.createTempFile("colombo", ".jpg").toFile();

        UploadService.UploadResult result = uploadService.uploadToS3WithRefresh("acme-user", "file.jpg", file);
        assertFalse(result.success());
    }

    @Test
    void uploadToS3WithRefreshSucceedsOnHappyPath() throws Exception {
        sessions.put("acme-user", new SessionData(tenant, "assignment", validCredentials(), "key"));
        File file = Files.createTempFile("colombo", ".jpg").toFile();
        Files.writeString(file.toPath(), "x");

        S3Client s3Client = mock(S3Client.class);
        S3ClientBuilder builder = mockS3Builder(s3Client);

        try (MockedStatic<S3Client> s3Static = mockStatic(S3Client.class)) {
            s3Static.when(S3Client::builder).thenReturn(builder);
            UploadService.UploadResult result = uploadService.uploadToS3WithRefresh("acme-user", "file.jpg", file);
            assertTrue(result.success());
            assertNotNull(result.s3Url());
            assertNotNull(result.sessionData());
        }
    }

    @Test
    void uploadToS3WithRefreshRefreshesOnExpiredCredentials() throws Exception {
        sessions.put("acme-user", new SessionData(tenant, "assignment", validCredentials(), "key"));
        File file = Files.createTempFile("colombo", ".jpg").toFile();
        S3Exception expired = s3("ExpiredToken", 400);

        S3Client s3Client = mock(S3Client.class);
        S3ClientBuilder builder = mockS3Builder(s3Client);
        when(userManager.refreshSessionFromValidation("acme-user")).thenReturn(ColomboUserManager.RefreshResult.DENIED);
        doThrow(expired).when(s3Client).putObject(any(PutObjectRequest.class), any(RequestBody.class));

        try (MockedStatic<S3Client> s3Static = mockStatic(S3Client.class)) {
            s3Static.when(S3Client::builder).thenReturn(builder);
            UploadService.UploadResult result = uploadService.uploadToS3WithRefresh("acme-user", "file.jpg", file);
            assertFalse(result.success());
        }
        verify(userManager).evictSession(eq("acme-user"), any());
    }

    @Test
    void uploadToS3WithRefreshEvictsOnImmediateAccessDenied() throws Exception {
        sessions.put("acme-user", new SessionData(tenant, "assignment", validCredentials(), "key"));
        File file = Files.createTempFile("colombo", ".jpg").toFile();

        S3Client s3Client = mock(S3Client.class);
        S3ClientBuilder builder = mockS3Builder(s3Client);
        doThrow(s3("AccessDenied", 403)).when(s3Client).putObject(any(PutObjectRequest.class), any(RequestBody.class));

        try (MockedStatic<S3Client> s3Static = mockStatic(S3Client.class)) {
            s3Static.when(S3Client::builder).thenReturn(builder);
            assertThrows(S3Exception.class,
                    () -> uploadService.uploadToS3WithRefresh("acme-user", "file.jpg", file));
        }
        verify(userManager).evictSession(eq("acme-user"), eq("S3 upload denied"));
    }

    @Test
    void uploadToS3WithRefreshSucceedsOnRetryAfterRefresh() throws Exception {
        sessions.put("acme-user", new SessionData(tenant, "assignment", validCredentials(), "key"));
        File file = Files.createTempFile("colombo", ".jpg").toFile();
        S3Exception expired = s3("ExpiredToken", 400);
        AtomicInteger calls = new AtomicInteger(0);

        S3Client s3Client = mock(S3Client.class);
        S3ClientBuilder builder = mockS3Builder(s3Client);
        when(s3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class))).thenAnswer(inv -> {
            if (calls.getAndIncrement() == 0) throw expired;
            return PutObjectResponse.builder().build();
        });
        when(userManager.refreshSessionFromValidation("acme-user")).thenAnswer(inv -> {
            sessions.put("acme-user", new SessionData(tenant, "assignment-refreshed", validCredentials(), "key"));
            return ColomboUserManager.RefreshResult.REFRESHED;
        });

        try (MockedStatic<S3Client> s3Static = mockStatic(S3Client.class)) {
            s3Static.when(S3Client::builder).thenReturn(builder);
            UploadService.UploadResult result = uploadService.uploadToS3WithRefresh("acme-user", "file.jpg", file);
            assertTrue(result.success());
        }
    }

    @Test
    void uploadToS3WithRefreshEvictsWhenRetryStillExpired() throws Exception {
        sessions.put("acme-user", new SessionData(tenant, "assignment", validCredentials(), "key"));
        File file = Files.createTempFile("colombo", ".jpg").toFile();
        S3Exception expired = s3("ExpiredToken", 400);

        S3Client s3Client = mock(S3Client.class);
        S3ClientBuilder builder = mockS3Builder(s3Client);
        when(s3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
                .thenThrow(expired).thenThrow(expired);
        when(userManager.refreshSessionFromValidation("acme-user")).thenAnswer(inv -> {
            sessions.put("acme-user", new SessionData(tenant, "assignment-refreshed", validCredentials(), "key"));
            return ColomboUserManager.RefreshResult.REFRESHED;
        });

        try (MockedStatic<S3Client> s3Static = mockStatic(S3Client.class)) {
            s3Static.when(S3Client::builder).thenReturn(builder);
            assertThrows(S3Exception.class,
                    () -> uploadService.uploadToS3WithRefresh("acme-user", "file.jpg", file));
        }
        verify(userManager).evictSession(eq("acme-user"), eq("S3 denied after credential refresh"));
    }

    @Test
    void uploadToS3WithRefreshEvictsWhenRetryDeniedAfterRefresh() throws Exception {
        sessions.put("acme-user", new SessionData(tenant, "assignment", validCredentials(), "key"));
        File file = Files.createTempFile("colombo", ".jpg").toFile();
        S3Exception expired = s3("ExpiredToken", 400);
        S3Exception denied = s3("AccessDenied", 403);

        S3Client s3Client = mock(S3Client.class);
        S3ClientBuilder builder = mockS3Builder(s3Client);
        doThrow(expired).doThrow(denied).when(s3Client).putObject(any(PutObjectRequest.class), any(RequestBody.class));
        when(userManager.refreshSessionFromValidation("acme-user")).thenAnswer(inv -> {
            sessions.put("acme-user", new SessionData(tenant, "assignment3", validCredentials(), "key3"));
            return ColomboUserManager.RefreshResult.REFRESHED;
        });

        try (MockedStatic<S3Client> s3Static = mockStatic(S3Client.class)) {
            s3Static.when(S3Client::builder).thenReturn(builder);
            assertThrows(S3Exception.class,
                    () -> uploadService.uploadToS3WithRefresh("acme-user", "file.jpg", file));
        }
    }

    @Test
    void uploadToS3WithRefreshDoesNotEvictOnRetryNonDeniedNonExpiredError() throws Exception {
        sessions.put("acme-user", new SessionData(tenant, "assignment", validCredentials(), "key"));
        File file = Files.createTempFile("colombo", ".jpg").toFile();
        S3Exception expired = s3("ExpiredToken", 400);
        S3Exception other = s3("OtherError", 500);

        S3Client s3Client = mock(S3Client.class);
        S3ClientBuilder builder = mockS3Builder(s3Client);
        when(s3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
                .thenThrow(expired).thenThrow(other);
        when(userManager.refreshSessionFromValidation("acme-user")).thenAnswer(inv -> {
            sessions.put("acme-user", new SessionData(tenant, "assignment-refreshed", validCredentials(), "key"));
            return ColomboUserManager.RefreshResult.REFRESHED;
        });

        try (MockedStatic<S3Client> s3Static = mockStatic(S3Client.class)) {
            s3Static.when(S3Client::builder).thenReturn(builder);
            assertThrows(S3Exception.class,
                    () -> uploadService.uploadToS3WithRefresh("acme-user", "file.jpg", file));
        }
        verify(userManager, never()).evictSession("acme-user", "S3 denied after credential refresh");
    }

    @Test
    void uploadToS3WithRefreshFailsWhenRefreshedSessionHasNullSession() throws Exception {
        sessions.put("acme-user", new SessionData(tenant, "assignment", validCredentials(), "key"));
        File file = Files.createTempFile("colombo", ".jpg").toFile();
        S3Exception expired = s3("ExpiredToken", 400);

        S3Client s3Client = mock(S3Client.class);
        S3ClientBuilder builder = mockS3Builder(s3Client);
        doThrow(expired).when(s3Client).putObject(any(PutObjectRequest.class), any(RequestBody.class));
        when(userManager.refreshSessionFromValidation("acme-user")).thenAnswer(inv -> {
            sessions.remove("acme-user");
            return ColomboUserManager.RefreshResult.REFRESHED;
        });

        try (MockedStatic<S3Client> s3Static = mockStatic(S3Client.class)) {
            s3Static.when(S3Client::builder).thenReturn(builder);
            UploadService.UploadResult result = uploadService.uploadToS3WithRefresh("acme-user", "file.jpg", file);
            assertFalse(result.success());
        }
    }

    @Test
    void uploadToS3WithRefreshFailsWhenRefreshedSessionHasInvalidAssignment() throws Exception {
        sessions.put("acme-user", new SessionData(tenant, "assignment", validCredentials(), "key"));
        File file = Files.createTempFile("colombo", ".jpg").toFile();
        S3Exception expired = s3("ExpiredToken", 400);

        S3Client s3Client = mock(S3Client.class);
        S3ClientBuilder builder = mockS3Builder(s3Client);
        doThrow(expired).when(s3Client).putObject(any(PutObjectRequest.class), any(RequestBody.class));
        when(userManager.refreshSessionFromValidation("acme-user")).thenAnswer(inv -> {
            sessions.put("acme-user", new SessionData(tenant, "", validCredentials(), "key"));
            return ColomboUserManager.RefreshResult.REFRESHED;
        });

        try (MockedStatic<S3Client> s3Static = mockStatic(S3Client.class)) {
            s3Static.when(S3Client::builder).thenReturn(builder);
            UploadService.UploadResult result = uploadService.uploadToS3WithRefresh("acme-user", "file.jpg", file);
            assertFalse(result.success());
        }
    }

    @Test
    void uploadToS3WithRefreshThrowsOnNonS3Error() throws Exception {
        sessions.put("acme-user", new SessionData(tenant, "assignment", validCredentials(), "key"));
        File file = Files.createTempFile("colombo", ".jpg").toFile();

        S3Client s3Client = mock(S3Client.class);
        S3ClientBuilder builder = mockS3Builder(s3Client);
        doThrow(new RuntimeException("boom")).when(s3Client).putObject(any(PutObjectRequest.class), any(RequestBody.class));

        try (MockedStatic<S3Client> s3Static = mockStatic(S3Client.class)) {
            s3Static.when(S3Client::builder).thenReturn(builder);
            assertThrows(RuntimeException.class,
                    () -> uploadService.uploadToS3WithRefresh("acme-user", "file.jpg", file));
        }
    }

    // ─────────────────────────── processFtpUpload ────────────────────────────────

    @Test
    void processFtpUploadReturnsTrueOnHappyPath() throws Exception {
        sessions.put("acme-user", new SessionData(tenant, "assignment", validCredentials(), "key"));
        File file = Files.createTempFile("colombo", ".jpg").toFile();
        Files.writeString(file.toPath(), "x");

        S3Client s3Client = mock(S3Client.class);
        S3ClientBuilder builder = mockS3Builder(s3Client);
        when(restTemplate.exchange(eq(tenant.getPhotoEndpoint()), eq(HttpMethod.POST), any(), eq(Void.class)))
                .thenReturn(new ResponseEntity<>(HttpStatus.OK));

        try (MockedStatic<S3Client> s3Static = mockStatic(S3Client.class)) {
            s3Static.when(S3Client::builder).thenReturn(builder);
            assertTrue(uploadService.processFtpUpload("acme-user", "file.jpg", file));
        }
    }

    @Test
    void processFtpUploadReturnsFalseWhenS3UploadFails() throws Exception {
        File file = Files.createTempFile("colombo", ".jpg").toFile();
        assertFalse(uploadService.processFtpUpload("missing-user", "file.jpg", file));
    }

    // ─────────────────────────── postPhotoCallback ───────────────────────────────

    @Test
    void postPhotoCallbackSucceedsOnOk() throws Exception {
        when(restTemplate.exchange(eq(tenant.getPhotoEndpoint()), eq(HttpMethod.POST), any(), eq(Void.class)))
                .thenReturn(new ResponseEntity<>(HttpStatus.OK));
        uploadService.postPhotoCallback(tenant, "assignment", "s3://bucket/key", "acme-user");
    }

    @Test
    void postPhotoCallbackThrowsOn4xxAndEvictsSession() {
        when(restTemplate.exchange(eq(tenant.getPhotoEndpoint()), eq(HttpMethod.POST), any(), eq(Void.class)))
                .thenReturn(new ResponseEntity<>(HttpStatus.UNAUTHORIZED));
        assertThrows(IllegalStateException.class,
                () -> uploadService.postPhotoCallback(tenant, "assignment", "s3://bucket/key", "acme-user"));
        verify(userManager).evictSession(eq("acme-user"), any());
    }

    @Test
    void postPhotoCallbackThrowsOn5xxWithoutEviction() {
        when(restTemplate.exchange(eq(tenant.getPhotoEndpoint()), eq(HttpMethod.POST), any(), eq(Void.class)))
                .thenReturn(new ResponseEntity<>(HttpStatus.BAD_GATEWAY));
        assertThrows(IllegalStateException.class,
                () -> uploadService.postPhotoCallback(tenant, "assignment", "s3://bucket/key", "acme-user"));
        verify(userManager, never()).evictSession(any(), any());
    }

    @Test
    void postPhotoCallbackHandlesHttpClientErrorException() {
        when(restTemplate.exchange(eq(tenant.getPhotoEndpoint()), eq(HttpMethod.POST), any(), eq(Void.class)))
                .thenThrow(HttpClientErrorException.create(
                        HttpStatus.UNAUTHORIZED, "Unauthorized",
                        HttpHeaders.EMPTY, new byte[0], StandardCharsets.UTF_8));
        assertThrows(HttpClientErrorException.class,
                () -> uploadService.postPhotoCallback(tenant, "assignment", "s3://bucket/key", "acme-user"));
        verify(userManager).evictSession(eq("acme-user"), any());
    }

    @Test
    void postPhotoCallbackHandlesHttpServerErrorException() {
        when(restTemplate.exchange(eq(tenant.getPhotoEndpoint()), eq(HttpMethod.POST), any(), eq(Void.class)))
                .thenThrow(HttpServerErrorException.create(
                        HttpStatus.BAD_GATEWAY, "Bad",
                        HttpHeaders.EMPTY, new byte[0], StandardCharsets.UTF_8));
        assertThrows(HttpServerErrorException.class,
                () -> uploadService.postPhotoCallback(tenant, "assignment", "s3://bucket/key", "acme-user"));
    }

    // ─────────────────────────── uploadToS3 (public, stateless) ─────────────────

    @Test
    void uploadToS3UploadsFileAndReturnsS3Url() throws Exception {
        SessionData sessionData = new SessionData(tenant, "assignment", validCredentials(), "key");
        File file = Files.createTempFile("colombo", ".jpg").toFile();
        Files.writeString(file.toPath(), "x");

        S3Client s3Client = mock(S3Client.class);
        S3ClientBuilder builder = mockS3Builder(s3Client);

        try (MockedStatic<S3Client> s3Static = mockStatic(S3Client.class)) {
            s3Static.when(S3Client::builder).thenReturn(builder);
            String s3Url = uploadService.uploadToS3(sessionData, "acme-user", "file.jpg", file);
            assertTrue(s3Url.startsWith("s3://"));
            assertTrue(s3Url.contains("file.jpg"));
        }
    }

    // ─────────────────────────── private helper coverage ────────────────────────

    @Test
    void helperMethodsCoverBranches() throws Exception {
        assertEquals("k/file.jpg", invokePrivate("buildObjectKey", new Class[]{String.class, String.class}, "k", "file.jpg"));
        assertEquals("k/file.jpg", invokePrivate("buildObjectKey", new Class[]{String.class, String.class}, "k/", "file.jpg"));

        assertTrue((boolean) invokePrivate("isExpiredCredentialError", new Class[]{S3Exception.class}, s3("ExpiredToken", 400)));
        assertTrue((boolean) invokePrivate("isExpiredCredentialError", new Class[]{S3Exception.class}, s3("RequestExpired", 400)));
        assertTrue((boolean) invokePrivate("isExpiredCredentialError", new Class[]{S3Exception.class}, s3("InvalidToken", 400)));
        assertFalse((boolean) invokePrivate("isExpiredCredentialError", new Class[]{S3Exception.class}, s3("Other", 400)));

        S3Exception noDetails = mock(S3Exception.class);
        when(noDetails.statusCode()).thenReturn(400);
        when(noDetails.awsErrorDetails()).thenReturn(null);
        assertFalse((boolean) invokePrivate("isExpiredCredentialError", new Class[]{S3Exception.class}, noDetails));

        assertTrue((boolean) invokePrivate("isDeniedUploadError", new Class[]{S3Exception.class}, s3("Other", 403)));
        assertTrue((boolean) invokePrivate("isDeniedUploadError", new Class[]{S3Exception.class}, s3("AccessDenied", 400)));
        assertFalse((boolean) invokePrivate("isDeniedUploadError", new Class[]{S3Exception.class}, s3("Other", 400)));
        assertFalse((boolean) invokePrivate("isDeniedUploadError", new Class[]{S3Exception.class}, noDetails));

        assertEquals("unknown", invokePrivate("extractFilenameFromS3Url", new Class[]{String.class}, (Object) null));
        assertEquals("unknown", invokePrivate("extractFilenameFromS3Url", new Class[]{String.class}, "   "));
        assertEquals("unknown", invokePrivate("extractFilenameFromS3Url", new Class[]{String.class}, "noslash"));
        assertEquals("unknown", invokePrivate("extractFilenameFromS3Url", new Class[]{String.class}, "s3://bucket/path/"));
        assertEquals("file.jpg", invokePrivate("extractFilenameFromS3Url", new Class[]{String.class}, "s3://bucket/path/file.jpg"));
    }

    @Test
    void isValidSessionForUploadCoversBranches() throws Exception {
        SessionData blankAssignment = new SessionData(tenant, "   ", validCredentials(), "key");
        assertFalse((boolean) invokePrivate("isValidSessionForUpload", new Class[]{String.class, SessionData.class}, "acme-user", blankAssignment));

        SessionData nullAssignment = new SessionData(tenant, null, validCredentials(), "key");
        assertFalse((boolean) invokePrivate("isValidSessionForUpload", new Class[]{String.class, SessionData.class}, "acme-user", nullAssignment));

        SessionUploadCredentials invalidCreds = new SessionUploadCredentials("a", "b", "c", "d", "e", "f", "   ");
        SessionData invalidUpload = new SessionData(tenant, "assignment", invalidCreds, "key");
        assertFalse((boolean) invokePrivate("isValidSessionForUpload", new Class[]{String.class, SessionData.class}, "acme-user", invalidUpload));

        SessionData nullTenant = new SessionData(null, "assignment", validCredentials(), "key");
        assertFalse((boolean) invokePrivate("isValidSessionForUpload", new Class[]{String.class, SessionData.class}, "acme-user", nullTenant));

        SessionData valid = new SessionData(tenant, "assignment", validCredentials(), "key");
        assertTrue((boolean) invokePrivate("isValidSessionForUpload", new Class[]{String.class, SessionData.class}, "acme-user", valid));
    }

    // ─────────────────────────── helpers ─────────────────────────────────────────

    private S3ClientBuilder mockS3Builder(S3Client s3Client) {
        S3ClientBuilder builder = mock(S3ClientBuilder.class);
        when(builder.region(any())).thenReturn(builder);
        when(builder.credentialsProvider(any())).thenReturn(builder);
        when(builder.build()).thenReturn(s3Client);
        return builder;
    }

    private S3Exception s3(String code, int statusCode) {
        S3Exception exception = mock(S3Exception.class);
        Mockito.lenient().when(exception.statusCode()).thenReturn(statusCode);
        Mockito.lenient().when(exception.awsErrorDetails()).thenReturn(AwsErrorDetails.builder().errorCode(code).build());
        return exception;
    }

    private Object invokePrivate(String methodName, Class<?>[] types, Object... args) throws Exception {
        Method method = UploadService.class.getDeclaredMethod(methodName, types);
        method.setAccessible(true);
        return method.invoke(uploadService, args);
    }
}
