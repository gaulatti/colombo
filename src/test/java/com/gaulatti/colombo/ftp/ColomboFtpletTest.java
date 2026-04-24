package com.gaulatti.colombo.ftp;

import static com.gaulatti.colombo.TestFixtures.tenant;
import static com.gaulatti.colombo.TestFixtures.validCredentials;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
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

import com.gaulatti.colombo.model.Tenant;
import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ftpserver.ftplet.FileSystemView;
import org.apache.ftpserver.ftplet.FtpFile;
import org.apache.ftpserver.ftplet.FtpReply;
import org.apache.ftpserver.ftplet.FtpRequest;
import org.apache.ftpserver.ftplet.FtpSession;
import org.apache.ftpserver.ftplet.FtpletResult;
import org.apache.ftpserver.ftplet.User;
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
class ColomboFtpletTest {

    @Mock
    private RestTemplate restTemplate;

    @Mock
    private ColomboUserManager userManager;

    @Mock
    private FtpSession session;

    @Mock
    private FtpRequest request;

    @Mock
    private FtpReply reply;

    private ConcurrentHashMap<String, SessionData> sessions;
    private ColomboFtplet ftplet;
    private Tenant tenant;

    @BeforeEach
    void setUp() {
        sessions = new ConcurrentHashMap<>();
        ftplet = new ColomboFtplet(sessions, restTemplate, userManager);
        tenant = tenant();
    }

    @Test
    void lifecycleAndSimpleCallbacksReturnDefault() throws Exception {
        when(session.getSessionId()).thenReturn(UUID.randomUUID());
        when(session.getClientAddress()).thenReturn(new InetSocketAddress("127.0.0.1", 2121));
        when(request.getArgument()).thenReturn("file.jpg");
        when(request.getCommand()).thenReturn("NOOP");

        ftplet.init(null);
        ftplet.destroy();
        assertEquals(FtpletResult.DEFAULT, ftplet.onConnect(session));
        assertEquals(FtpletResult.DEFAULT, ftplet.onDisconnect(session));
        assertEquals(FtpletResult.DEFAULT, ftplet.beforeCommand(session, request));
        assertEquals(FtpletResult.DEFAULT, ftplet.onUploadStart(session, request));
        assertEquals(FtpletResult.DEFAULT, ftplet.onUploadUniqueStart(session, request));
        assertEquals(FtpletResult.DEFAULT, ftplet.onUploadUniqueEnd(session, request));
        assertEquals(FtpletResult.DEFAULT, ftplet.onAppendStart(session, request));
        assertEquals(FtpletResult.DEFAULT, ftplet.onAppendEnd(session, request));
        assertEquals(FtpletResult.DEFAULT, ftplet.onDownloadStart(session, request));
        assertEquals(FtpletResult.DEFAULT, ftplet.onDownloadEnd(session, request));
        assertEquals(FtpletResult.DEFAULT, ftplet.onLogin(session, request));

        when(reply.getCode()).thenReturn(500);
        assertEquals(FtpletResult.DEFAULT, ftplet.afterCommand(session, request, reply));
    }

    @Test
    void callbacksHandleNullRequestOrReplyArguments() {
        when(session.getSessionId()).thenReturn(UUID.randomUUID());

        assertEquals(FtpletResult.DEFAULT, ftplet.beforeCommand(session, null));
        assertEquals(FtpletResult.DEFAULT, ftplet.afterCommand(session, null, null));
        assertEquals(FtpletResult.DEFAULT, ftplet.onUploadStart(session, null));
        assertEquals(FtpletResult.DEFAULT, ftplet.onUploadUniqueStart(session, null));
        assertEquals(FtpletResult.DEFAULT, ftplet.onUploadUniqueEnd(session, null));
        assertEquals(FtpletResult.DEFAULT, ftplet.onAppendStart(session, null));
        assertEquals(FtpletResult.DEFAULT, ftplet.onAppendEnd(session, null));
        assertEquals(FtpletResult.DEFAULT, ftplet.onDownloadStart(session, null));
        assertEquals(FtpletResult.DEFAULT, ftplet.onDownloadEnd(session, null));
    }

    @Test
    void uploadFlowHandlesEarlyFailureBranches() throws Exception {
        when(request.getCommand()).thenReturn("STOR");
        when(request.getArgument()).thenReturn("file.jpg");
        when(reply.getCode()).thenReturn(226);
        when(session.getSessionId()).thenReturn(UUID.randomUUID());

        assertEquals(FtpletResult.DEFAULT, ftplet.afterCommand(session, request, reply));
        assertEquals(FtpletResult.DISCONNECT, ftplet.onUploadEnd(session, request));

        when(request.getCommand()).thenReturn("STOR");
        when(reply.getCode()).thenReturn(500);
        assertEquals(FtpletResult.DEFAULT, ftplet.afterCommand(session, request, reply));
    }

    @Test
    void duplicateUploadMarkerSkipsProcessing() throws Exception {
        when(request.getArgument()).thenReturn("file.jpg");
        when(request.getCommand()).thenReturn("STOR");
        when(reply.getCode()).thenReturn(226);
        when(session.getAttribute("colombo.upload.processed:file.jpg")).thenReturn(Boolean.TRUE);

        assertEquals(FtpletResult.DEFAULT, ftplet.afterCommand(session, request, reply));
        verify(session, never()).setAttribute(any(), any());
    }

    @Test
    void onUploadEndDisconnectsWhenSessionDataMissing() {
        mockLoggedInUser("acme-user");
        when(request.getArgument()).thenReturn("file.jpg");

        assertEquals(FtpletResult.DISCONNECT, ftplet.onUploadEnd(session, request));
    }

    @Test
    void onUploadEndDisconnectsWhenTenantMissing() {
        mockLoggedInUser("acme-user");
        sessions.put("acme-user", new SessionData(null, "assignment", validCredentials(), "key"));
        when(request.getArgument()).thenReturn("file.jpg");

        assertEquals(FtpletResult.DISCONNECT, ftplet.onUploadEnd(session, request));
    }

    @Test
    void onUploadEndDisconnectsWhenPhysicalFileCannotBeResolved() throws Exception {
        mockLoggedInUser("acme-user");
        sessions.put("acme-user", new SessionData(tenant, "assignment", validCredentials(), "key"));
        when(request.getArgument()).thenReturn("file.jpg");
        when(session.getFileSystemView()).thenReturn(null);

        assertEquals(FtpletResult.DISCONNECT, ftplet.onUploadEnd(session, request));
    }

    @Test
    void onUploadEndCompletesHappyPathAndMarksUploadProcessed() throws Exception {
        String username = "acme-user";
        mockLoggedInUser(username);
        SessionData sessionData = new SessionData(tenant, "assignment-1", validCredentials(), "key");
        sessions.put(username, sessionData);

        File file = Files.createTempFile("colombo", ".jpg").toFile();
        Files.writeString(file.toPath(), "x");

        when(request.getArgument()).thenReturn(file.getName());
        when(restTemplate.exchange(eq(tenant.getPhotoEndpoint()), eq(HttpMethod.POST), any(), eq(Void.class)))
                .thenReturn(new ResponseEntity<>(HttpStatus.OK));

        mockPhysicalFile(file);

        S3Client s3Client = mock(S3Client.class);
        S3ClientBuilder builder = mock(S3ClientBuilder.class);
        when(builder.region(any())).thenReturn(builder);
        when(builder.credentialsProvider(any())).thenReturn(builder);
        when(builder.build()).thenReturn(s3Client);

        try (MockedStatic<S3Client> s3Static = mockStatic(S3Client.class)) {
            s3Static.when(S3Client::builder).thenReturn(builder);
            FtpletResult result = ftplet.onUploadEnd(session, request);
            assertEquals(FtpletResult.DEFAULT, result);
        }

        verify(session).setAttribute(eq("colombo.upload.processed:" + file.getName()), eq(Boolean.TRUE));
    }

    @Test
    void uploadToS3WithRefreshHandlesSessionValidationAndS3Errors() throws Exception {
        Method method = ColomboFtplet.class.getDeclaredMethod("uploadToS3WithRefresh", String.class, String.class, File.class);
        method.setAccessible(true);

        File file = Files.createTempFile("colombo", ".jpg").toFile();

        Object noSession = method.invoke(ftplet, "missing", "file.jpg", file);
        assertFalse((boolean) invokeUploadResultMethod(noSession, "success"));

        sessions.put("bad", new SessionData(null, "assignment", validCredentials(), "key"));
        Object invalidSession = method.invoke(ftplet, "bad", "file.jpg", file);
        assertFalse((boolean) invokeUploadResultMethod(invalidSession, "success"));

        SessionData validSession = new SessionData(tenant, "assignment", validCredentials(), "key");
        sessions.put("acme-user", validSession);

        S3Client s3Client = mock(S3Client.class);
        S3ClientBuilder builder = mock(S3ClientBuilder.class);
        when(builder.region(any())).thenReturn(builder);
        when(builder.credentialsProvider(any())).thenReturn(builder);
        when(builder.build()).thenReturn(s3Client);

        try (MockedStatic<S3Client> s3Static = mockStatic(S3Client.class)) {
            s3Static.when(S3Client::builder).thenReturn(builder);
            Object success = method.invoke(ftplet, "acme-user", "file.jpg", file);
            assertTrue((boolean) invokeUploadResultMethod(success, "success"));
            assertNotNull(invokeUploadResultMethod(success, "s3Url"));
        }

        S3Exception expired = s3("ExpiredToken", 400);
        doThrow(expired).when(s3Client).putObject(any(PutObjectRequest.class), any(RequestBody.class));
        when(userManager.refreshSessionFromValidation("acme-user"))
                .thenReturn(ColomboUserManager.RefreshResult.DENIED);

        try (MockedStatic<S3Client> s3Static = mockStatic(S3Client.class)) {
            s3Static.when(S3Client::builder).thenReturn(builder);
            Object failed = method.invoke(ftplet, "acme-user", "file.jpg", file);
            assertFalse((boolean) invokeUploadResultMethod(failed, "success"));
        }

        verify(userManager).evictSession(eq("acme-user"), any());
    }

    @Test
    void uploadToS3WithRefreshCoversRefreshRetryBranches() throws Exception {
        Method method = ColomboFtplet.class.getDeclaredMethod("uploadToS3WithRefresh", String.class, String.class, File.class);
        method.setAccessible(true);
        File file = Files.createTempFile("colombo", ".jpg").toFile();
        SessionData validSession = new SessionData(tenant, "assignment", validCredentials(), "key");
        sessions.put("acme-user", validSession);

        S3Client s3Client = mock(S3Client.class);
        S3ClientBuilder builder = mock(S3ClientBuilder.class);
        when(builder.region(any())).thenReturn(builder);
        when(builder.credentialsProvider(any())).thenReturn(builder);
        when(builder.build()).thenReturn(s3Client);

        S3Exception expired = s3("ExpiredToken", 400);
        S3Exception denied = s3("AccessDenied", 403);

        doThrow(expired).when(s3Client).putObject(any(PutObjectRequest.class), any(RequestBody.class));
        when(userManager.refreshSessionFromValidation("acme-user")).thenAnswer(invocation -> {
            sessions.put("acme-user", new SessionData(tenant, "", validCredentials(), "key"));
            return ColomboUserManager.RefreshResult.REFRESHED;
        });
        try (MockedStatic<S3Client> s3Static = mockStatic(S3Client.class)) {
            s3Static.when(S3Client::builder).thenReturn(builder);
            Object failed = method.invoke(ftplet, "acme-user", "file.jpg", file);
            assertFalse((boolean) invokeUploadResultMethod(failed, "success"));
        }

        sessions.put("acme-user", new SessionData(tenant, "assignment", validCredentials(), "key"));
        Mockito.reset(s3Client);
        when(s3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
                .thenThrow(expired)
                .thenReturn(PutObjectResponse.builder().build())
                .thenThrow(expired);
        when(userManager.refreshSessionFromValidation("acme-user")).thenAnswer(invocation -> {
            sessions.put("acme-user", new SessionData(tenant, "assignment2", validCredentials(), "key2"));
            return ColomboUserManager.RefreshResult.REFRESHED;
        });
        try (MockedStatic<S3Client> s3Static = mockStatic(S3Client.class)) {
            s3Static.when(S3Client::builder).thenReturn(builder);
            Object retriedSuccess = method.invoke(ftplet, "acme-user", "file.jpg", file);
            assertNotNull(retriedSuccess);
        }

        sessions.put("acme-user", new SessionData(tenant, "assignment", validCredentials(), "key"));
        Mockito.reset(s3Client);
        doThrow(expired).doThrow(denied).when(s3Client).putObject(any(PutObjectRequest.class), any(RequestBody.class));
        when(userManager.refreshSessionFromValidation("acme-user")).thenAnswer(invocation -> {
            sessions.put("acme-user", new SessionData(tenant, "assignment3", validCredentials(), "key3"));
            return ColomboUserManager.RefreshResult.REFRESHED;
        });
        try (MockedStatic<S3Client> s3Static = mockStatic(S3Client.class)) {
            s3Static.when(S3Client::builder).thenReturn(builder);
            assertThrows(InvocationTargetException.class,
                    () -> method.invoke(ftplet, "acme-user", "file.jpg", file));
        }

        sessions.put("acme-user", new SessionData(tenant, "assignment", validCredentials(), "key"));
        Mockito.reset(s3Client);
        doThrow(s3("OtherError", 500)).when(s3Client).putObject(any(PutObjectRequest.class), any(RequestBody.class));
        try (MockedStatic<S3Client> s3Static = mockStatic(S3Client.class)) {
            s3Static.when(S3Client::builder).thenReturn(builder);
            assertThrows(InvocationTargetException.class,
                    () -> method.invoke(ftplet, "acme-user", "file.jpg", file));
        }
    }

    @Test
    void processUploadHandlesUploadFailureAndUnexpectedExceptions() throws Exception {
        String username = "acme-user";
        mockLoggedInUser(username);
        File file = Files.createTempFile("colombo", ".jpg").toFile();
        Files.writeString(file.toPath(), "x");
        when(request.getArgument()).thenReturn(file.getName());
        mockPhysicalFile(file);

        sessions.put(username, new SessionData(tenant, "", validCredentials(), "key"));
        assertEquals(FtpletResult.DISCONNECT, ftplet.onUploadEnd(session, request));

        sessions.put(username, new SessionData(tenant, "assignment", validCredentials(), "key"));
        S3Client s3Client = mock(S3Client.class);
        S3ClientBuilder builder = mock(S3ClientBuilder.class);
        when(builder.region(any())).thenReturn(builder);
        when(builder.credentialsProvider(any())).thenReturn(builder);
        when(builder.build()).thenReturn(s3Client);
        doThrow(new RuntimeException("boom"))
                .when(s3Client).putObject(any(PutObjectRequest.class), any(RequestBody.class));
        try (MockedStatic<S3Client> s3Static = mockStatic(S3Client.class)) {
            s3Static.when(S3Client::builder).thenReturn(builder);
            assertEquals(FtpletResult.DISCONNECT, ftplet.onUploadEnd(session, request));
        }
    }

    @Test
    void postPhotoCallbackCoversStatusAndExceptionBranches() throws Exception {
        Method method = ColomboFtplet.class.getDeclaredMethod(
                "postPhotoCallback",
                Tenant.class,
                String.class,
                String.class,
                String.class
        );
        method.setAccessible(true);

        when(restTemplate.exchange(eq(tenant.getPhotoEndpoint()), eq(HttpMethod.POST), any(), eq(Void.class)))
                .thenReturn(new ResponseEntity<>(HttpStatus.OK));
        method.invoke(ftplet, tenant, "assignment", "s3://bucket/key", "acme-user");

        when(restTemplate.exchange(eq(tenant.getPhotoEndpoint()), eq(HttpMethod.POST), any(), eq(Void.class)))
                .thenReturn(new ResponseEntity<>(HttpStatus.UNAUTHORIZED));
        assertThrows(InvocationTargetException.class,
                () -> method.invoke(ftplet, tenant, "assignment", "s3://bucket/key", "acme-user"));

        when(restTemplate.exchange(eq(tenant.getPhotoEndpoint()), eq(HttpMethod.POST), any(), eq(Void.class)))
                .thenReturn(new ResponseEntity<>(HttpStatus.BAD_GATEWAY));
        assertThrows(InvocationTargetException.class,
                () -> method.invoke(ftplet, tenant, "assignment", "s3://bucket/key", "acme-user"));

        when(restTemplate.exchange(eq(tenant.getPhotoEndpoint()), eq(HttpMethod.POST), any(), eq(Void.class)))
                .thenThrow(HttpClientErrorException.create(
                        HttpStatus.UNAUTHORIZED,
                        "Unauthorized",
                        HttpHeaders.EMPTY,
                        new byte[0],
                        StandardCharsets.UTF_8
                ));
        assertThrows(InvocationTargetException.class,
                () -> method.invoke(ftplet, tenant, "assignment", "s3://bucket/key", "acme-user"));

        when(restTemplate.exchange(eq(tenant.getPhotoEndpoint()), eq(HttpMethod.POST), any(), eq(Void.class)))
                .thenThrow(HttpServerErrorException.create(
                        HttpStatus.BAD_GATEWAY,
                        "Bad",
                        HttpHeaders.EMPTY,
                        new byte[0],
                        StandardCharsets.UTF_8
                ));
        assertThrows(InvocationTargetException.class,
                () -> method.invoke(ftplet, tenant, "assignment", "s3://bucket/key", "acme-user"));
    }

    @Test
    void helperMethodsCoverBranches() throws Exception {
        assertNull(invoke(ftplet, "extractUsername", new Class[]{FtpSession.class}, new Object[]{null}));
        assertEquals("unknown", invoke(ftplet, "extractFilename", new Class[]{String.class}, new Object[]{""}));
        assertEquals("a.jpg", invoke(ftplet, "extractFilename", new Class[]{String.class}, new Object[]{"x/a.jpg"}));
        assertEquals("unknown", invoke(ftplet, "extractFilename", new Class[]{String.class}, new Object[]{null}));

        assertNull(invoke(ftplet, "resolvePhysicalFile", new Class[]{FtpSession.class, String.class}, new Object[]{null, "a"}));
        assertNull(invoke(ftplet, "resolvePhysicalFile", new Class[]{FtpSession.class, String.class}, new Object[]{session, " "}));
        assertNull(invoke(ftplet, "resolvePhysicalFile", new Class[]{FtpSession.class, String.class}, new Object[]{session, null}));
        FileSystemView view = mock(FileSystemView.class);
        FtpFile ftpFile = mock(FtpFile.class);
        when(session.getFileSystemView()).thenReturn(view);
        when(view.getFile("x")).thenReturn(ftpFile);
        when(ftpFile.getPhysicalFile()).thenReturn("not-a-file");
        assertNull(invoke(ftplet, "resolvePhysicalFile", new Class[]{FtpSession.class, String.class}, new Object[]{session, "x"}));
        when(view.getFile("broken")).thenThrow(new org.apache.ftpserver.ftplet.FtpException("nope"));
        assertNull(invoke(ftplet, "resolvePhysicalFile", new Class[]{FtpSession.class, String.class}, new Object[]{session, "broken"}));

        when(request.getCommand()).thenReturn(null);
        assertFalse((boolean) invoke(ftplet, "isStorCommand", new Class[]{FtpRequest.class}, new Object[]{request}));
        when(request.getCommand()).thenReturn("STOR");
        assertTrue((boolean) invoke(ftplet, "isStorCommand", new Class[]{FtpRequest.class}, new Object[]{request}));

        assertFalse((boolean) invoke(ftplet, "isSuccessfulUploadReply", new Class[]{FtpReply.class}, new Object[]{null}));
        when(reply.getCode()).thenReturn(199);
        assertFalse((boolean) invoke(ftplet, "isSuccessfulUploadReply", new Class[]{FtpReply.class}, new Object[]{reply}));
        when(reply.getCode()).thenReturn(200);
        assertTrue((boolean) invoke(ftplet, "isSuccessfulUploadReply", new Class[]{FtpReply.class}, new Object[]{reply}));
        when(reply.getCode()).thenReturn(299);
        assertTrue((boolean) invoke(ftplet, "isSuccessfulUploadReply", new Class[]{FtpReply.class}, new Object[]{reply}));
        when(reply.getCode()).thenReturn(300);
        assertFalse((boolean) invoke(ftplet, "isSuccessfulUploadReply", new Class[]{FtpReply.class}, new Object[]{reply}));

        assertFalse((boolean) invoke(ftplet, "isUploadAlreadyProcessed", new Class[]{FtpSession.class, String.class}, new Object[]{null, "k"}));
        when(session.getAttribute("k")).thenReturn(Boolean.TRUE);
        assertTrue((boolean) invoke(ftplet, "isUploadAlreadyProcessed", new Class[]{FtpSession.class, String.class}, new Object[]{session, "k"}));
        when(session.getAttribute("k")).thenReturn("x");
        assertFalse((boolean) invoke(ftplet, "isUploadAlreadyProcessed", new Class[]{FtpSession.class, String.class}, new Object[]{session, "k"}));
        when(session.getAttribute("k")).thenReturn(Boolean.FALSE);
        assertFalse((boolean) invoke(ftplet, "isUploadAlreadyProcessed", new Class[]{FtpSession.class, String.class}, new Object[]{session, "k"}));

        invoke(ftplet, "markUploadProcessed", new Class[]{FtpSession.class, String.class}, new Object[]{session, "mk"});
        verify(session).setAttribute("mk", Boolean.TRUE);
        invoke(ftplet, "markUploadProcessed", new Class[]{FtpSession.class, String.class}, new Object[]{null, "mk"});

        assertEquals("k/file.jpg", invoke(ftplet, "buildObjectKey", new Class[]{String.class, String.class}, new Object[]{"k", "file.jpg"}));
        assertEquals("k/file.jpg", invoke(ftplet, "buildObjectKey", new Class[]{String.class, String.class}, new Object[]{"k/", "file.jpg"}));

        assertTrue((boolean) invoke(ftplet, "isExpiredCredentialError", new Class[]{S3Exception.class}, new Object[]{s3("ExpiredToken", 400)}));
        assertTrue((boolean) invoke(ftplet, "isExpiredCredentialError", new Class[]{S3Exception.class}, new Object[]{s3("RequestExpired", 400)}));
        assertTrue((boolean) invoke(ftplet, "isExpiredCredentialError", new Class[]{S3Exception.class}, new Object[]{s3("InvalidToken", 400)}));
        assertFalse((boolean) invoke(ftplet, "isExpiredCredentialError", new Class[]{S3Exception.class}, new Object[]{s3("Other", 400)}));
        S3Exception noDetails = mock(S3Exception.class);
        when(noDetails.statusCode()).thenReturn(400);
        when(noDetails.awsErrorDetails()).thenReturn(null);
        assertFalse((boolean) invoke(ftplet, "isExpiredCredentialError", new Class[]{S3Exception.class}, new Object[]{noDetails}));

        assertTrue((boolean) invoke(ftplet, "isDeniedUploadError", new Class[]{S3Exception.class}, new Object[]{s3("Other", 403)}));
        assertTrue((boolean) invoke(ftplet, "isDeniedUploadError", new Class[]{S3Exception.class}, new Object[]{s3("AccessDenied", 400)}));
        assertFalse((boolean) invoke(ftplet, "isDeniedUploadError", new Class[]{S3Exception.class}, new Object[]{s3("Other", 400)}));
        assertFalse((boolean) invoke(ftplet, "isDeniedUploadError", new Class[]{S3Exception.class}, new Object[]{noDetails}));
    }

    @Test
    void processUploadHandlesNullRequestAndNullSession() throws Exception {
        Method processUpload = ColomboFtplet.class.getDeclaredMethod(
                "processUpload",
                FtpSession.class,
                FtpRequest.class,
                String.class,
                boolean.class
        );
        processUpload.setAccessible(true);

        FtpletResult fromNullSession = (FtpletResult) processUpload.invoke(ftplet, null, null, "source", false);
        assertEquals(FtpletResult.DEFAULT, fromNullSession);

        when(session.getUser()).thenReturn(null);
        FtpletResult disconnect = (FtpletResult) processUpload.invoke(ftplet, session, null, "source", true);
        assertEquals(FtpletResult.DISCONNECT, disconnect);

        User blankUser = mock(User.class);
        when(blankUser.getName()).thenReturn("   ");
        when(session.getUser()).thenReturn(blankUser);
        FtpletResult blankUsername = (FtpletResult) processUpload.invoke(ftplet, session, request, "source", true);
        assertEquals(FtpletResult.DISCONNECT, blankUsername);
    }

    @Test
    void uploadToS3WithRefreshFailsWhenCredentialsAreInvalid() throws Exception {
        Method method = ColomboFtplet.class.getDeclaredMethod("uploadToS3WithRefresh", String.class, String.class, File.class);
        method.setAccessible(true);
        File file = Files.createTempFile("colombo", ".jpg").toFile();
        sessions.put("acme-user", new SessionData(tenant, "assignment", null, "key"));

        Object failed = method.invoke(ftplet, "acme-user", "file.jpg", file);
        assertFalse((boolean) invokeUploadResultMethod(failed, "success"));
    }

    @Test
    void helperMethodsCoverNewFilenameRedactionBranches() throws Exception {
        assertEquals("unknown",
                invoke(ftplet, "extractFilenameFromS3Url", new Class[]{String.class}, new Object[]{null}));
        assertEquals("unknown",
                invoke(ftplet, "extractFilenameFromS3Url", new Class[]{String.class}, new Object[]{"   "}));
        assertEquals("unknown",
                invoke(ftplet, "extractFilenameFromS3Url", new Class[]{String.class}, new Object[]{"noslash"}));
        assertEquals("unknown",
                invoke(ftplet, "extractFilenameFromS3Url", new Class[]{String.class}, new Object[]{"s3://bucket/path/"}));
        assertEquals("file.jpg",
                invoke(ftplet, "extractFilenameFromS3Url", new Class[]{String.class}, new Object[]{"s3://bucket/path/file.jpg"}));
    }

    @Test
    void onUploadEndDisconnectsWhenPhysicalFileDoesNotExistOrIsDirectory() throws Exception {
        String username = "acme-user";
        mockLoggedInUser(username);
        sessions.put(username, new SessionData(tenant, "assignment", validCredentials(), "key"));

        File missing = new File("/tmp/definitely-missing-colombo-file.jpg");
        when(request.getArgument()).thenReturn(missing.getName());
        mockPhysicalFile(missing);
        assertEquals(FtpletResult.DISCONNECT, ftplet.onUploadEnd(session, request));

        File directory = Files.createTempDirectory("colombo-dir").toFile();
        when(request.getArgument()).thenReturn(directory.getName());
        mockPhysicalFile(directory);
        assertEquals(FtpletResult.DISCONNECT, ftplet.onUploadEnd(session, request));
    }

    @Test
    void uploadToS3WithRefreshCoversNullRefreshedSessionAndRetryExpiredBranch() throws Exception {
        Method method = ColomboFtplet.class.getDeclaredMethod("uploadToS3WithRefresh", String.class, String.class, File.class);
        method.setAccessible(true);
        File file = Files.createTempFile("colombo", ".jpg").toFile();
        sessions.put("acme-user", new SessionData(tenant, "assignment", validCredentials(), "key"));

        S3Client s3Client = mock(S3Client.class);
        S3ClientBuilder builder = mock(S3ClientBuilder.class);
        when(builder.region(any())).thenReturn(builder);
        when(builder.credentialsProvider(any())).thenReturn(builder);
        when(builder.build()).thenReturn(s3Client);

        S3Exception expired = s3("ExpiredToken", 400);

        // Branch: refresh says refreshed, but session map has null afterwards.
        doThrow(expired).when(s3Client).putObject(any(PutObjectRequest.class), any(RequestBody.class));
        when(userManager.refreshSessionFromValidation("acme-user")).thenAnswer(invocation -> {
            sessions.remove("acme-user");
            return ColomboUserManager.RefreshResult.REFRESHED;
        });
        try (MockedStatic<S3Client> s3Static = mockStatic(S3Client.class)) {
            s3Static.when(S3Client::builder).thenReturn(builder);
            Object failed = method.invoke(ftplet, "acme-user", "file.jpg", file);
            assertFalse((boolean) invokeUploadResultMethod(failed, "success"));
        }

    }

    @Test
    void uploadToS3WithRefreshEvictsOnImmediateAccessDenied() throws Exception {
        Method method = ColomboFtplet.class.getDeclaredMethod("uploadToS3WithRefresh", String.class, String.class, File.class);
        method.setAccessible(true);
        File file = Files.createTempFile("colombo", ".jpg").toFile();
        sessions.put("acme-user", new SessionData(tenant, "assignment", validCredentials(), "key"));

        S3Client s3Client = mock(S3Client.class);
        S3ClientBuilder builder = mock(S3ClientBuilder.class);
        when(builder.region(any())).thenReturn(builder);
        when(builder.credentialsProvider(any())).thenReturn(builder);
        when(builder.build()).thenReturn(s3Client);

        doThrow(s3("AccessDenied", 403)).when(s3Client).putObject(any(PutObjectRequest.class), any(RequestBody.class));

        try (MockedStatic<S3Client> s3Static = mockStatic(S3Client.class)) {
            s3Static.when(S3Client::builder).thenReturn(builder);
            assertThrows(InvocationTargetException.class,
                    () -> method.invoke(ftplet, "acme-user", "file.jpg", file));
        }

        verify(userManager).evictSession(eq("acme-user"), eq("S3 upload denied"));
    }

    @Test
    void uploadToS3WithRefreshEvictsWhenRetryStillExpired() throws Exception {
        Method method = ColomboFtplet.class.getDeclaredMethod("uploadToS3WithRefresh", String.class, String.class, File.class);
        method.setAccessible(true);
        File file = Files.createTempFile("colombo", ".jpg").toFile();
        sessions.put("acme-user", new SessionData(tenant, "assignment", validCredentials(), "key"));

        S3Client s3Client = mock(S3Client.class);
        S3ClientBuilder builder = mock(S3ClientBuilder.class);
        when(builder.region(any())).thenReturn(builder);
        when(builder.credentialsProvider(any())).thenReturn(builder);
        when(builder.build()).thenReturn(s3Client);

        S3Exception expired = s3("ExpiredToken", 400);
        when(s3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
                .thenThrow(expired)
                .thenThrow(expired);
        when(userManager.refreshSessionFromValidation("acme-user")).thenAnswer(invocation -> {
            sessions.put("acme-user", new SessionData(tenant, "assignment-refreshed", validCredentials(), "key"));
            return ColomboUserManager.RefreshResult.REFRESHED;
        });

        try (MockedStatic<S3Client> s3Static = mockStatic(S3Client.class)) {
            s3Static.when(S3Client::builder).thenReturn(builder);
            assertThrows(InvocationTargetException.class,
                    () -> method.invoke(ftplet, "acme-user", "file.jpg", file));
        }

        verify(userManager).evictSession(eq("acme-user"), eq("S3 denied after credential refresh"));
    }

    @Test
    void uploadToS3WithRefreshCoversRetrySuccessAndRetryExpiredPaths() throws Exception {
        Method method = ColomboFtplet.class.getDeclaredMethod("uploadToS3WithRefresh", String.class, String.class, File.class);
        method.setAccessible(true);
        File file = Files.createTempFile("colombo", ".jpg").toFile();
        S3Exception expired = s3("ExpiredToken", 400);

        S3Client s3Client = mock(S3Client.class);
        S3ClientBuilder builder = mock(S3ClientBuilder.class);
        when(builder.region(any())).thenReturn(builder);
        when(builder.credentialsProvider(any())).thenReturn(builder);
        when(builder.build()).thenReturn(s3Client);

        // Retry success path (covers UploadResult.success(...) return branch in refresh block).
        sessions.put("acme-user", new SessionData(tenant, "assignment", validCredentials(), "key"));
        AtomicInteger calls = new AtomicInteger(0);
        when(s3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class))).thenAnswer(inv -> {
            if (calls.getAndIncrement() == 0) {
                throw expired;
            }
            return PutObjectResponse.builder().build();
        });
        when(userManager.refreshSessionFromValidation("acme-user")).thenAnswer(invocation -> {
            sessions.put("acme-user", new SessionData(tenant, "assignment-refreshed", validCredentials(), "key"));
            return ColomboUserManager.RefreshResult.REFRESHED;
        });
        try (MockedStatic<S3Client> s3Static = mockStatic(S3Client.class)) {
            s3Static.when(S3Client::builder).thenReturn(builder);
            Object success = method.invoke(ftplet, "acme-user", "file.jpg", file);
            assertTrue((boolean) invokeUploadResultMethod(success, "success"));
        }

        // Retry still expired path (covers isExpiredCredentialError branch in retry-exception OR).
        sessions.put("acme-user", new SessionData(tenant, "assignment", validCredentials(), "key"));
        AtomicInteger calls2 = new AtomicInteger(0);
        when(s3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class))).thenAnswer(inv -> {
            calls2.getAndIncrement();
            throw expired;
        });
        when(userManager.refreshSessionFromValidation("acme-user")).thenAnswer(invocation -> {
            sessions.put("acme-user", new SessionData(tenant, "assignment-refreshed", validCredentials(), "key"));
            return ColomboUserManager.RefreshResult.REFRESHED;
        });
        try (MockedStatic<S3Client> s3Static = mockStatic(S3Client.class)) {
            s3Static.when(S3Client::builder).thenReturn(builder);
            assertThrows(InvocationTargetException.class,
                    () -> method.invoke(ftplet, "acme-user", "file.jpg", file));
        }
    }

    @Test
    void helperMethodsCoverResolvePhysicalFileAndSessionValidationEdgeBranches() throws Exception {
        // resolvePhysicalFile: ensure argument.isBlank() is evaluated with a non-null fileSystemView.
        FileSystemView view = mock(FileSystemView.class);
        when(session.getFileSystemView()).thenReturn(view);
        assertNull(invoke(ftplet, "resolvePhysicalFile", new Class[]{FtpSession.class, String.class}, new Object[]{session, "   "}));
        assertNull(invoke(ftplet, "resolvePhysicalFile", new Class[]{FtpSession.class, String.class}, new Object[]{session, null}));

        Method validSessionMethod = ColomboFtplet.class.getDeclaredMethod("isValidSessionForUpload", String.class, SessionData.class);
        validSessionMethod.setAccessible(true);

        SessionData blankAssignment = new SessionData(tenant, "   ", validCredentials(), "key");
        assertFalse((boolean) validSessionMethod.invoke(ftplet, "acme-user", blankAssignment));
        SessionData nullAssignment = new SessionData(tenant, null, validCredentials(), "key");
        assertFalse((boolean) validSessionMethod.invoke(ftplet, "acme-user", nullAssignment));

        SessionUploadCredentials invalidCredentials = new SessionUploadCredentials(
                "access", "secret", "token", "us-east-1", "bucket", "prefix", "   "
        );
        SessionData invalidUpload = new SessionData(tenant, "assignment", invalidCredentials, "key");
        assertFalse((boolean) validSessionMethod.invoke(ftplet, "acme-user", invalidUpload));
    }

    @Test
    void uploadToS3WithRefreshDoesNotEvictOnRetryNonDeniedNonExpiredError() throws Exception {
        Method method = ColomboFtplet.class.getDeclaredMethod("uploadToS3WithRefresh", String.class, String.class, File.class);
        method.setAccessible(true);
        File file = Files.createTempFile("colombo", ".jpg").toFile();
        sessions.put("acme-user", new SessionData(tenant, "assignment", validCredentials(), "key"));

        S3Client s3Client = mock(S3Client.class);
        S3ClientBuilder builder = mock(S3ClientBuilder.class);
        when(builder.region(any())).thenReturn(builder);
        when(builder.credentialsProvider(any())).thenReturn(builder);
        when(builder.build()).thenReturn(s3Client);

        S3Exception expired = s3("ExpiredToken", 400);
        S3Exception other = s3("OtherError", 500);
        when(s3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
                .thenThrow(expired)
                .thenThrow(other);
        when(userManager.refreshSessionFromValidation("acme-user")).thenAnswer(invocation -> {
            sessions.put("acme-user", new SessionData(tenant, "assignment-refreshed", validCredentials(), "key"));
            return ColomboUserManager.RefreshResult.REFRESHED;
        });

        try (MockedStatic<S3Client> s3Static = mockStatic(S3Client.class)) {
            s3Static.when(S3Client::builder).thenReturn(builder);
            assertThrows(InvocationTargetException.class,
                    () -> method.invoke(ftplet, "acme-user", "file.jpg", file));
        }

        verify(userManager, never()).evictSession("acme-user", "S3 denied after credential refresh");
    }

    private void mockLoggedInUser(String username) {
        User user = mock(User.class);
        when(user.getName()).thenReturn(username);
        when(session.getUser()).thenReturn(user);
        when(session.getSessionId()).thenReturn(UUID.randomUUID());
    }

    private void mockPhysicalFile(File file) throws Exception {
        FileSystemView view = mock(FileSystemView.class);
        FtpFile ftpFile = mock(FtpFile.class);

        when(session.getFileSystemView()).thenReturn(view);
        when(view.getFile(file.getName())).thenReturn(ftpFile);
        when(ftpFile.getPhysicalFile()).thenReturn(file);
    }

    private Object invoke(Object target, String methodName, Class<?>[] types, Object[] args) throws Exception {
        Method method = ColomboFtplet.class.getDeclaredMethod(methodName, types);
        method.setAccessible(true);
        return method.invoke(target, args);
    }

    private Object invokeUploadResultMethod(Object uploadResult, String methodName) throws Exception {
        Method method = uploadResult.getClass().getDeclaredMethod(methodName);
        method.setAccessible(true);
        return method.invoke(uploadResult);
    }

    private S3Exception s3(String code, int statusCode) {
        S3Exception exception = mock(S3Exception.class);
        Mockito.lenient().when(exception.statusCode()).thenReturn(statusCode);
        Mockito.lenient().when(exception.awsErrorDetails()).thenReturn(AwsErrorDetails.builder().errorCode(code).build());
        return exception;
    }
}
