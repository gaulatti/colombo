package com.gaulatti.colombo.ftp;

import static com.gaulatti.colombo.TestFixtures.tenant;
import static com.gaulatti.colombo.TestFixtures.validCredentials;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.gaulatti.colombo.model.Tenant;
import com.gaulatti.colombo.service.UploadService;
import java.io.File;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.nio.file.Files;
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
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ColomboFtpletTest {

    @Mock
    private UploadService uploadService;

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
        ftplet = new ColomboFtplet(sessions, uploadService);
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
        sessions.put(username, new SessionData(tenant, "assignment-1", validCredentials(), "key"));

        File file = Files.createTempFile("colombo", ".jpg").toFile();
        Files.writeString(file.toPath(), "x");

        when(request.getArgument()).thenReturn(file.getName());
        mockPhysicalFile(file);

        when(uploadService.processFtpUpload(eq(username), eq(file.getName()), eq(file))).thenReturn(true);

        FtpletResult result = ftplet.onUploadEnd(session, request);
        assertEquals(FtpletResult.DEFAULT, result);

        verify(session).setAttribute(eq("colombo.upload.processed:" + file.getName()), eq(Boolean.TRUE));
    }

    @Test
    void processUploadHandlesUploadFailureAndUnexpectedExceptions() throws Exception {
        String username = "acme-user";
        mockLoggedInUser(username);
        File file = Files.createTempFile("colombo", ".jpg").toFile();
        Files.writeString(file.toPath(), "x");
        when(request.getArgument()).thenReturn(file.getName());
        mockPhysicalFile(file);

        sessions.put(username, new SessionData(tenant, "assignment", validCredentials(), "key"));
        when(uploadService.processFtpUpload(any(), any(), any())).thenReturn(false);
        assertEquals(FtpletResult.DISCONNECT, ftplet.onUploadEnd(session, request));

        when(uploadService.processFtpUpload(any(), any(), any())).thenThrow(new RuntimeException("boom"));
        assertEquals(FtpletResult.DISCONNECT, ftplet.onUploadEnd(session, request));
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
    }

    @Test
    void helperMethodsCoverResolvePhysicalFileEdgeBranches() throws Exception {
        FileSystemView view = mock(FileSystemView.class);
        when(session.getFileSystemView()).thenReturn(view);
        assertNull(invoke(ftplet, "resolvePhysicalFile", new Class[]{FtpSession.class, String.class}, new Object[]{session, "   "}));
        assertNull(invoke(ftplet, "resolvePhysicalFile", new Class[]{FtpSession.class, String.class}, new Object[]{session, null}));
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
}
