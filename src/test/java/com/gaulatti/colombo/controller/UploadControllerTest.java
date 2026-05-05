package com.gaulatti.colombo.controller;

import static com.gaulatti.colombo.TestFixtures.tenant;
import static com.gaulatti.colombo.TestFixtures.validCredentials;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.multipart;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.gaulatti.colombo.ftp.ColomboUserManager;
import com.gaulatti.colombo.ftp.SessionData;
import com.gaulatti.colombo.model.Tenant;
import com.gaulatti.colombo.repository.TenantRepository;
import com.gaulatti.colombo.service.UploadService;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import org.apache.ftpserver.ftplet.AuthenticationFailedException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.ArgumentCaptor;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.MediaType;
import org.springframework.http.HttpStatus;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.server.ResponseStatusException;

@ExtendWith(MockitoExtension.class)
class UploadControllerTest {

    @Mock
    private TenantRepository tenantRepository;

    @Mock
    private ColomboUserManager colomboUserManager;

    @Mock
    private UploadService uploadService;

    private UploadController controller;
    private MockMvc mockMvc;
    private Tenant tenant;

    @BeforeEach
    void setUp() {
        controller = new UploadController(tenantRepository, colomboUserManager, uploadService);
        mockMvc = MockMvcBuilders.standaloneSetup(controller).build();
        tenant = tenant();
    }

    @Test
    void uploadReturns202WithAssignmentIdAfterReceivingFile() throws Exception {
        SessionData sessionData = new SessionData(tenant, "assignment-123", validCredentials(), "key");
        when(tenantRepository.findByFtpUsername("acme-user")).thenReturn(Optional.of(tenant));
        when(colomboUserManager.validateForUpload(eq(tenant), eq("acme-user"), eq("secret")))
                .thenReturn(sessionData);

        MockMultipartFile file = new MockMultipartFile(
                "file", "photo.jpg", MediaType.IMAGE_JPEG_VALUE, "imgdata".getBytes());

        mockMvc.perform(multipart("/upload")
                        .file(file)
                        .header("X-Colombo-Username", "acme-user")
                        .header("X-Colombo-Password", "secret"))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.status").value("accepted"))
                .andExpect(jsonPath("$.assignment_id").value("assignment-123"));

        ArgumentCaptor<Path> pathCaptor = ArgumentCaptor.forClass(Path.class);
        verify(uploadService).processHttpUploadAsync(eq(sessionData), eq("acme-user"), eq("photo.jpg"), pathCaptor.capture());
        Files.deleteIfExists(pathCaptor.getValue());
    }

    @Test
    void uploadReturns404WhenTenantNotFound() throws Exception {
        when(tenantRepository.findByFtpUsername("unknown")).thenReturn(Optional.empty());

        MockMultipartFile file = new MockMultipartFile(
                "file", "photo.jpg", MediaType.IMAGE_JPEG_VALUE, "imgdata".getBytes());

        mockMvc.perform(multipart("/upload")
                        .file(file)
                        .header("X-Colombo-Username", "unknown")
                        .header("X-Colombo-Password", "secret"))
                .andExpect(status().isNotFound());
    }

    @Test
    void uploadReturns401WhenCredentialsRejected() throws Exception {
        when(tenantRepository.findByFtpUsername("acme-user")).thenReturn(Optional.of(tenant));
        when(colomboUserManager.validateForUpload(any(), any(), any()))
                .thenThrow(new AuthenticationFailedException("denied"));

        MockMultipartFile file = new MockMultipartFile(
                "file", "photo.jpg", MediaType.IMAGE_JPEG_VALUE, "imgdata".getBytes());

        mockMvc.perform(multipart("/upload")
                        .file(file)
                        .header("X-Colombo-Username", "acme-user")
                        .header("X-Colombo-Password", "wrong"))
                .andExpect(status().isUnauthorized());
    }

    @Test
    void uploadReturns400WhenFileIsEmpty() throws Exception {
        MockMultipartFile emptyFile = new MockMultipartFile(
                "file", "photo.jpg", MediaType.IMAGE_JPEG_VALUE, new byte[0]);

        mockMvc.perform(multipart("/upload")
                        .file(emptyFile)
                        .header("X-Colombo-Username", "acme-user")
                        .header("X-Colombo-Password", "secret"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void uploadReturns400WhenUsernameIsNullOrBlank() {
        MockMultipartFile file = new MockMultipartFile(
                "file", "photo.jpg", MediaType.IMAGE_JPEG_VALUE, "imgdata".getBytes());

        ResponseStatusException nullUsername = assertThrows(ResponseStatusException.class,
                () -> controller.upload(null, "secret", file));
        assertEquals(HttpStatus.BAD_REQUEST, nullUsername.getStatusCode());

        ResponseStatusException blankUsername = assertThrows(ResponseStatusException.class,
                () -> controller.upload("   ", "secret", file));
        assertEquals(HttpStatus.BAD_REQUEST, blankUsername.getStatusCode());
    }

    @Test
    void uploadReturns400WhenFileIsNull() {
        ResponseStatusException exception = assertThrows(ResponseStatusException.class,
                () -> controller.upload("acme-user", "secret", null));

        assertEquals(HttpStatus.BAD_REQUEST, exception.getStatusCode());
    }

    @Test
    void uploadReturns202WithoutWaitingForBackgroundProcessing() throws Exception {
        SessionData sessionData = new SessionData(tenant, "assignment-123", validCredentials(), "key");
        when(tenantRepository.findByFtpUsername("acme-user")).thenReturn(Optional.of(tenant));
        when(colomboUserManager.validateForUpload(any(), any(), any())).thenReturn(sessionData);

        MockMultipartFile file = new MockMultipartFile(
                "file", "photo.jpg", MediaType.IMAGE_JPEG_VALUE, "imgdata".getBytes());

        ArgumentCaptor<Path> pathCaptor = ArgumentCaptor.forClass(Path.class);

        mockMvc.perform(multipart("/upload")
                        .file(file)
                        .header("X-Colombo-Username", "acme-user")
                        .header("X-Colombo-Password", "secret"))
                .andExpect(status().isAccepted());

        verify(uploadService).processHttpUploadAsync(any(), any(), any(), pathCaptor.capture());
        Files.deleteIfExists(pathCaptor.getValue());
    }

    @Test
    void uploadHandlesFileWithNoOriginalFilename() throws Exception {
        SessionData sessionData = new SessionData(tenant, "assignment-123", validCredentials(), "key");
        when(tenantRepository.findByFtpUsername("acme-user")).thenReturn(Optional.of(tenant));
        when(colomboUserManager.validateForUpload(any(), any(), any())).thenReturn(sessionData);

        MockMultipartFile file = new MockMultipartFile(
                "file", (String) null, MediaType.IMAGE_JPEG_VALUE, "imgdata".getBytes());

        mockMvc.perform(multipart("/upload")
                        .file(file)
                        .header("X-Colombo-Username", "acme-user")
                        .header("X-Colombo-Password", "secret"))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.status").value("accepted"));

        ArgumentCaptor<Path> pathCaptor = ArgumentCaptor.forClass(Path.class);
        verify(uploadService).processHttpUploadAsync(eq(sessionData), eq("acme-user"), eq("upload"), pathCaptor.capture());
        Files.deleteIfExists(pathCaptor.getValue());
    }

    @Test
    void uploadHandlesFileWithNullOriginalFilename() throws Exception {
        SessionData sessionData = new SessionData(tenant, "assignment-123", validCredentials(), "key");
        when(tenantRepository.findByFtpUsername("acme-user")).thenReturn(Optional.of(tenant));
        when(colomboUserManager.validateForUpload(any(), any(), any())).thenReturn(sessionData);
        ArgumentCaptor<Path> pathCaptor = ArgumentCaptor.forClass(Path.class);

        assertEquals(HttpStatus.ACCEPTED,
                controller.upload("acme-user", "secret", new TestMultipartFile(null)).getStatusCode());

        verify(uploadService).processHttpUploadAsync(eq(sessionData), eq("acme-user"), eq("upload"), pathCaptor.capture());
        Files.deleteIfExists(pathCaptor.getValue());
    }

    @Test
    void uploadReturns500WhenTempFileTransferFails() throws Exception {
        when(tenantRepository.findByFtpUsername("acme-user")).thenReturn(Optional.of(tenant));
        when(colomboUserManager.validateForUpload(any(), any(), any()))
                .thenReturn(new SessionData(tenant, "assignment-123", validCredentials(), "key"));

        ResponseStatusException exception = assertThrows(ResponseStatusException.class,
                () -> controller.upload("acme-user", "secret", new ThrowingMultipartFile("photo.jpg")));

        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, exception.getStatusCode());
    }

    @Test
    void uploadSchedulesBackgroundProcessingWhenTempCleanupWouldFailInRequestThread() throws Exception {
        SessionData sessionData = new SessionData(tenant, "assignment-123", validCredentials(), "key");
        when(tenantRepository.findByFtpUsername("acme-user")).thenReturn(Optional.of(tenant));
        when(colomboUserManager.validateForUpload(eq(tenant), eq("acme-user"), eq("secret")))
                .thenReturn(sessionData);

        MockMultipartFile file = new MockMultipartFile(
                "file", "photo.jpg", MediaType.IMAGE_JPEG_VALUE, "imgdata".getBytes());

        try (MockedStatic<Files> files = mockStatic(Files.class, CALLS_REAL_METHODS)) {
            files.when(() -> Files.deleteIfExists(any(Path.class))).thenThrow(new IOException("cleanup failed"));

            assertEquals(HttpStatus.ACCEPTED, controller.upload("acme-user", "secret", file).getStatusCode());
        }
    }

    private static final class ThrowingMultipartFile implements MultipartFile {
        private final String originalFilename;

        private ThrowingMultipartFile(String originalFilename) {
            this.originalFilename = originalFilename;
        }

        @Override
        public String getName() {
            return "file";
        }

        @Override
        public String getOriginalFilename() {
            return originalFilename;
        }

        @Override
        public String getContentType() {
            return MediaType.IMAGE_JPEG_VALUE;
        }

        @Override
        public boolean isEmpty() {
            return false;
        }

        @Override
        public long getSize() {
            return 1;
        }

        @Override
        public byte[] getBytes() {
            return new byte[]{1};
        }

        @Override
        public InputStream getInputStream() {
            return InputStream.nullInputStream();
        }

        @Override
        public void transferTo(java.io.File dest) throws IOException {
            throw new IOException("transfer failed");
        }

        @Override
        public void transferTo(Path dest) throws IOException {
            throw new IOException("transfer failed");
        }
    }

    private static final class TestMultipartFile implements MultipartFile {
        private final String originalFilename;

        private TestMultipartFile(String originalFilename) {
            this.originalFilename = originalFilename;
        }

        @Override
        public String getName() {
            return "file";
        }

        @Override
        public String getOriginalFilename() {
            return originalFilename;
        }

        @Override
        public String getContentType() {
            return MediaType.IMAGE_JPEG_VALUE;
        }

        @Override
        public boolean isEmpty() {
            return false;
        }

        @Override
        public long getSize() {
            return 1;
        }

        @Override
        public byte[] getBytes() {
            return new byte[]{1};
        }

        @Override
        public InputStream getInputStream() {
            return InputStream.nullInputStream();
        }

        @Override
        public void transferTo(java.io.File dest) throws IOException {
            Files.write(dest.toPath(), getBytes());
        }

        @Override
        public void transferTo(Path dest) throws IOException {
            Files.write(dest, getBytes());
        }
    }
}
