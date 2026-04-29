package com.gaulatti.colombo.controller;

import static com.gaulatti.colombo.TestFixtures.tenant;
import static com.gaulatti.colombo.TestFixtures.validCredentials;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mockStatic;
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
    void uploadReturns200WithS3UrlAndAssignmentIdOnHappyPath() throws Exception {
        SessionData sessionData = new SessionData(tenant, "assignment-123", validCredentials(), "key");
        when(tenantRepository.findByFtpUsername("acme-user")).thenReturn(Optional.of(tenant));
        when(colomboUserManager.validateForUpload(eq(tenant), eq("acme-user"), eq("secret")))
                .thenReturn(sessionData);
        when(uploadService.uploadToS3(eq(sessionData), eq("acme-user"), any(), any()))
                .thenReturn("s3://bucket/prefix/photo.jpg");

        MockMultipartFile file = new MockMultipartFile(
                "file", "photo.jpg", MediaType.IMAGE_JPEG_VALUE, "imgdata".getBytes());

        mockMvc.perform(multipart("/upload")
                        .file(file)
                        .header("X-Colombo-Username", "acme-user")
                        .header("X-Colombo-Password", "secret"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.s3_url").value("s3://bucket/prefix/photo.jpg"))
                .andExpect(jsonPath("$.assignment_id").value("assignment-123"));
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
    void uploadReturns500WhenS3Fails() throws Exception {
        SessionData sessionData = new SessionData(tenant, "assignment-123", validCredentials(), "key");
        when(tenantRepository.findByFtpUsername("acme-user")).thenReturn(Optional.of(tenant));
        when(colomboUserManager.validateForUpload(any(), any(), any())).thenReturn(sessionData);
        when(uploadService.uploadToS3(any(), any(), any(), any()))
                .thenThrow(new RuntimeException("S3 failure"));

        MockMultipartFile file = new MockMultipartFile(
                "file", "photo.jpg", MediaType.IMAGE_JPEG_VALUE, "imgdata".getBytes());

        mockMvc.perform(multipart("/upload")
                        .file(file)
                        .header("X-Colombo-Username", "acme-user")
                        .header("X-Colombo-Password", "secret"))
                .andExpect(status().isInternalServerError());
    }

    @Test
    void uploadReturns500WhenPhotoCallbackFails() throws Exception {
        SessionData sessionData = new SessionData(tenant, "assignment-123", validCredentials(), "key");
        when(tenantRepository.findByFtpUsername("acme-user")).thenReturn(Optional.of(tenant));
        when(colomboUserManager.validateForUpload(any(), any(), any())).thenReturn(sessionData);
        when(uploadService.uploadToS3(any(), any(), any(), any())).thenReturn("s3://bucket/key");
        doThrow(new RuntimeException("callback failure"))
                .when(uploadService).postPhotoCallback(any(), any(), any(), any());

        MockMultipartFile file = new MockMultipartFile(
                "file", "photo.jpg", MediaType.IMAGE_JPEG_VALUE, "imgdata".getBytes());

        mockMvc.perform(multipart("/upload")
                        .file(file)
                        .header("X-Colombo-Username", "acme-user")
                        .header("X-Colombo-Password", "secret"))
                .andExpect(status().isInternalServerError());
    }

    @Test
    void uploadHandlesFileWithNoOriginalFilename() throws Exception {
        SessionData sessionData = new SessionData(tenant, "assignment-123", validCredentials(), "key");
        when(tenantRepository.findByFtpUsername("acme-user")).thenReturn(Optional.of(tenant));
        when(colomboUserManager.validateForUpload(any(), any(), any())).thenReturn(sessionData);
        when(uploadService.uploadToS3(any(), any(), eq("upload"), any()))
                .thenReturn("s3://bucket/prefix/upload");

        MockMultipartFile file = new MockMultipartFile(
                "file", (String) null, MediaType.IMAGE_JPEG_VALUE, "imgdata".getBytes());

        mockMvc.perform(multipart("/upload")
                        .file(file)
                        .header("X-Colombo-Username", "acme-user")
                        .header("X-Colombo-Password", "secret"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.s3_url").value("s3://bucket/prefix/upload"));
    }

    @Test
    void uploadHandlesFileWithNullOriginalFilename() throws Exception {
        SessionData sessionData = new SessionData(tenant, "assignment-123", validCredentials(), "key");
        when(tenantRepository.findByFtpUsername("acme-user")).thenReturn(Optional.of(tenant));
        when(colomboUserManager.validateForUpload(any(), any(), any())).thenReturn(sessionData);
        when(uploadService.uploadToS3(any(), any(), eq("upload"), any()))
                .thenReturn("s3://bucket/prefix/upload");

        assertEquals(HttpStatus.OK,
                controller.upload("acme-user", "secret", new TestMultipartFile(null)).getStatusCode());
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
    void uploadStillSucceedsWhenTempCleanupFails() throws Exception {
        SessionData sessionData = new SessionData(tenant, "assignment-123", validCredentials(), "key");
        when(tenantRepository.findByFtpUsername("acme-user")).thenReturn(Optional.of(tenant));
        when(colomboUserManager.validateForUpload(eq(tenant), eq("acme-user"), eq("secret")))
                .thenReturn(sessionData);
        when(uploadService.uploadToS3(eq(sessionData), eq("acme-user"), eq("photo.jpg"), any()))
                .thenReturn("s3://bucket/prefix/photo.jpg");

        MockMultipartFile file = new MockMultipartFile(
                "file", "photo.jpg", MediaType.IMAGE_JPEG_VALUE, "imgdata".getBytes());

        try (MockedStatic<Files> files = mockStatic(Files.class, CALLS_REAL_METHODS)) {
            files.when(() -> Files.deleteIfExists(any(Path.class))).thenThrow(new IOException("cleanup failed"));

            assertEquals(HttpStatus.OK, controller.upload("acme-user", "secret", file).getStatusCode());
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
