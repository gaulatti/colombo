package com.gaulatti.colombo.controller;

import static com.gaulatti.colombo.TestFixtures.tenant;
import static com.gaulatti.colombo.TestFixtures.validCredentials;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.multipart;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.gaulatti.colombo.ftp.ColomboUserManager;
import com.gaulatti.colombo.ftp.SessionData;
import com.gaulatti.colombo.model.Tenant;
import com.gaulatti.colombo.repository.TenantRepository;
import com.gaulatti.colombo.service.UploadService;
import java.util.Optional;
import org.apache.ftpserver.ftplet.AuthenticationFailedException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

@ExtendWith(MockitoExtension.class)
class UploadControllerTest {

    @Mock
    private TenantRepository tenantRepository;

    @Mock
    private ColomboUserManager colomboUserManager;

    @Mock
    private UploadService uploadService;

    private MockMvc mockMvc;
    private Tenant tenant;

    @BeforeEach
    void setUp() {
        UploadController controller = new UploadController(tenantRepository, colomboUserManager, uploadService);
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
}
