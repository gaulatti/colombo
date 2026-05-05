package com.gaulatti.colombo.controller;

import com.gaulatti.colombo.ftp.ColomboUserManager;
import com.gaulatti.colombo.ftp.SessionData;
import com.gaulatti.colombo.model.Tenant;
import com.gaulatti.colombo.repository.TenantRepository;
import com.gaulatti.colombo.service.UploadService;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.ftpserver.ftplet.AuthenticationFailedException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.server.ResponseStatusException;

/**
 * REST controller that exposes an HTTP file-upload endpoint for Colombo.
 *
 * <p>Mobile apps and any HTTP client can upload files to S3 via this endpoint
 * without using FTP. The endpoint reuses the same CMS authentication and S3
 * upload logic as the FTP path, but in a stateless, per-request fashion.
 *
 * <h3>Request format</h3>
 * <pre>
 * POST /upload
 * Content-Type: multipart/form-data
 * X-Colombo-Username: &lt;ftpUsername&gt;
 * X-Colombo-Password: &lt;cms credential key&gt;
 *
 * file=&lt;image binary&gt;
 * </pre>
 *
 * <h3>Response format (202 Accepted)</h3>
 * <pre>
 * {
 *   "status": "accepted",
 *   "assignment_id": "..."
 * }
 * </pre>
 */
@Slf4j
@RestController
public class UploadController {

    /** Repository used to resolve tenant records by FTP username. */
    private final TenantRepository tenantRepository;

    /** User manager used to validate credentials against the CMS. */
    private final ColomboUserManager colomboUserManager;

    /** Service used to upload to S3 and fire the CMS photo callback. */
    private final UploadService uploadService;

    /**
     * Creates a new {@code UploadController}.
     *
     * @param tenantRepository   repository for resolving tenant records
     * @param colomboUserManager user manager for CMS credential validation
     * @param uploadService      service for S3 upload and photo callback
     */
    public UploadController(
            TenantRepository tenantRepository,
            ColomboUserManager colomboUserManager,
            UploadService uploadService
    ) {
        this.tenantRepository = tenantRepository;
        this.colomboUserManager = colomboUserManager;
        this.uploadService = uploadService;
    }

    /**
     * Accepts a multipart file upload, authenticates the caller against the CMS,
     * writes the file to local temporary storage, schedules S3 upload and CMS photo
     * callback processing, and returns an acceptance receipt to the uploader.
     *
     * <p>The master-password bypass present in the FTP path is intentionally not
     * reachable through this endpoint; only real CMS credentials are accepted.
     *
     * @param username the FTP username, supplied in the {@code X-Colombo-Username} header
     * @param password the CMS credential key, supplied in the {@code X-Colombo-Password} header
     * @param file     the file to upload, supplied as the {@code file} form field
     * @return {@code 202 Accepted} with {@code {"status": "accepted", "assignment_id": "..."}} on success
     * @throws ResponseStatusException {@code 400} if the username or file is missing;
     *                                 {@code 401} if credentials are rejected by the CMS;
     *                                 {@code 404} if no tenant is registered for the username;
     *                                 {@code 500} if the upload cannot be received locally
     */
    @PostMapping("/upload")
    public ResponseEntity<Map<String, String>> upload(
            @RequestHeader("X-Colombo-Username") String username,
            @RequestHeader("X-Colombo-Password") String password,
            @RequestParam("file") MultipartFile file
    ) {
        log.info("[UPLOAD] HTTP upload request username='{}' filename='{}'",
                username, file == null ? null : file.getOriginalFilename());

        if (username == null || username.isBlank()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "X-Colombo-Username header is required");
        }
        if (file == null || file.isEmpty()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "file part is required and must not be empty");
        }

        Tenant tenant = tenantRepository.findByFtpUsername(username)
                .orElseThrow(() -> {
                    log.warn("[UPLOAD] unknown username='{}'", username);
                    return new ResponseStatusException(HttpStatus.NOT_FOUND,
                            "No tenant registered for username: " + username);
                });

        SessionData sessionData;
        try {
            sessionData = colomboUserManager.validateForUpload(tenant, username, password);
        } catch (AuthenticationFailedException ex) {
            log.warn("[UPLOAD] authentication failed username='{}'", username);
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "Invalid credentials");
        }

        String originalFilename = file.getOriginalFilename();
        String filename = (originalFilename != null && !originalFilename.isBlank())
                ? Path.of(originalFilename).getFileName().toString()
                : "upload";

        Path tempPath;
        try {
            tempPath = Files.createTempFile("colombo-upload-", "-" + filename);
            file.transferTo(tempPath);
        } catch (IOException ex) {
            log.error("[UPLOAD] failed to write temp file username='{}' filename='{}'", username, filename, ex);
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to process uploaded file");
        }

        uploadService.processHttpUploadAsync(sessionData, username, filename, tempPath);

        log.info("[UPLOAD] accepted username='{}' assignmentId='{}' filename='{}'",
                username, sessionData.getAssignmentId(), filename);

        Map<String, String> response = new LinkedHashMap<>();
        response.put("status", "accepted");
        response.put("assignment_id", sessionData.getAssignmentId());
        return ResponseEntity.accepted().body(response);
    }
}
