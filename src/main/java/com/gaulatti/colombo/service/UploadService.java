package com.gaulatti.colombo.service;

import com.gaulatti.colombo.ftp.ColomboUserManager;
import com.gaulatti.colombo.ftp.SessionData;
import com.gaulatti.colombo.ftp.SessionUploadCredentials;
import com.gaulatti.colombo.ftp.UploadNamingPolicy;
import com.gaulatti.colombo.model.Tenant;
import com.gaulatti.colombo.observability.ColomboMetrics;
import io.micrometer.core.instrument.Timer;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.net.URI;
import java.net.URLConnection;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.RestTemplate;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * Spring service that encapsulates S3 upload and CMS photo-callback logic.
 *
 * <p>Used by both the FTP path ({@link com.gaulatti.colombo.ftp.ColomboFtplet}) and
 * the REST HTTP path ({@link com.gaulatti.colombo.controller.UploadController}).
 *
 * <p>FTP callers use {@link #processFtpUpload(String, String, File)}, which reads from
 * the shared session map, handles credential refresh, and fires the photo callback.
 * HTTP callers invoke {@link #processHttpUploadAsync(SessionData, String, String, Path)}
 * with a pre-validated {@link SessionData} obtained from the CMS so the request can
 * return before S3 upload and CMS photo-callback processing completes.
 */
@Slf4j
@Service
public class UploadService {

    /** HTTP header name used to authenticate outbound requests to the CMS. */
    private static final String API_KEY_HEADER = "X-Colombo-API-Key";

    /** Shared concurrent map of active session data keyed by FTP username. */
    private final ConcurrentHashMap<String, SessionData> sessions;

    /** HTTP client for outbound calls to the CMS. */
    private final RestTemplate restTemplate;

    /** User manager used for session refresh and eviction operations. */
    private final ColomboUserManager colomboUserManager;

    /** Executor used for outbound S3 uploads after Colombo has accepted a file locally. */
    private final Executor s3UploadExecutor;

    /** Executor used for CMS photo callbacks after S3 upload succeeds. */
    private final Executor cmsCallbackExecutor;
    private final UploadNameRenderer uploadNameRenderer;
    private final CaptureTimeReader captureTimeReader;
    private final ColomboMetrics metrics;

    /**
     * Creates a new {@code UploadService}.
     *
     * @param sessions           shared session map
     * @param restTemplate       HTTP client for CMS calls
     * @param colomboUserManager user manager for session refresh and eviction
     * @param s3UploadExecutor   executor for asynchronous S3 upload processing
     * @param cmsCallbackExecutor executor for asynchronous CMS callback processing
     */
    @Autowired
    public UploadService(
            ConcurrentHashMap<String, SessionData> sessions,
            RestTemplate restTemplate,
            ColomboUserManager colomboUserManager,
            @Qualifier("s3UploadExecutor") Executor s3UploadExecutor,
            @Qualifier("cmsCallbackExecutor") Executor cmsCallbackExecutor,
            UploadNameRenderer uploadNameRenderer,
            CaptureTimeReader captureTimeReader,
            ColomboMetrics metrics
    ) {
        this.sessions = sessions;
        this.restTemplate = restTemplate;
        this.colomboUserManager = colomboUserManager;
        this.s3UploadExecutor = s3UploadExecutor;
        this.cmsCallbackExecutor = cmsCallbackExecutor;
        this.uploadNameRenderer = uploadNameRenderer;
        this.captureTimeReader = captureTimeReader;
        this.metrics = metrics;
    }

    public UploadService(
            ConcurrentHashMap<String, SessionData> sessions,
            RestTemplate restTemplate,
            ColomboUserManager colomboUserManager,
            Executor s3UploadExecutor,
            Executor cmsCallbackExecutor
    ) {
        this(sessions, restTemplate, colomboUserManager, s3UploadExecutor, cmsCallbackExecutor,
                new UploadNameRenderer(), new CaptureTimeReader(), ColomboMetrics.noop());
    }

    public UploadService(
            ConcurrentHashMap<String, SessionData> sessions,
            RestTemplate restTemplate,
            ColomboUserManager colomboUserManager,
            Executor s3UploadExecutor,
            Executor cmsCallbackExecutor,
            UploadNameRenderer uploadNameRenderer,
            CaptureTimeReader captureTimeReader
    ) {
        this(sessions, restTemplate, colomboUserManager, s3UploadExecutor, cmsCallbackExecutor,
                uploadNameRenderer, captureTimeReader, ColomboMetrics.noop());
    }

    /**
     * Performs the full FTP upload pipeline: S3 upload (with transparent credential
     * refresh) followed by the CMS photo callback.
     *
     * <p>Reads the session from the shared map, handles {@code ExpiredToken} errors by
     * re-validating against the CMS, and evicts the session on unrecoverable failures.
     *
     * @param username  the FTP username that owns the session
     * @param filename  the bare filename to use as the S3 object name suffix
     * @param localFile the local file to upload
     * @return {@code true} if both the S3 upload and photo callback succeeded;
     *         {@code false} on any failure
     */
    public boolean processFtpUpload(String username, String filename, File localFile) {
        UploadResult result = uploadToS3WithRefresh(username, filename, localFile);
        if (!result.success()) {
            return false;
        }
        SessionData activeSession = result.sessionData();
        postPhotoCallback(activeSession.getTenant(), activeSession.getAssignmentId(), result.s3Url(), username,
                result.originalFilename(), result.targetFilename());
        return true;
    }

    /**
     * Schedules the FTP upload pipeline on outbound queues.
     *
     * <p>This keeps FTP command handling responsive after Colombo has accepted the
     * file locally. The S3 upload is queued first; the CMS photo callback is queued
     * only after S3 succeeds.
     *
     * @param username  the FTP username that owns the session
     * @param filename  the bare filename to use as the S3 object name suffix
     * @param localFile the local file to upload
     * @return a future that completes with {@code true} on success, {@code false} on
     *         recoverable failure, or exceptionally on unexpected errors
     */
    public CompletableFuture<Boolean> processFtpUploadAsync(String username, String filename, File localFile) {
        metrics.upload("ftp", "accepted", "queued");
        return CompletableFuture
                .supplyAsync(() -> uploadToS3WithRefresh(username, filename, localFile), s3UploadExecutor)
                .thenCompose(result -> queuePhotoCallback(result, username))
                .whenComplete((success, throwable) -> metrics.upload(
                        "ftp", "complete", Boolean.TRUE.equals(success) ? "success" : "failure"));
    }

    /**
     * Schedules the HTTP upload pipeline after the request body has been received.
     *
     * <p>The uploader's request is considered complete once the multipart body has
     * been written to {@code localPath}. S3 upload and CMS notification continue in
     * the background, and this service owns cleanup of the temporary file.
     *
     * @param sessionData the session returned by CMS validation
     * @param username    the FTP username associated with the upload
     * @param filename    the bare filename to use in S3
     * @param localPath   the temporary file containing the received upload
     */
    public void processHttpUploadAsync(SessionData sessionData, String username, String filename, Path localPath) {
        metrics.upload("http", "accepted", "queued");
        CompletableFuture<UploadResult> s3Upload = CompletableFuture.supplyAsync(
                () -> processHttpUploadToS3(sessionData, username, filename, localPath),
                s3UploadExecutor
        );

        s3Upload.whenComplete((result, throwable) -> deleteLocalPath(localPath));

        s3Upload
                .thenCompose(result -> queuePhotoCallback(result, username)
                        .thenApply(success -> {
                            log.info("[UPLOAD] background complete username='{}' assignmentId='{}' s3Url='{}'",
                                    username, result.sessionData().getAssignmentId(), result.s3Url());
                            return success;
                        }))
                .whenComplete((success, throwable) -> {
                    metrics.upload("http", "complete",
                            Boolean.TRUE.equals(success) ? "success" : "failure");
                    if (throwable != null) {
                        log.error("[UPLOAD] background upload or callback failed username='{}' filename='{}'",
                                username, filename, throwable);
                    }
                });
    }

    void processHttpUpload(SessionData sessionData, String username, String filename, Path localPath) {
        try {
            UploadResult result = processHttpUploadToS3(sessionData, username, filename, localPath);
            postPhotoCallback(
                    result.sessionData().getTenant(),
                    result.sessionData().getAssignmentId(),
                    result.s3Url(),
                    username,
                    result.originalFilename(),
                    result.targetFilename()
            );

            log.info("[UPLOAD] background complete username='{}' assignmentId='{}' s3Url='{}'",
                    username, sessionData.getAssignmentId(), result.s3Url());
        } catch (Exception ex) {
            log.error("[UPLOAD] background upload or callback failed username='{}' filename='{}'",
                    username, filename, ex);
        } finally {
            deleteLocalPath(localPath);
        }
    }

    private UploadResult processHttpUploadToS3(SessionData sessionData, String username, String filename, Path localPath) {
        NamedUpload upload = uploadToS3Named(sessionData, username, filename, localPath.toFile());
        return UploadResult.success(sessionData, upload.s3Url(), filename, upload.targetFilename());
    }

    private CompletableFuture<Boolean> queuePhotoCallback(UploadResult result, String username) {
        if (!result.success()) {
            return CompletableFuture.completedFuture(false);
        }
        SessionData activeSession = result.sessionData();
        return CompletableFuture.supplyAsync(() -> {
            postPhotoCallback(activeSession.getTenant(), activeSession.getAssignmentId(), result.s3Url(), username,
                    result.originalFilename(), result.targetFilename());
            return true;
        }, cmsCallbackExecutor);
    }

    private void deleteLocalPath(Path localPath) {
        try {
            Files.deleteIfExists(localPath);
        } catch (IOException deleteEx) {
            log.warn("[UPLOAD] failed to delete temp file path='{}'", localPath, deleteEx);
        }
    }

    /**
     * Uploads a file to S3 using the credentials contained in {@code sessionData}.
     *
     * <p>This method is stateless and does not interact with the session map. It is
     * intended for use by the HTTP upload path where the caller has already obtained
     * a validated {@link SessionData} from the CMS.
     *
     * @param sessionData the session providing temporary AWS credentials and assignment context
     * @param username    the username (used for logging)
     * @param filename    the bare filename to append to the key prefix
     * @param localFile   the local file to upload
     * @return the resulting S3 URL in {@code s3://bucket/key} format
     */
    public String uploadToS3(SessionData sessionData, String username, String filename, File localFile) {
        return uploadToS3Named(sessionData, username, filename, localFile).s3Url();
    }

    private NamedUpload uploadToS3Named(SessionData sessionData, String username, String filename, File localFile) {
        String assignmentId = sessionData.getAssignmentId();
        SessionUploadCredentials uploadCredentials = sessionData.getUploadCredentials();
        String targetFilename = resolveTargetFilename(sessionData, filename, localFile);
        String s3Key = buildObjectKey(uploadCredentials.getKeyPrefix(), targetFilename);
        String bucket = uploadCredentials.getBucket();

        log.info("[S3 UPLOAD] start username='{}' assignmentId='{}' localFile='{}' bucket='{}' key='{}'",
                username, assignmentId, localFile.getAbsolutePath(), bucket, s3Key);
        S3Client client = resolveS3Client(uploadCredentials);
        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(bucket)
                .key(s3Key)
                .contentType(resolveContentType(filename))
                .build();
        Timer.Sample sample = metrics.startDependency();
        try {
            client.putObject(putObjectRequest, RequestBody.fromFile(localFile.toPath()));
            metrics.dependency(sample, "s3", "put_object", "success");
        } catch (S3Exception exception) {
            String result = isExpiredCredentialError(exception) ? "expired"
                    : isDeniedUploadError(exception) ? "denied" : "error";
            metrics.dependency(sample, "s3", "put_object", result);
            throw exception;
        } catch (RuntimeException exception) {
            metrics.dependency(sample, "s3", "put_object", "error");
            throw exception;
        }

        String s3Url = "s3://" + bucket + "/" + s3Key;
        log.info("[S3 UPLOAD] success username='{}' assignmentId='{}' file='{}'", username, assignmentId, filename);
        return new NamedUpload(s3Url, targetFilename);
    }

    @SuppressWarnings("rawtypes")
    private String resolveTargetFilename(SessionData sessionData, String originalFilename, File localFile) {
        SessionUploadCredentials credentials = sessionData.getUploadCredentials();
        UploadNamingPolicy policy = credentials.getNamingPolicy();
        if (policy == null) {
            return originalFilename;
        }

        String endpoint = resolveCmsEndpoint(sessionData.getTenant().getValidationEndpoint(), credentials.getSequenceEndpoint());
        HttpHeaders headers = new HttpHeaders();
        headers.set(API_KEY_HEADER, sessionData.getTenant().getApiKey());
        Map<String, String> body = Map.of("assignment_id", sessionData.getAssignmentId());
        ResponseEntity<Map> response = restTemplate.exchange(
                endpoint, HttpMethod.POST, new HttpEntity<>(body, headers), Map.class);
        Object rawSequence = response.getBody() == null ? null : response.getBody().get("sequence");
        long sequence;
        if (rawSequence instanceof Number number) {
            sequence = number.longValue();
        } else if (rawSequence instanceof String string) {
            sequence = Long.parseLong(string);
        } else {
            throw new IllegalStateException("CMS sequence response is invalid");
        }
        if (sequence < 1) {
            throw new IllegalStateException("CMS sequence response is invalid");
        }
        Instant uploadedAt = Instant.now();
        Instant capturedAt = captureTimeReader.read(localFile);
        return uploadNameRenderer.render(
                policy, sessionData.getAssignmentId(), originalFilename, capturedAt, uploadedAt, sequence);
    }

    private String resolveCmsEndpoint(String validationEndpoint, String sequenceEndpoint) {
        if (sequenceEndpoint == null || sequenceEndpoint.isBlank()) {
            throw new IllegalStateException("CMS naming policy is missing its sequence endpoint");
        }
        return URI.create(validationEndpoint).resolve(sequenceEndpoint).toString();
    }

    private String resolveContentType(String filename) {
        String detected = URLConnection.guessContentTypeFromName(filename);
        return detected == null ? "application/octet-stream" : detected;
    }

    /**
     * Sends a photo-uploaded callback to the CMS, notifying it of the new S3 URL.
     *
     * <p>On a 4xx denial the session for {@code username} is evicted from the shared
     * session map (a no-op when no FTP session exists for that user, as in the HTTP path).
     *
     * @param tenant       the tenant whose CMS photo endpoint should be called
     * @param assignmentId the assignment associated with the upload
     * @param s3Url        the S3 URL of the uploaded file
     * @param username     the username (used for session eviction on denial and logging)
     */
    public void postPhotoCallback(Tenant tenant, String assignmentId, String s3Url, String username) {
        String filename = extractFilenameFromS3Url(s3Url);
        postPhotoCallback(tenant, assignmentId, s3Url, username, filename, filename);
    }

    private void postPhotoCallback(
            Tenant tenant,
            String assignmentId,
            String s3Url,
            String username,
            String originalFilename,
            String targetFilename
    ) {
        Timer.Sample sample = metrics.startDependency();
        HttpHeaders headers = new HttpHeaders();
        headers.set(API_KEY_HEADER, tenant.getApiKey());

        Map<String, String> body = new LinkedHashMap<>();
        body.put("assignment_id", assignmentId);
        body.put("s3_url", s3Url);
        body.put("original_filename", originalFilename);
        body.put("target_filename", targetFilename);

        log.info("[PHOTO CALLBACK] sending assignmentId='{}' file='{}'",
                assignmentId, extractFilenameFromS3Url(s3Url));

        try {
            ResponseEntity<Void> response = restTemplate.exchange(
                    tenant.getPhotoEndpoint(),
                    HttpMethod.POST,
                    new HttpEntity<>(body, headers),
                    Void.class
            );

            if (!response.getStatusCode().is2xxSuccessful()) {
                if (response.getStatusCode().is4xxClientError()) {
                    colomboUserManager.evictSession(username, "photo callback denied");
                }
                throw new IllegalStateException("Photo callback failed with status: " + response.getStatusCode());
            }

            metrics.dependency(sample, "cms", "photo_callback", "success");
            log.info("[PHOTO CALLBACK] accepted assignmentId='{}' status='{}'", assignmentId, response.getStatusCode());
        } catch (HttpStatusCodeException ex) {
            if (ex.getStatusCode().is4xxClientError()) {
                colomboUserManager.evictSession(username, "photo callback denied");
            }
            metrics.dependency(sample, "cms", "photo_callback",
                    ex.getStatusCode().is4xxClientError() ? "denied" : "error");
            throw ex;
        } catch (RuntimeException ex) {
            metrics.dependency(sample, "cms", "photo_callback", "error");
            throw ex;
        }
    }

    /**
     * Uploads a file to S3, transparently refreshing expired credentials once if needed.
     *
     * <p>On an {@code ExpiredToken} / {@code InvalidToken} S3 error, the session is
     * re-validated against the CMS and the upload is retried with fresh credentials.
     * If refresh is denied or the retry also fails, the session is evicted.
     *
     * @param username  the FTP username that owns the session
     * @param filename  the bare filename to use as the S3 object name suffix
     * @param localFile the local file to upload
     * @return an {@link UploadResult} describing success or failure
     */
    UploadResult uploadToS3WithRefresh(String username, String filename, File localFile) {
        SessionData sessionData = sessions.get(username);
        if (sessionData == null) {
            log.warn("No in-memory session found for username='{}'", username);
            return UploadResult.failure();
        }
        if (!isValidSessionForUpload(username, sessionData)) {
            return UploadResult.failure();
        }

        try {
            NamedUpload upload = uploadToS3Named(sessionData, username, filename, localFile);
            return UploadResult.success(sessionData, upload.s3Url(), filename, upload.targetFilename());
        } catch (S3Exception s3Exception) {
            if (isExpiredCredentialError(s3Exception)) {
                metrics.retry("s3_put", "started");
                log.warn("[S3 UPLOAD] expired credentials username='{}' — refreshing via validate endpoint", username);
                ColomboUserManager.RefreshResult refreshResult = colomboUserManager.refreshSessionFromValidation(username);
                if (refreshResult != ColomboUserManager.RefreshResult.REFRESHED) {
                    metrics.retry("s3_put", "abandoned");
                    colomboUserManager.evictSession(username, "validate refresh denied/failed after expired S3 credentials");
                    return UploadResult.failure();
                }
                SessionData refreshedSession = sessions.get(username);
                if (refreshedSession == null || !isValidSessionForUpload(username, refreshedSession)) {
                    metrics.retry("s3_put", "abandoned");
                    colomboUserManager.evictSession(username, "refreshed session invalid after validate refresh");
                    return UploadResult.failure();
                }
                try {
                    NamedUpload upload = uploadToS3Named(refreshedSession, username, filename, localFile);
                    metrics.retry("s3_put", "success");
                    return UploadResult.success(refreshedSession, upload.s3Url(), filename, upload.targetFilename());
                } catch (S3Exception retryException) {
                    metrics.retry("s3_put", "failure");
                    if (isDeniedUploadError(retryException) || isExpiredCredentialError(retryException)) {
                        colomboUserManager.evictSession(username, "S3 denied after credential refresh");
                    }
                    throw retryException;
                }
            }
            if (isDeniedUploadError(s3Exception)) {
                colomboUserManager.evictSession(username, "S3 upload denied");
            }
            throw s3Exception;
        }
    }

    /**
     * Returns {@code true} if the session contains all data required to perform an S3 upload.
     *
     * @param username    the username (used for logging)
     * @param sessionData the session to validate
     * @return {@code true} when the session is ready for upload
     */
    private boolean isValidSessionForUpload(String username, SessionData sessionData) {
        Tenant tenant = sessionData.getTenant();
        if (tenant == null) {
            log.warn("Session exists for username='{}' but tenant data is missing", username);
            return false;
        }

        String assignmentId = sessionData.getAssignmentId();
        if (assignmentId == null || assignmentId.isBlank()) {
            log.warn("Missing assignmentId in session for username='{}'", username);
            return false;
        }

        SessionUploadCredentials uploadCredentials = sessionData.getUploadCredentials();
        if (uploadCredentials == null || !uploadCredentials.isValid()) {
            log.warn("Missing or invalid upload credentials in session for username='{}'", username);
            return false;
        }
        return true;
    }

    /**
     * Builds an authenticated {@link S3Client} from the given temporary credentials.
     *
     * @param uploadCredentials the session upload credentials containing STS tokens
     * @return a configured {@link S3Client}
     */
    private S3Client resolveS3Client(SessionUploadCredentials uploadCredentials) {
        AwsSessionCredentials credentials = AwsSessionCredentials.create(
                uploadCredentials.getAccessKeyId(),
                uploadCredentials.getSecretAccessKey(),
                uploadCredentials.getSessionToken()
        );
        return S3Client.builder()
                .region(Region.of(uploadCredentials.getRegion()))
                .credentialsProvider(StaticCredentialsProvider.create(credentials))
                .build();
    }

    /**
     * Constructs the full S3 object key by joining the key prefix and filename.
     *
     * @param keyPrefix the destination prefix (may or may not end with {@code /})
     * @param filename  the bare filename of the uploaded file
     * @return the full S3 object key
     */
    private String buildObjectKey(String keyPrefix, String filename) {
        if (keyPrefix.endsWith("/")) {
            return keyPrefix + filename;
        }
        return keyPrefix + "/" + filename;
    }

    /**
     * Returns {@code true} if the S3 exception indicates that the session credentials
     * have expired or are otherwise invalid.
     *
     * @param exception the S3 exception to inspect
     * @return {@code true} for {@code ExpiredToken}, {@code RequestExpired}, or {@code InvalidToken} errors
     */
    private boolean isExpiredCredentialError(S3Exception exception) {
        String errorCode = exception.awsErrorDetails() == null ? null : exception.awsErrorDetails().errorCode();
        if (errorCode == null) {
            return false;
        }
        return "ExpiredToken".equalsIgnoreCase(errorCode)
                || "RequestExpired".equalsIgnoreCase(errorCode)
                || "InvalidToken".equalsIgnoreCase(errorCode);
    }

    /**
     * Returns {@code true} if the S3 exception indicates that the upload was denied
     * due to insufficient permissions.
     *
     * @param exception the S3 exception to inspect
     * @return {@code true} for HTTP 403 or {@code AccessDenied} errors
     */
    private boolean isDeniedUploadError(S3Exception exception) {
        String errorCode = exception.awsErrorDetails() == null ? null : exception.awsErrorDetails().errorCode();
        return exception.statusCode() == 403 || "AccessDenied".equalsIgnoreCase(errorCode);
    }

    /**
     * Extracts the filename segment from an {@code s3://bucket/key} URL.
     *
     * @param s3Url the S3 URL; may be {@code null}
     * @return the trailing filename, or {@code "unknown"} if unavailable
     */
    private String extractFilenameFromS3Url(String s3Url) {
        if (s3Url == null || s3Url.isBlank()) {
            return "unknown";
        }
        int lastSlash = s3Url.lastIndexOf('/');
        if (lastSlash < 0 || lastSlash == s3Url.length() - 1) {
            return "unknown";
        }
        return s3Url.substring(lastSlash + 1);
    }

    /**
     * Value object that carries the result of an S3 upload attempt.
     *
     * <p>On success, holds the active {@link SessionData} and the resulting S3 URL.
     * On failure, all fields are {@code null}/{@code false}.
     */
    static final class UploadResult {

        /** Whether the upload succeeded. */
        private final boolean success;

        /** The session data active at the time of upload; {@code null} on failure. */
        private final SessionData sessionData;

        /** The resulting S3 URL; {@code null} on failure. */
        private final String s3Url;
        private final String originalFilename;
        private final String targetFilename;

        /**
         * Creates an {@code UploadResult}.
         *
         * @param success     whether the upload succeeded
         * @param sessionData the session that performed the upload
         * @param s3Url       the S3 URL of the uploaded object
         */
        private UploadResult(boolean success, SessionData sessionData, String s3Url,
                             String originalFilename, String targetFilename) {
            this.success = success;
            this.sessionData = sessionData;
            this.s3Url = s3Url;
            this.originalFilename = originalFilename;
            this.targetFilename = targetFilename;
        }

        /**
         * Creates a successful result.
         *
         * @param sessionData the active session
         * @param s3Url       the URL of the uploaded object
         * @return a successful {@code UploadResult}
         */
        static UploadResult success(SessionData sessionData, String s3Url) {
            String filename = s3Url == null ? null : s3Url.substring(s3Url.lastIndexOf('/') + 1);
            return success(sessionData, s3Url, filename, filename);
        }

        static UploadResult success(SessionData sessionData, String s3Url,
                                    String originalFilename, String targetFilename) {
            return new UploadResult(true, sessionData, s3Url, originalFilename, targetFilename);
        }

        /**
         * Creates a failed result with no session or URL.
         *
         * @return a failed {@code UploadResult}
         */
        static UploadResult failure() {
            return new UploadResult(false, null, null, null, null);
        }

        /**
         * Returns {@code true} if the upload was successful.
         *
         * @return {@code true} on success
         */
        boolean success() {
            return success;
        }

        /**
         * Returns the session data associated with the upload.
         *
         * @return the active {@link SessionData}, or {@code null} on failure
         */
        SessionData sessionData() {
            return sessionData;
        }

        /**
         * Returns the S3 URL of the uploaded object.
         *
         * @return the S3 URL, or {@code null} on failure
         */
        String s3Url() {
            return s3Url;
        }

        String originalFilename() {
            return originalFilename;
        }

        String targetFilename() {
            return targetFilename;
        }
    }

    private record NamedUpload(String s3Url, String targetFilename) {
    }
}
