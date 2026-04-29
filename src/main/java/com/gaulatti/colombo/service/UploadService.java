package com.gaulatti.colombo.service;

import com.gaulatti.colombo.ftp.ColomboUserManager;
import com.gaulatti.colombo.ftp.SessionData;
import com.gaulatti.colombo.ftp.SessionUploadCredentials;
import com.gaulatti.colombo.model.Tenant;
import java.io.File;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
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
 * HTTP callers invoke {@link #uploadToS3(SessionData, String, String, File)} and
 * {@link #postPhotoCallback(Tenant, String, String, String)} directly with a
 * pre-validated {@link SessionData} obtained from the CMS.
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

    /**
     * Creates a new {@code UploadService}.
     *
     * @param sessions           shared session map
     * @param restTemplate       HTTP client for CMS calls
     * @param colomboUserManager user manager for session refresh and eviction
     */
    public UploadService(
            ConcurrentHashMap<String, SessionData> sessions,
            RestTemplate restTemplate,
            ColomboUserManager colomboUserManager
    ) {
        this.sessions = sessions;
        this.restTemplate = restTemplate;
        this.colomboUserManager = colomboUserManager;
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
        postPhotoCallback(activeSession.getTenant(), activeSession.getAssignmentId(), result.s3Url(), username);
        return true;
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
        String assignmentId = sessionData.getAssignmentId();
        SessionUploadCredentials uploadCredentials = sessionData.getUploadCredentials();
        String s3Key = buildObjectKey(uploadCredentials.getKeyPrefix(), filename);
        String bucket = uploadCredentials.getBucket();

        log.info("[S3 UPLOAD] start username='{}' assignmentId='{}' localFile='{}' bucket='{}' key='{}'",
                username, assignmentId, localFile.getAbsolutePath(), bucket, s3Key);
        S3Client client = resolveS3Client(uploadCredentials);
        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(bucket)
                .key(s3Key)
                .build();
        client.putObject(putObjectRequest, RequestBody.fromFile(localFile.toPath()));

        String s3Url = "s3://" + bucket + "/" + s3Key;
        log.info("[S3 UPLOAD] success username='{}' assignmentId='{}' file='{}'", username, assignmentId, filename);
        return s3Url;
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
        HttpHeaders headers = new HttpHeaders();
        headers.set(API_KEY_HEADER, tenant.getApiKey());

        Map<String, String> body = new LinkedHashMap<>();
        body.put("assignment_id", assignmentId);
        body.put("s3_url", s3Url);

        log.info("[PHOTO CALLBACK] sending assignmentId='{}' file='{}'",
                assignmentId, extractFilenameFromS3Url(s3Url));

        try {
            ResponseEntity<Void> response = restTemplate.exchange(
                    tenant.getPhotoEndpoint(),
                    HttpMethod.POST,
                    new HttpEntity<>(body, headers),
                    Void.class
            );

            if (!HttpStatus.OK.equals(response.getStatusCode())) {
                if (response.getStatusCode().is4xxClientError()) {
                    colomboUserManager.evictSession(username, "photo callback denied");
                }
                throw new IllegalStateException("Photo callback failed with status: " + response.getStatusCode());
            }

            log.info("[PHOTO CALLBACK] accepted assignmentId='{}' status='{}'", assignmentId, response.getStatusCode());
        } catch (HttpStatusCodeException ex) {
            if (ex.getStatusCode().is4xxClientError()) {
                colomboUserManager.evictSession(username, "photo callback denied");
            }
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
            String s3Url = uploadToS3(sessionData, username, filename, localFile);
            return UploadResult.success(sessionData, s3Url);
        } catch (S3Exception s3Exception) {
            if (isExpiredCredentialError(s3Exception)) {
                log.warn("[S3 UPLOAD] expired credentials username='{}' — refreshing via validate endpoint", username);
                ColomboUserManager.RefreshResult refreshResult = colomboUserManager.refreshSessionFromValidation(username);
                if (refreshResult != ColomboUserManager.RefreshResult.REFRESHED) {
                    colomboUserManager.evictSession(username, "validate refresh denied/failed after expired S3 credentials");
                    return UploadResult.failure();
                }
                SessionData refreshedSession = sessions.get(username);
                if (refreshedSession == null || !isValidSessionForUpload(username, refreshedSession)) {
                    colomboUserManager.evictSession(username, "refreshed session invalid after validate refresh");
                    return UploadResult.failure();
                }
                try {
                    return UploadResult.success(
                            refreshedSession,
                            uploadToS3(refreshedSession, username, filename, localFile)
                    );
                } catch (S3Exception retryException) {
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

        /**
         * Creates an {@code UploadResult}.
         *
         * @param success     whether the upload succeeded
         * @param sessionData the session that performed the upload
         * @param s3Url       the S3 URL of the uploaded object
         */
        private UploadResult(boolean success, SessionData sessionData, String s3Url) {
            this.success = success;
            this.sessionData = sessionData;
            this.s3Url = s3Url;
        }

        /**
         * Creates a successful result.
         *
         * @param sessionData the active session
         * @param s3Url       the URL of the uploaded object
         * @return a successful {@code UploadResult}
         */
        static UploadResult success(SessionData sessionData, String s3Url) {
            return new UploadResult(true, sessionData, s3Url);
        }

        /**
         * Creates a failed result with no session or URL.
         *
         * @return a failed {@code UploadResult}
         */
        static UploadResult failure() {
            return new UploadResult(false, null, null);
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
    }
}
