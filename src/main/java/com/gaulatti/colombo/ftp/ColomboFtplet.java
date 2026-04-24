package com.gaulatti.colombo.ftp;

import com.gaulatti.colombo.model.Tenant;
import java.io.File;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.ftpserver.ftplet.Authentication;
import org.apache.ftpserver.ftplet.DefaultFtplet;
import org.apache.ftpserver.ftplet.FtpException;
import org.apache.ftpserver.ftplet.FtpReply;
import org.apache.ftpserver.ftplet.FtpRequest;
import org.apache.ftpserver.ftplet.FtpSession;
import org.apache.ftpserver.ftplet.Ftplet;
import org.apache.ftpserver.ftplet.FtpletContext;
import org.apache.ftpserver.ftplet.FtpletResult;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
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
 * Custom Apache FTP server ftplet that intercepts FTP lifecycle events for Colombo.
 *
 * <p>Handles session connect/disconnect logging, command tracing, and post-upload
 * processing (S3 upload + CMS photo callback). Implements deduplication to guard
 * against duplicate upload events that the Apache FTP server may fire for a single
 * {@code STOR} command.
 */
@Slf4j
public class ColomboFtplet extends DefaultFtplet implements Ftplet {

    /** HTTP header name used to authenticate outbound requests to the CMS. */
    private static final String API_KEY_HEADER = "X-Colombo-API-Key";

    /**
     * Session-attribute key prefix used to mark uploads that have already been
     * processed, preventing double-handling of the same {@code STOR} transfer.
     */
    private static final String UPLOAD_PROCESSED_ATTR_PREFIX = "colombo.upload.processed:";

    /** Shared concurrent map of active session data keyed by FTP username. */
    private final ConcurrentHashMap<String, SessionData> sessions;

    /** HTTP client for outbound calls to the CMS. */
    private final RestTemplate restTemplate;

    /** User manager used for session refresh and eviction operations. */
    private final ColomboUserManager colomboUserManager;

    /**
     * Creates a new {@code ColomboFtplet}.
     *
     * @param sessions           shared session map
     * @param restTemplate       HTTP client for CMS calls
     * @param colomboUserManager user manager for session refresh and eviction
     */
    public ColomboFtplet(
            ConcurrentHashMap<String, SessionData> sessions,
            RestTemplate restTemplate,
            ColomboUserManager colomboUserManager
    ) {
        this.sessions = sessions;
        this.restTemplate = restTemplate;
        this.colomboUserManager = colomboUserManager;
    }

    /**
     * Initialises the ftplet within the server context.
     *
     * @param ftpletContext the server-provided context
     * @throws FtpException if initialisation fails
     */
    @Override
    public void init(FtpletContext ftpletContext) throws FtpException {
        log.info("ColomboFtplet initialized");
    }

    /**
     * Releases any resources held by the ftplet when the server shuts down.
     */
    @Override
    public void destroy() {
        log.info("ColomboFtplet destroyed");
    }

    /**
     * Called when a new client connects to the FTP server. Logs the connection event.
     *
     * @param session the newly established FTP session
     * @return {@link FtpletResult#DEFAULT} to continue normal processing
     */
    @Override
    public FtpletResult onConnect(FtpSession session) {
        log.info("[CONNECT] sessionId='{}' remoteAddress='{}'",
                session.getSessionId(), session.getClientAddress());
        return FtpletResult.DEFAULT;
    }

    /**
     * Called when a client disconnects from the FTP server. Logs the disconnection event.
     *
     * @param session the FTP session that has ended
     * @return {@link FtpletResult#DEFAULT} to continue normal processing
     */
    @Override
    public FtpletResult onDisconnect(FtpSession session) {
        String username = extractUsername(session);
        log.info("[DISCONNECT] sessionId='{}' username='{}' remoteAddress='{}'",
                session.getSessionId(), username, session.getClientAddress());
        return FtpletResult.DEFAULT;
    }

    /**
     * Called before every FTP command is processed. Emits a debug log entry.
     *
     * @param session the active FTP session
     * @param request the incoming FTP command request
     * @return {@link FtpletResult#DEFAULT} to allow the command to proceed
     */
    @Override
    public FtpletResult beforeCommand(FtpSession session, FtpRequest request) {
        String username = extractUsername(session);
        log.debug("[CMD >>>] sessionId='{}' username='{}' command='{}' argument='{}'",
                session.getSessionId(), username,
                request == null ? null : request.getCommand(),
                request == null ? null : request.getArgument());
        return FtpletResult.DEFAULT;
    }

    /**
     * Called after every FTP command has been processed. Triggers upload handling
     * when a {@code STOR} command completes with a 2xx reply code.
     *
     * @param session the active FTP session
     * @param request the FTP command that was processed
     * @param reply   the server's reply to the command
     * @return {@link FtpletResult#DEFAULT} to continue normal processing
     */
    @Override
    public FtpletResult afterCommand(FtpSession session, FtpRequest request, FtpReply reply) {
        String username = extractUsername(session);
        log.debug("[CMD <<<] sessionId='{}' username='{}' command='{}' replyCode='{}' replyMessage='{}'",
                session.getSessionId(), username,
                request == null ? null : request.getCommand(),
                reply == null ? null : reply.getCode(),
                reply == null ? null : reply.getMessage());

        if (isStorCommand(request) && isSuccessfulUploadReply(reply)) {
            processUpload(session, request, "afterCommand", false);
        }
        return FtpletResult.DEFAULT;
    }

    /**
     * Called when a file upload (STOR) has completed. Delegates to {@link #processUpload}
     * with disconnect-on-failure enabled.
     *
     * @param session the active FTP session
     * @param request the STOR command request
     * @return the result of upload processing
     */
    @Override
    public FtpletResult onUploadEnd(FtpSession session, FtpRequest request) {
        return processUpload(session, request, "onUploadEnd", true);
    }

    /**
     * Core upload handler shared by {@link #onUploadEnd} and {@link #afterCommand}.
     *
     * <p>Validates the session, resolves the uploaded file on disk, pushes it to S3,
     * and fires the CMS photo callback. Deduplication is enforced via a session attribute
     * so that the same transfer is not processed twice.
     *
     * @param session              the active FTP session
     * @param request              the STOR command request
     * @param source               a label identifying the call site (for logging)
     * @param disconnectOnFailure  whether to disconnect the client on a processing error
     * @return {@link FtpletResult#DEFAULT} on success, or disconnect/default on failure
     */
    private FtpletResult processUpload(FtpSession session, FtpRequest request, String source, boolean disconnectOnFailure) {
        Object sessionId = session == null ? null : session.getSessionId();
        String username = extractUsername(session);
        String rawArgument = request == null ? null : request.getArgument();
        String filename = extractFilename(rawArgument);
        String markerKey = UPLOAD_PROCESSED_ATTR_PREFIX + (rawArgument == null ? "<null>" : rawArgument);

        if (isUploadAlreadyProcessed(session, markerKey)) {
            log.debug("[UPLOAD] skipping duplicate processing source='{}' sessionId='{}' username='{}' argument='{}'",
                source, sessionId, username, rawArgument);
            return FtpletResult.DEFAULT;
        }

        log.info("[UPLOAD] processing source='{}' sessionId='{}' username='{}' argument='{}'",
            source, sessionId, username, rawArgument);

        if (username == null || username.isBlank()) {
            log.warn("Missing FTP username in session for upload argument='{}'", rawArgument);
            return failureResult(disconnectOnFailure);
        }

        SessionData sessionData = sessions.get(username);
        if (sessionData == null) {
            log.warn("No in-memory session found for ftpUsername='{}'", username);
            return failureResult(disconnectOnFailure);
        }

        Tenant tenant = sessionData.getTenant();
        if (tenant == null) {
            log.warn("Session exists for ftpUsername='{}' but tenant data is missing", username);
            return failureResult(disconnectOnFailure);
        }

        File localFile = resolvePhysicalFile(session, rawArgument);
        if (localFile == null || !localFile.exists() || !localFile.isFile()) {
            log.warn("Uploaded file not found or invalid for ftpUsername='{}', argument='{}'", username, rawArgument);
            return failureResult(disconnectOnFailure);
        }

        try {
            UploadResult uploadResult = uploadToS3WithRefresh(username, filename, localFile);
            if (!uploadResult.success()) {
                return failureResult(disconnectOnFailure);
            }
            SessionData activeSession = uploadResult.sessionData();
            postPhotoCallback(activeSession.getTenant(), activeSession.getAssignmentId(), uploadResult.s3Url(), username);
            markUploadProcessed(session, markerKey);
                log.info("Upload processed for ftpUsername='{}', file='{}'",
                    username, filename);
            return FtpletResult.DEFAULT;
        } catch (Exception ex) {
            log.error("Unrecoverable upload processing error for ftpUsername='{}', file='{}'", username, filename, ex);
            return failureResult(disconnectOnFailure);
        }
    }

    /**
     * Returns the appropriate {@link FtpletResult} for a failed upload.
     *
     * @param disconnectOnFailure if {@code true}, returns {@link FtpletResult#DISCONNECT};
     *                            otherwise returns {@link FtpletResult#DEFAULT}
     * @return the failure result
     */
    private FtpletResult failureResult(boolean disconnectOnFailure) {
        return disconnectOnFailure ? FtpletResult.DISCONNECT : FtpletResult.DEFAULT;
    }

    /**
     * Called when a file upload starts. Logs the event.
     *
     * @param session the active FTP session
     * @param request the STOR command request
     * @return {@link FtpletResult#DEFAULT}
     */
    @Override
    public FtpletResult onUploadStart(FtpSession session, FtpRequest request) {
        log.info("[UPLOAD START] sessionId='{}' username='{}' argument='{}'",
                session.getSessionId(), extractUsername(session),
                request == null ? null : request.getArgument());
        return FtpletResult.DEFAULT;
    }

    /**
     * Called when a unique-name file upload starts. Logs the event.
     *
     * @param session the active FTP session
     * @param request the STOU command request
     * @return {@link FtpletResult#DEFAULT}
     */
    @Override
    public FtpletResult onUploadUniqueStart(FtpSession session, FtpRequest request) {
        log.info("[UPLOAD UNIQUE START] sessionId='{}' username='{}' argument='{}'",
                session.getSessionId(), extractUsername(session),
                request == null ? null : request.getArgument());
        return FtpletResult.DEFAULT;
    }

    /**
     * Called when a unique-name file upload ends. Logs the event.
     *
     * @param session the active FTP session
     * @param request the STOU command request
     * @return {@link FtpletResult#DEFAULT}
     */
    @Override
    public FtpletResult onUploadUniqueEnd(FtpSession session, FtpRequest request) {
        log.info("[UPLOAD UNIQUE END] sessionId='{}' username='{}' argument='{}'",
                session.getSessionId(), extractUsername(session),
                request == null ? null : request.getArgument());
        return FtpletResult.DEFAULT;
    }

    /**
     * Called when an append operation starts. Logs the event.
     *
     * @param session the active FTP session
     * @param request the APPE command request
     * @return {@link FtpletResult#DEFAULT}
     */
    @Override
    public FtpletResult onAppendStart(FtpSession session, FtpRequest request) {
        log.info("[APPEND START] sessionId='{}' username='{}' argument='{}'",
                session.getSessionId(), extractUsername(session),
                request == null ? null : request.getArgument());
        return FtpletResult.DEFAULT;
    }

    /**
     * Called when an append operation ends. Logs the event.
     *
     * @param session the active FTP session
     * @param request the APPE command request
     * @return {@link FtpletResult#DEFAULT}
     */
    @Override
    public FtpletResult onAppendEnd(FtpSession session, FtpRequest request) {
        log.info("[APPEND END] sessionId='{}' username='{}' argument='{}'",
                session.getSessionId(), extractUsername(session),
                request == null ? null : request.getArgument());
        return FtpletResult.DEFAULT;
    }

    /**
     * Called when a download starts. Logs the event.
     *
     * @param session the active FTP session
     * @param request the RETR command request
     * @return {@link FtpletResult#DEFAULT}
     */
    @Override
    public FtpletResult onDownloadStart(FtpSession session, FtpRequest request) {
        log.info("[DOWNLOAD START] sessionId='{}' username='{}' argument='{}'",
                session.getSessionId(), extractUsername(session),
                request == null ? null : request.getArgument());
        return FtpletResult.DEFAULT;
    }

    /**
     * Called when a download ends. Logs the event.
     *
     * @param session the active FTP session
     * @param request the RETR command request
     * @return {@link FtpletResult#DEFAULT}
     */
    @Override
    public FtpletResult onDownloadEnd(FtpSession session, FtpRequest request) {
        log.info("[DOWNLOAD END] sessionId='{}' username='{}' argument='{}'",
                session.getSessionId(), extractUsername(session),
                request == null ? null : request.getArgument());
        return FtpletResult.DEFAULT;
    }

    /**
     * Called after a user has successfully logged in. Logs the event.
     *
     * @param session the active FTP session
     * @param request the login command request
     * @return {@link FtpletResult#DEFAULT}
     */
    @Override
    public FtpletResult onLogin(FtpSession session, FtpRequest request) {
        log.info("[LOGIN] sessionId='{}' username='{}' remoteAddress='{}'",
                session.getSessionId(), extractUsername(session), session.getClientAddress());
        return FtpletResult.DEFAULT;
    }

    /**
     * Extracts the authenticated username from the session.
     *
     * @param session the FTP session; may be {@code null}
     * @return the username, or {@code null} if the session or user is absent
     */
    private String extractUsername(FtpSession session) {
        if (session == null || session.getUser() == null) {
            return null;
        }
        return session.getUser().getName();
    }

    /**
     * Extracts the bare filename from a raw FTP path argument.
     *
     * @param argument the raw argument from the FTP request; may be {@code null}
     * @return the filename component, or {@code "unknown"} if the argument is blank
     */
    private String extractFilename(String argument) {
        if (argument == null || argument.isBlank()) {
            return "unknown";
        }
        return Paths.get(argument).getFileName().toString();
    }

    /**
     * Resolves the physical {@link File} on the server's filesystem corresponding
     * to the given FTP path argument.
     *
     * @param session  the active FTP session
     * @param argument the virtual FTP path from the request
     * @return the resolved {@link File}, or {@code null} if it cannot be determined
     */
    private File resolvePhysicalFile(FtpSession session, String argument) {
        if (session == null || session.getFileSystemView() == null || argument == null || argument.isBlank()) {
            return null;
        }

        try {
            Object physical = session.getFileSystemView().getFile(argument).getPhysicalFile();
            if (physical instanceof File file) {
                return file;
            }
        } catch (FtpException ex) {
            log.error("Unable to resolve physical file for argument='{}'", argument, ex);
        }

        return null;
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
    private UploadResult uploadToS3WithRefresh(String username, String filename, File localFile) {
        SessionData sessionData = sessions.get(username);
        if (sessionData == null) {
            log.warn("No in-memory session found for ftpUsername='{}'", username);
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
                log.warn("[S3 UPLOAD] expired credentials ftpUsername='{}' — refreshing via validate endpoint", username);
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
     * Performs the actual S3 {@code PutObject} call for the given session and file.
     *
     * @param sessionData the session providing upload credentials and assignment context
     * @param username    the FTP username (used for logging)
     * @param filename    the bare filename to append to the key prefix
     * @param localFile   the file to upload
     * @return the resulting S3 URL in {@code s3://bucket/key} format
     */
    private String uploadToS3(SessionData sessionData, String username, String filename, File localFile) {
        String assignmentId = sessionData.getAssignmentId();
        SessionUploadCredentials uploadCredentials = sessionData.getUploadCredentials();
        String s3Key = buildObjectKey(uploadCredentials.getKeyPrefix(), filename);
        String bucket = uploadCredentials.getBucket();

        log.info("[S3 UPLOAD] start ftpUsername='{}' assignmentId='{}' localFile='{}' bucket='{}' key='{}'",
                username, assignmentId, localFile.getAbsolutePath(), bucket, s3Key);
        S3Client client = resolveS3Client(uploadCredentials);
        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(bucket)
                .key(s3Key)
                .build();
        client.putObject(putObjectRequest, RequestBody.fromFile(localFile.toPath()));

        String s3Url = "s3://" + bucket + "/" + s3Key;
        log.info("[S3 UPLOAD] success ftpUsername='{}' assignmentId='{}' file='{}'", username, assignmentId, filename);
        return s3Url;
    }

    /**
     * Returns {@code true} if the session contains all data required to perform an S3 upload.
     *
     * @param username    the FTP username (used for logging)
     * @param sessionData the session to validate
     * @return {@code true} when the session is ready for upload
     */
    private boolean isValidSessionForUpload(String username, SessionData sessionData) {
        Tenant tenant = sessionData.getTenant();
        if (tenant == null) {
            log.warn("Session exists for ftpUsername='{}' but tenant data is missing", username);
            return false;
        }

        String assignmentId = sessionData.getAssignmentId();
        if (assignmentId == null || assignmentId.isBlank()) {
            log.warn("Missing assignmentId in session for ftpUsername='{}'", username);
            return false;
        }

        SessionUploadCredentials uploadCredentials = sessionData.getUploadCredentials();
        if (uploadCredentials == null || !uploadCredentials.isValid()) {
            log.warn("Missing or invalid upload credentials in session for ftpUsername='{}'", username);
            return false;
        }
        return true;
    }

    /**
     * Sends a photo-uploaded callback to the CMS, notifying it of the S3 URL.
     *
     * @param tenant       the tenant whose CMS endpoint should be called
     * @param assignmentId the assignment associated with the upload
     * @param s3Url        the S3 URL of the uploaded file
     * @param username     the FTP username (used for session eviction on denial)
     */
    private void postPhotoCallback(Tenant tenant, String assignmentId, String s3Url, String username) {
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
     * Returns {@code true} if the given FTP request is a {@code STOR} command.
     *
     * @param request the FTP request to inspect; may be {@code null}
     * @return {@code true} for STOR commands
     */
    private boolean isStorCommand(FtpRequest request) {
        if (request == null || request.getCommand() == null) {
            return false;
        }
        return "STOR".equalsIgnoreCase(request.getCommand());
    }

    /**
     * Returns {@code true} if the FTP reply indicates a successful transfer (2xx).
     *
     * @param reply the FTP reply to inspect; may be {@code null}
     * @return {@code true} for 2xx reply codes
     */
    private boolean isSuccessfulUploadReply(FtpReply reply) {
        if (reply == null) {
            return false;
        }
        return reply.getCode() >= 200 && reply.getCode() < 300;
    }

    /**
     * Returns {@code true} if the upload identified by {@code markerKey} has already
     * been processed in this session, preventing duplicate handling.
     *
     * @param session   the FTP session holding attributes
     * @param markerKey the attribute key used to flag processed uploads
     * @return {@code true} if the upload was already handled
     */
    private boolean isUploadAlreadyProcessed(FtpSession session, String markerKey) {
        if (session == null) {
            return false;
        }
        Object markerValue = session.getAttribute(markerKey);
        return markerValue instanceof Boolean processed && processed;
    }

    /**
     * Stores a boolean flag in the session to mark an upload as processed.
     *
     * @param session   the FTP session holding attributes
     * @param markerKey the attribute key to set
     */
    private void markUploadProcessed(FtpSession session, String markerKey) {
        if (session != null) {
            session.setAttribute(markerKey, Boolean.TRUE);
        }
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
    private static final class UploadResult {
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
        private static UploadResult success(SessionData sessionData, String s3Url) {
            return new UploadResult(true, sessionData, s3Url);
        }

        /**
         * Creates a failed result with no session or URL.
         *
         * @return a failed {@code UploadResult}
         */
        private static UploadResult failure() {
            return new UploadResult(false, null, null);
        }

        /**
         * Returns {@code true} if the upload was successful.
         *
         * @return {@code true} on success
         */
        private boolean success() {
            return success;
        }

        /**
         * Returns the session data associated with the upload.
         *
         * @return the active {@link SessionData}, or {@code null} on failure
         */
        private SessionData sessionData() {
            return sessionData;
        }

        /**
         * Returns the S3 URL of the uploaded object.
         *
         * @return the S3 URL, or {@code null} on failure
         */
        private String s3Url() {
            return s3Url;
        }
    }
}
