package com.gaulatti.colombo.ftp;

import com.gaulatti.colombo.model.Tenant;
import com.gaulatti.colombo.service.UploadService;
import java.io.File;
import java.nio.file.Paths;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.ftpserver.ftplet.DefaultFtplet;
import org.apache.ftpserver.ftplet.FtpException;
import org.apache.ftpserver.ftplet.FtpReply;
import org.apache.ftpserver.ftplet.FtpRequest;
import org.apache.ftpserver.ftplet.FtpSession;
import org.apache.ftpserver.ftplet.Ftplet;
import org.apache.ftpserver.ftplet.FtpletContext;
import org.apache.ftpserver.ftplet.FtpletResult;

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

    /**
     * Session-attribute key prefix used to mark uploads that have already been
     * processed, preventing double-handling of the same {@code STOR} transfer.
     */
    private static final String UPLOAD_PROCESSED_ATTR_PREFIX = "colombo.upload.processed:";

    /** Shared concurrent map of active session data keyed by FTP username. */
    private final ConcurrentHashMap<String, SessionData> sessions;

    /** Service delegate for S3 upload and CMS photo-callback operations. */
    private final UploadService uploadService;

    /**
     * Creates a new {@code ColomboFtplet}.
     *
     * @param sessions      shared session map
     * @param uploadService service delegate for S3 upload and CMS photo callback
     */
    public ColomboFtplet(
            ConcurrentHashMap<String, SessionData> sessions,
            UploadService uploadService
    ) {
        this.sessions = sessions;
        this.uploadService = uploadService;
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
            boolean success = uploadService.processFtpUpload(username, filename, localFile);
            if (!success) {
                return failureResult(disconnectOnFailure);
            }
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
}
