package com.gaulatti.colombo.ftp;

import com.gaulatti.colombo.model.Tenant;
import com.gaulatti.colombo.repository.TenantRepository;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ftpserver.ftplet.Authentication;
import org.apache.ftpserver.ftplet.AuthenticationFailedException;
import org.apache.ftpserver.ftplet.FtpException;
import org.apache.ftpserver.ftplet.User;
import org.apache.ftpserver.ftplet.UserManager;
import org.apache.ftpserver.usermanager.UsernamePasswordAuthentication;
import org.apache.ftpserver.usermanager.impl.BaseUser;
import org.apache.ftpserver.usermanager.impl.ConcurrentLoginPermission;
import org.apache.ftpserver.usermanager.impl.WritePermission;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.RestTemplate;

/**
 * Apache FTP {@link UserManager} implementation that authenticates FTP users
 * against a tenant-specific CMS validation endpoint.
 *
 * <p>On successful login, temporary AWS upload credentials returned by the CMS are
 * stored in an in-memory session map. The manager also supports credential refresh
 * (re-validation without a full logout/login cycle) and a configurable master-password
 * bypass for support operations.
 */
public class ColomboUserManager implements UserManager {


    private static final Logger log = LoggerFactory.getLogger(ColomboUserManager.class);

    /**
     * Key used to extract the assignment ID from a CMS validation response.
     */
    private static final String ASSIGNMENT_ID_KEY = "assignmentId";

    /**
     * Key used to locate the upload credentials block in a CMS validation response.
     */
    private static final String UPLOAD_KEY = "upload";

    /**
     * AWS access key ID field within the upload credentials block.
     */
    private static final String ACCESS_KEY_ID_KEY = "accessKeyId";

    /**
     * AWS secret access key field within the upload credentials block.
     */
    private static final String SECRET_ACCESS_KEY_KEY = "secretAccessKey";

    /**
     * AWS session token field within the upload credentials block.
     */
    private static final String SESSION_TOKEN_KEY = "sessionToken";

    /**
     * AWS region field within the upload credentials block.
     */
    private static final String REGION_KEY = "region";

    /**
     * S3 bucket name field within the upload credentials block.
     */
    private static final String BUCKET_KEY = "bucket";

    /**
     * S3 key prefix field within the upload credentials block.
     */
    private static final String KEY_PREFIX_KEY = "keyPrefix";

    /**
     * Credential expiry timestamp field within the upload credentials block.
     */
    private static final String EXPIRES_AT_KEY = "expiresAt";

    /**
     * Repository for resolving tenants by FTP username.
     */
    private final TenantRepository tenantRepository;

    /**
     * HTTP client used to call CMS validation endpoints.
     */
    private final RestTemplate restTemplate;

    /**
     * Shared concurrent map of active session data keyed by FTP username.
     */
    private final ConcurrentHashMap<String, SessionData> sessions;

    /**
     * Optional master password that, when matched, bypasses per-tenant CMS validation.
     * Intended solely for support access.
     */
    private final String configuredMasterPassword;

    /**
     * Creates a new {@code ColomboUserManager}.
     *
     * @param tenantRepository        repository for resolving tenant records
     * @param restTemplate            HTTP client for CMS validation calls
     * @param sessions                shared session map
     * @param configuredMasterPassword optional master password for support bypass
     */
    public ColomboUserManager(
            TenantRepository tenantRepository,
            RestTemplate restTemplate,
            ConcurrentHashMap<String, SessionData> sessions,
            String configuredMasterPassword
    ) {
        this.tenantRepository = tenantRepository;
        this.restTemplate = restTemplate;
        this.sessions = sessions;
        this.configuredMasterPassword = configuredMasterPassword;
    }

    /**
     * Returns an empty string; this implementation does not use an admin account.
     *
     * @return empty string
     */
    @Override
    public String getAdminName() {
        return "";
    }

    /**
     * Looks up an FTP {@link User} by username, resolving the backing {@link Tenant}.
     *
     * @param username the FTP login name
     * @return a {@link User} representing the tenant
     * @throws FtpException if no tenant is registered for the given username
     */
    @Override
    public User getUserByName(String username) throws FtpException {
        log.info("[USER LOOKUP] username='{}'", username);
        Tenant tenant = tenantRepository.findByFtpUsername(username)
                .orElseThrow(() -> {
                    log.warn("[USER LOOKUP] no tenant found for username='{}'", username);
                    return new FtpException("Tenant not found for username: " + username);
                });
        log.info("[USER LOOKUP] resolved tenant='{}' ftpUsername='{}'", tenant.getName(), username);
        return new ColomboUser(tenant);
    }

    /**
     * Returns an empty array; user enumeration is not supported.
     *
     * @return empty string array
     */
    @Override
    public String[] getAllUserNames() {
        return new String[0];
    }

    /**
     * Required by {@link UserManager}; user deletion is managed externally via the CMS.
     *
     * @param username the username to delete (ignored)
     */
    @Override
    public void delete(String username) {
    }

    /**
     * Required by {@link UserManager}; user persistence is managed externally via the CMS.
     *
     * @param user the user to save (ignored)
     */
    @Override
    public void save(User user) {
    }

    /**
     * Returns {@code true} if a tenant with the given FTP username exists in the database.
     *
     * @param username the FTP login name to check
     * @return {@code true} if the username is registered
     */
    @Override
    public boolean doesExist(String username) {
        boolean exists = tenantRepository.findByFtpUsername(username).isPresent();
        if (exists) {
            log.info("[USER EXISTS] username='{}' found in tenant registry", username);
        } else {
            log.warn("[USER EXISTS] username='{}' not found in tenant registry — rejecting", username);
        }
        return exists;
    }

    /**
     * Authenticates an FTP user against the CMS validation endpoint.
     *
     * <p>Only {@link UsernamePasswordAuthentication} is accepted. If a master password
     * is configured and matches, the CMS call is bypassed and a synthetic support session
     * is created. Otherwise, the password is forwarded to the tenant's validation endpoint
     * and the resulting upload credentials are stored in the session map.
     *
     * @param authentication the authentication token provided by the FTP client
     * @return the authenticated {@link User}
     * @throws AuthenticationFailedException if credentials are invalid or the CMS rejects them
     */
    @Override
    public User authenticate(Authentication authentication) throws AuthenticationFailedException {
        if (!(authentication instanceof UsernamePasswordAuthentication usernamePasswordAuthentication)) {
            log.warn("[AUTH] unsupported authentication type='{}'", authentication.getClass().getSimpleName());
            throw new AuthenticationFailedException("Unsupported authentication type");
        }

        String username = usernamePasswordAuthentication.getUsername();
        String password = usernamePasswordAuthentication.getPassword();

        log.info("[AUTH] login attempt username='{}' passwordLength='{}'", username, password == null ? 0 : password.length());

        Tenant tenant = tenantRepository.findByFtpUsername(username)
                .orElseThrow(() -> {
                    log.warn("[AUTH] unknown FTP user username='{}'", username);
                    return new AuthenticationFailedException("Unknown FTP user");
                });

        log.info("[AUTH] found tenant='{}' for username='{}', calling validationEndpoint='{}'",
                tenant.getName(), username, tenant.getValidationEndpoint());

        String masterPassword = firstNonBlank(
                configuredMasterPassword,
                System.getenv("COLOMBO_MASTER_PASSWORD"),
                System.getProperty("COLOMBO_MASTER_PASSWORD"),
                System.getenv("COLOMBO_DEV_PASSWORD"),
                System.getProperty("COLOMBO_DEV_PASSWORD")
        );
        if (isMasterPasswordBypass(masterPassword, password)) {
            String supportAssignmentId = "support-assignment-" + username;
            log.warn("[AUTH] MASTER PASSWORD support bypass used for username='{}' assignmentId='{}'",
                    username, supportAssignmentId);
            sessions.put(username, new SessionData(tenant, supportAssignmentId, null, null));
            return new ColomboUser(tenant);
        }

        ValidationResult validationResult = validateAgainstCms(tenant, username, password, "login");
        if (validationResult.status == ValidationStatus.DENIED) {
            sessions.remove(username);
            throw new AuthenticationFailedException("Tenant validation rejected credentials");
        }
        if (validationResult.status != ValidationStatus.SUCCESS) {
            throw new AuthenticationFailedException("Tenant validation request failed");
        }
        SessionData refreshedSession = validationResult.sessionData;
        log.info("[AUTH] success username='{}' assignmentId='{}' tenant='{}' uploadBucket='{}' uploadRegion='{}' uploadKeyPrefix='{}'",
                username, refreshedSession.getAssignmentId(), tenant.getName(),
                refreshedSession.getUploadCredentials().getBucket(), refreshedSession.getUploadCredentials().getRegion(),
                refreshedSession.getUploadCredentials().getKeyPrefix());
        sessions.put(username, refreshedSession);
        return new ColomboUser(tenant);
    }

    /**
     * Always returns {@code false}; Colombo does not designate admin users.
     *
     * @param username the username to check
     * @return {@code false}
     */
    @Override
    public boolean isAdmin(String username) {
        log.debug("[IS ADMIN] username='{}' → false", username);
        return false;
    }

    /**
     * Extracts the assignment ID string from a raw CMS response body.
     *
     * @param responseBody the deserialized response map; may be {@code null}
     * @return the assignment ID, or {@code null} if absent
     */
    private String extractAssignmentId(Map responseBody) {
        if (responseBody == null) {
            return null;
        }
        Object assignmentId = responseBody.get(ASSIGNMENT_ID_KEY);
        return assignmentId == null ? null : assignmentId.toString();
    }

    /**
     * Extracts the upload credentials block from a raw CMS response body.
     *
     * @param responseBody the deserialized response map; may be {@code null}
     * @return a populated {@link SessionUploadCredentials}, or {@code null} if the
     *         upload block is absent or not a {@link Map}
     */
    @SuppressWarnings("rawtypes")
    private SessionUploadCredentials extractUploadCredentials(Map responseBody) {
        if (responseBody == null) {
            return null;
        }

        Object uploadObj = responseBody.get(UPLOAD_KEY);
        if (!(uploadObj instanceof Map uploadMap)) {
            return null;
        }

        return new SessionUploadCredentials(
                asString(uploadMap.get(ACCESS_KEY_ID_KEY)),
                asString(uploadMap.get(SECRET_ACCESS_KEY_KEY)),
                asString(uploadMap.get(SESSION_TOKEN_KEY)),
                asString(uploadMap.get(REGION_KEY)),
                asString(uploadMap.get(BUCKET_KEY)),
                asString(uploadMap.get(KEY_PREFIX_KEY)),
                asString(uploadMap.get(EXPIRES_AT_KEY))
        );
    }

    /**
     * Converts an object to its string representation, returning {@code null} for {@code null} input.
     *
     * @param value the object to convert
     * @return the string value, or {@code null}
     */
    private String asString(Object value) {
        return value == null ? null : value.toString();
    }

    /**
     * Re-validates the session for the given username against the CMS using the
     * stored validation key, and updates the session map with fresh credentials.
     *
     * <p>Called automatically by {@link ColomboFtplet} when an S3 upload fails due to
     * expired credentials.
     *
     * @param username the FTP username whose session should be refreshed
     * @return the outcome of the refresh attempt
     */
    public RefreshResult refreshSessionFromValidation(String username) {
        SessionData existingSession = sessions.get(username);
        if (existingSession == null) {
            return RefreshResult.NOT_FOUND;
        }

        Tenant tenant = existingSession.getTenant();
        String validationKey = existingSession.getValidationKey();
        boolean validationKeyPresent = validationKey != null && !validationKey.isBlank();

        if (tenant == null || !validationKeyPresent) {
            log.warn("[AUTH REFRESH] missing refresh context for username='{}' tenantPresent='{}' keyPresent='{}'",
                username, tenant != null, validationKeyPresent);
            sessions.remove(username);
            return RefreshResult.DENIED;
        }

        ValidationResult validationResult = validateAgainstCms(tenant, username, validationKey, "refresh");
        if (validationResult.status == ValidationStatus.SUCCESS) {
            SessionData refreshedSession = validationResult.sessionData;
            sessions.put(username, refreshedSession);
            log.info("[AUTH REFRESH] success username='{}' assignmentId='{}' uploadBucket='{}' uploadRegion='{}' uploadKeyPrefix='{}'",
                    username, refreshedSession.getAssignmentId(),
                    refreshedSession.getUploadCredentials().getBucket(),
                    refreshedSession.getUploadCredentials().getRegion(),
                    refreshedSession.getUploadCredentials().getKeyPrefix());
            return RefreshResult.REFRESHED;
        }

        if (validationResult.status == ValidationStatus.DENIED) {
            sessions.remove(username);
            log.warn("[AUTH REFRESH] denied username='{}' — evicting session", username);
            return RefreshResult.DENIED;
        }

        log.error("[AUTH REFRESH] failed username='{}' due to validation transport/server error", username);
        return RefreshResult.ERROR;
    }

    /**
     * Removes the session for the given username from the in-memory map.
     *
     * @param username the FTP username to evict
     * @param reason   a short description of why the session is being evicted (logged)
     */
    public void evictSession(String username, String reason) {
        sessions.remove(username);
        log.warn("[SESSION] evicted username='{}' reason='{}'", username, reason);
    }

    /**
     * Validates the given credentials against the CMS and returns the resulting
     * {@link SessionData} on success.
     *
     * <p>Unlike the FTP {@link #authenticate} path, this method does <strong>not</strong>
     * perform the master-password bypass — it always forwards the supplied password to the
     * CMS validation endpoint.
     *
     * @param tenant   the tenant whose CMS endpoint should be called
     * @param username the username (used for logging)
     * @param password the credential to validate
     * @return the {@link SessionData} returned by the CMS on success
     * @throws org.apache.ftpserver.ftplet.AuthenticationFailedException if the CMS rejects
     *         the credentials or the request fails
     */
    public SessionData validateForUpload(Tenant tenant, String username, String password)
            throws org.apache.ftpserver.ftplet.AuthenticationFailedException {
        ValidationResult result = validateAgainstCms(tenant, username, password, "upload");
        if (result.status == ValidationStatus.DENIED) {
            throw new org.apache.ftpserver.ftplet.AuthenticationFailedException(
                    "CMS rejected credentials for username: " + username);
        }
        if (result.status != ValidationStatus.SUCCESS) {
            throw new org.apache.ftpserver.ftplet.AuthenticationFailedException(
                    "CMS validation request failed for username: " + username);
        }
        return result.sessionData;
    }

    /**
     * Calls the tenant's CMS validation endpoint with the given password and
     * parses the response into a {@link ValidationResult}.
     *
     * <p>4xx responses are treated as credential denial; other HTTP or network
     * errors are treated as transient failures.
     *
     * @param tenant   the tenant whose endpoint should be called
     * @param username the FTP username (used for logging)
     * @param password the credential to validate (FTP password or validation key)
     * @param flow     a label for the call site ({@code "login"} or {@code "refresh"})
     * @return the validation result with status and optional session data
     */
    @SuppressWarnings("rawtypes")
    private ValidationResult validateAgainstCms(Tenant tenant, String username, String password, String flow) {
        HttpHeaders headers = new HttpHeaders();
        headers.set("X-Colombo-API-Key", tenant.getApiKey());

        Map<String, String> requestBody = Collections.singletonMap("key", password);
        HttpEntity<Map<String, String>> requestEntity = new HttpEntity<>(requestBody, headers);

        ResponseEntity<Map> response;
        try {
            response = restTemplate.exchange(
                    tenant.getValidationEndpoint(),
                    HttpMethod.POST,
                    requestEntity,
                    Map.class
            );
        } catch (HttpStatusCodeException ex) {
            if (ex.getStatusCode().is4xxClientError()) {
                log.warn("[AUTH {}] validation denied username='{}' status='{}' body='{}'",
                        flow.toUpperCase(), username, ex.getStatusCode(), ex.getResponseBodyAsString());
                return ValidationResult.denied();
            }
            log.error("[AUTH {}] validation request failed username='{}' endpoint='{}' status='{}'",
                    flow.toUpperCase(), username, tenant.getValidationEndpoint(), ex.getStatusCode(), ex);
            return ValidationResult.error();
        } catch (RuntimeException ex) {
            log.error("[AUTH {}] validation request failed username='{}' endpoint='{}'",
                    flow.toUpperCase(), username, tenant.getValidationEndpoint(), ex);
            return ValidationResult.error();
        }

        log.info("[AUTH {}] CMS responded for username='{}' status='{}'",
                flow.toUpperCase(), username, response.getStatusCode());

        if (!HttpStatus.OK.equals(response.getStatusCode())) {
            if (response.getStatusCode().is4xxClientError()) {
                log.warn("[AUTH {}] validation rejected for username='{}' status='{}' body='{}'",
                        flow.toUpperCase(), username, response.getStatusCode(), response.getBody());
                return ValidationResult.denied();
            }
            log.error("[AUTH {}] validation upstream non-OK for username='{}' status='{}' body='{}'",
                    flow.toUpperCase(), username, response.getStatusCode(), response.getBody());
            return ValidationResult.error();
        }

        String assignmentId = extractAssignmentId(response.getBody());
        SessionUploadCredentials uploadCredentials = extractUploadCredentials(response.getBody());
        boolean uploadCredentialsValid = uploadCredentials != null && uploadCredentials.isValid();

        if (assignmentId == null || assignmentId.isBlank()) {
            log.warn("[AUTH {}] validation response invalid for username='{}' assignmentId='{}' uploadCredentialsValid='{}'",
                flow.toUpperCase(), username, assignmentId, uploadCredentialsValid);
            return ValidationResult.denied();
        }

        if (!uploadCredentialsValid) {
            log.warn("[AUTH {}] validation response invalid for username='{}' assignmentId='{}' uploadCredentialsValid='{}'",
                flow.toUpperCase(), username, assignmentId, uploadCredentialsValid);
            return ValidationResult.denied();
        }

        return ValidationResult.success(new SessionData(tenant, assignmentId, uploadCredentials, password));
    }

    /**
     * Returns the first non-null, non-blank value from the given candidates.
     *
     * @param values the candidate strings in priority order
     * @return the first non-blank value, or {@code null} if all are blank
     */
    private static String firstNonBlank(String... values) {
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                return value;
            }
        }
        return null;
    }

    /**
     * Returns {@code true} when the supplied password should trigger master-password bypass.
     *
     * @param masterPassword configured or resolved master password value
     * @param password       credential supplied by the user
     * @return {@code true} if bypass should be applied
     */
    private boolean isMasterPasswordBypass(String masterPassword, String password) {
        return masterPassword != null && !masterPassword.isBlank() && masterPassword.equals(password);
    }

    /**
     * Outcome of a session credential refresh attempt.
     */
    public enum RefreshResult {
        /** 
         * Credentials were refreshed successfully.
         */
        REFRESHED,
        /** 
         * The CMS denied the refresh (invalid/revoked credentials).
         */
        DENIED,
        /** 
         * A transport or server error occurred during the refresh call.
         */
        ERROR,
        /** 
         * No in-memory session was found for the given username.
         */
        NOT_FOUND
    }

    /**
     * Internal status codes returned by the CMS validation helper.
     */
    private enum ValidationStatus {
        /**
         * The CMS accepted the credentials and returned upload metadata.
         */
        SUCCESS,
        /**
         * The CMS rejected the credentials (4xx response).
         */
        DENIED,
        /**
         * A network or server error prevented validation.
         */
        ERROR
    }

    /**
     * Encapsulates the outcome of a CMS validation call, pairing a
     * {@link ValidationStatus} with the optional {@link SessionData} produced on success.
     */
    private static final class ValidationResult {
        private final ValidationStatus status;
        private final SessionData sessionData;

        /**
         * Creates a new {@code ValidationResult}.
         *
         * @param status      the outcome status
         * @param sessionData the session data on success; {@code null} otherwise
         */
        private ValidationResult(ValidationStatus status, SessionData sessionData) {
            this.status = status;
            this.sessionData = sessionData;
        }

        /**
         * Creates a successful result with the given session data.
         *
         * @param sessionData the session data returned by the CMS
         * @return a successful {@code ValidationResult}
         */
        private static ValidationResult success(SessionData sessionData) {
            return new ValidationResult(ValidationStatus.SUCCESS, sessionData);
        }

        /**
         * Creates a denial result indicating rejected credentials.
         *
         * @return a denied {@code ValidationResult}
         */
        private static ValidationResult denied() {
            return new ValidationResult(ValidationStatus.DENIED, null);
        }

        /**
         * Creates an error result indicating a transport or server failure.
         *
         * @return an error {@code ValidationResult}
         */
        private static ValidationResult error() {
            return new ValidationResult(ValidationStatus.ERROR, null);
        }
    }

    /**
     * Apache FTP {@link BaseUser} adapter that maps a {@link Tenant} to an FTP user account.
     *
     * <p>The home directory is set to the JVM's temporary directory. Write and
     * unlimited concurrent login permissions are granted automatically.
     */
    private static final class ColomboUser extends BaseUser {

        /**
         * Creates a new {@code ColomboUser} from the given tenant.
         *
         * @param tenant the tenant whose FTP credentials should be applied
         */
        private ColomboUser(Tenant tenant) {
            setName(tenant.getFtpUsername());
            setHomeDirectory(System.getProperty("java.io.tmpdir"));
            setEnabled(true);
            setMaxIdleTime(300);
            setAuthorities(java.util.List.of(
                    new WritePermission(),
                    new ConcurrentLoginPermission(Integer.MAX_VALUE, Integer.MAX_VALUE)
            ));
        }
    }
}
