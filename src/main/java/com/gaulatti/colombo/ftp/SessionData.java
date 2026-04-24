package com.gaulatti.colombo.ftp;

import com.gaulatti.colombo.model.Tenant;
import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Holds the runtime state for an active FTP session.
 *
 * <p>Instances are stored in the shared session map keyed by FTP username and
 * carry the tenant context, the assignment being worked on, temporary AWS upload
 * credentials, and the validation key used to refresh those credentials.
 */
@Data
@AllArgsConstructor
public class SessionData {

    /**
     * The tenant associated with the session. 
     * This is used to determine the context in which the session operates, 
     * such as permissions and access rights.
     */
    private Tenant tenant;

    /**
     * The unique identifier for the assignment associated with the session. 
     * This is used to link the session to a specific task or job that the user is working on.
     */
    private String assignmentId;

    /**
     * The credentials required for uploading files during the session.
     */
    private SessionUploadCredentials uploadCredentials;

    /**
     * The validation key used to verify the integrity and authenticity of the session.
     */
    private String validationKey;
}
