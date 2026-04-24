package com.gaulatti.colombo.ftp;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Temporary AWS credentials and upload target configuration obtained from the CMS
 * during FTP authentication or session refresh.
 *
 * <p>All fields must be non-blank for a session to be considered ready to upload.
 * Use {@link #isValid()} to verify completeness before attempting an S3 transfer.
 */
@Data
@AllArgsConstructor
public class SessionUploadCredentials {

    /** 
     * AWS access key ID for the temporary session credentials.
     */
    private String accessKeyId;

    /** 
     * AWS secret access key paired with {@link #accessKeyId}.
     */
    private String secretAccessKey;

    /** 
     * Temporary session token that authorises the STS credentials.
     */
    private String sessionToken;

    /** 
     * AWS region in which the target S3 bucket resides.
     */
    private String region;

    /** 
     * Name of the S3 bucket that uploaded files should be written to.
     */
    private String bucket;

    /** 
     * Key prefix (folder path) under which uploaded objects are stored.
     */
    private String keyPrefix;

    /** 
     * ISO-8601 timestamp at which the temporary credentials expire.
     */
    private String expiresAt;

    /**
     * Returns {@code true} if all credential and destination fields are present and non-blank,
     * indicating that this object is safe to use for an S3 upload.
     *
     * @return {@code true} when all required fields are populated
     */
    public boolean isValid() {
        return isNonBlank(accessKeyId)
                && isNonBlank(secretAccessKey)
                && isNonBlank(sessionToken)
                && isNonBlank(region)
                && isNonBlank(bucket)
                && isNonBlank(keyPrefix)
                && isNonBlank(expiresAt);
    }

    /**
     * Returns {@code true} if {@code value} is neither {@code null} nor blank.
     *
     * @param value the string to test
     * @return {@code true} when the value is present
     */
    private boolean isNonBlank(String value) {
        return value != null && !value.isBlank();
    }
}
