package com.gaulatti.colombo.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Data;

/**
 * Persistent entity representing a Colombo tenant.
 *
 * <p>Each tenant maps to a single FTP user account and owns the CMS endpoints
 * and API key needed to validate credentials and report uploaded photos.
 */
@Data
@Entity
@Table(name = "tenants")
public class Tenant {

    /** 
     * Auto-generated surrogate primary key.
     */
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    /** 
     * Human-readable display name that uniquely identifies the tenant.
     */
    @Column(nullable = false, unique = true)
    private String name;

    /** 
     * FTP username assigned to this tenant; used as the login credential.
     */
    @Column(name = "ftp_username", nullable = false, unique = true)
    private String ftpUsername;

    /** 
     * Secret API key sent in the {@code X-Colombo-API-Key} header on every CMS call.
     */
    @Column(name = "api_key", nullable = false)
    private String apiKey;

    /** 
     * URL of the CMS endpoint used to validate FTP passwords and obtain upload credentials.
     */
    @Column(name = "validation_endpoint", nullable = false)
    private String validationEndpoint;

    /** 
     * URL of the CMS endpoint called after a file has been successfully uploaded to S3.
     */
    @Column(name = "photo_endpoint", nullable = false)
    private String photoEndpoint;
}
