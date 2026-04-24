package com.gaulatti.colombo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Main entry point for the Colombo FTP server application.
 *
 * <p>Colombo is a Spring Boot application that provides a multi-tenant FTP server
 * backed by CMS-driven authentication and AWS S3 upload capabilities.
 */
@SpringBootApplication
public class ColomboApplication {

    /**
     * Starts the Spring Boot application.
     *
     * @param args command-line arguments passed to the application
     */
    public static void main(String[] args) {
        SpringApplication.run(ColomboApplication.class, args);
    }

}
