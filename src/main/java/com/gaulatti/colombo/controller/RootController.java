package com.gaulatti.colombo.controller;

import java.net.URI;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Minimal root endpoint that redirects to a public information page.
 */
@RestController
public class RootController {

    /**
     * Piazza Colombo, Sanremo.
     * Friday, February 27, 2026. 10:54PM Italian time.
     */
    private static final URI LANDING_URI = URI.create("https://www.youtube.com/watch?v=KieE_MLv-ZY");

    /**
     * Redirects callers to the public landing page.
     *
     * @return redirect response with {@code Location} header
     */
    @GetMapping("/")
    public ResponseEntity<Void> root() {
        return ResponseEntity.status(HttpStatus.FOUND)
                .header(HttpHeaders.LOCATION, LANDING_URI.toString())
                .build();
    }
}
