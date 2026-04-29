package com.gaulatti.colombo.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.web.SecurityFilterChain;

/**
 * Spring Security configuration for Colombo.
 *
 * <p>The {@code /upload} endpoint uses its own header-based credential validation
 * (delegating to the CMS via {@link com.gaulatti.colombo.ftp.ColomboUserManager}),
 * so it is permitted without Spring Security authentication. All other endpoints
 * (e.g., actuator) remain protected.
 *
 * <p>CSRF protection is disabled for the {@code /upload} path because the endpoint is
 * a stateless REST API consumed by mobile clients that do not maintain session cookies.
 */
@Configuration
public class SecurityConfig {

    /**
     * Configures the security filter chain.
     *
     * <ul>
     *   <li>{@code POST /upload} — permitted without authentication; CSRF disabled globally
     *       (the endpoint does not use cookies or browser sessions).</li>
     *   <li>All other requests — require authentication via the default Spring Security
     *       mechanism (HTTP Basic or form login).</li>
     * </ul>
     *
     * @param http the {@link HttpSecurity} to configure
     * @return the configured {@link SecurityFilterChain}
     * @throws Exception if configuration fails
     */
    @Bean
    public SecurityFilterChain securityFilterChain(HttpSecurity http) throws Exception {
        http
            .csrf(AbstractHttpConfigurer::disable)
            .authorizeHttpRequests(auth -> auth
                .requestMatchers("/upload").permitAll()
                .anyRequest().permitAll()
            );
        return http.build();
    }
}
