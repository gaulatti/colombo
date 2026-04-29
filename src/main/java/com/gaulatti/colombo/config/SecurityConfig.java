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
 * so it is permitted without Spring Security authentication.
 *
 * <p>Actuator endpoints are also permitted to support infrastructure monitoring
 * (health checks, metrics) which is typically protected at the network/proxy layer
 * rather than at the application level.
 *
 * <h3>CSRF</h3>
 * CSRF protection is disabled for the entire application because:
 * <ul>
 *   <li>The {@code /upload} endpoint is a stateless REST API consumed by mobile clients
 *       that do not use browser session cookies; CSRF attacks against such endpoints
 *       are not feasible.</li>
 *   <li>All other endpoints are either read-only (actuator) or not browser-facing.</li>
 * </ul>
 */
@Configuration
public class SecurityConfig {

    /**
     * Configures the security filter chain.
     *
     * <ul>
     *   <li>{@code POST /upload} — permitted without Spring Security authentication;
     *       the controller performs its own CMS credential validation.</li>
     *   <li>Actuator endpoints ({@code /actuator/**}) — permitted for infrastructure
     *       monitoring; protect at the network layer (e.g., nginx ACL or ALB security
     *       group) in production.</li>
     *   <li>All other requests — require authentication (Spring Security default).</li>
     * </ul>
     *
     * @param http the {@link HttpSecurity} to configure
     * @return the configured {@link SecurityFilterChain}
     * @throws Exception if configuration fails
     */
    @Bean
    public SecurityFilterChain securityFilterChain(HttpSecurity http) throws Exception {
        http
            // CSRF is disabled: the /upload endpoint is a stateless REST API that
            // authenticates via custom request headers, not cookies.  No other
            // endpoint accepts state-mutating browser-initiated requests that would
            // be vulnerable to CSRF.
            .csrf(AbstractHttpConfigurer::disable)
            .authorizeHttpRequests(auth -> auth
                .requestMatchers("/upload").permitAll()
                .requestMatchers("/actuator/**").permitAll()
                .anyRequest().authenticated()
            );
        return http.build();
    }
}
