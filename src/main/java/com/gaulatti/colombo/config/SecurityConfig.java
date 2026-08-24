package com.gaulatti.colombo.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.access.intercept.AuthorizationFilter;

/**
 * Spring Security configuration for Colombo.
 *
 * <p>The {@code /upload} endpoint uses its own header-based credential validation
 * (delegating to the CMS via {@link com.gaulatti.colombo.ftp.ColomboUserManager}),
 * so it is permitted without Spring Security authentication.
 *
 * <p>The health actuator is public for infrastructure probes. The Prometheus actuator
 * is protected by a dedicated bearer credential in {@link MetricsTokenFilter}.
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

    private final MetricsTokenFilter metricsTokenFilter;

    public SecurityConfig(MetricsTokenFilter metricsTokenFilter) {
        this.metricsTokenFilter = metricsTokenFilter;
    }

    /**
     * Configures the security filter chain.
     *
     * <ul>
    *   <li>{@code GET /} — permitted without authentication; useful for a basic
    *       uptime probe and quick manual verification.</li>
     *   <li>{@code POST /upload} — permitted without Spring Security authentication;
     *       the controller performs its own CMS credential validation.</li>
     *   <li>{@code /actuator/health} — public for infrastructure health probes.</li>
     *   <li>{@code /actuator/prometheus} — permitted only after the metrics-token
     *       filter accepts its dedicated bearer credential.</li>
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
            .addFilterBefore(metricsTokenFilter, AuthorizationFilter.class)
            .authorizeHttpRequests(auth -> auth
                .requestMatchers("/").permitAll()
                .requestMatchers("/upload").permitAll()
                .requestMatchers("/actuator/health").permitAll()
                .requestMatchers("/actuator/prometheus").permitAll()
                .anyRequest().authenticated()
            );
        return http.build();
    }
}
