package com.gaulatti.colombo.config;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

/** Enforces a dedicated bearer credential on the Prometheus scrape endpoint. */
@Component
public class MetricsTokenFilter extends OncePerRequestFilter {

    private static final String METRICS_PATH = "/actuator/prometheus";
    private final byte[] token;

    public MetricsTokenFilter(@Value("${colombo.metrics.token:}") String configuredToken) {
        String normalized = configuredToken == null ? "" : configuredToken.trim();
        if (normalized.length() < 16) {
            throw new IllegalStateException("COLOMBO_METRICS_TOKEN must be configured with at least 16 characters");
        }
        this.token = normalized.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        return !METRICS_PATH.equals(request.getRequestURI());
    }

    @Override
    protected void doFilterInternal(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain filterChain
    ) throws ServletException, IOException {
        String authorization = request.getHeader("Authorization");
        byte[] candidate = authorization != null && authorization.startsWith("Bearer ")
                ? authorization.substring("Bearer ".length()).getBytes(StandardCharsets.UTF_8)
                : new byte[0];
        if (!MessageDigest.isEqual(token, candidate)) {
            response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
            return;
        }
        filterChain.doFilter(request, response);
    }
}
