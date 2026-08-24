package com.gaulatti.colombo.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockFilterChain;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

class MetricsTokenFilterTest {

    private static final String TOKEN = "metrics-token-123456";

    @Test
    void requiresAConfiguredNontrivialToken() {
        assertThrows(IllegalStateException.class, () -> new MetricsTokenFilter(null));
        assertThrows(IllegalStateException.class, () -> new MetricsTokenFilter(" short "));
    }

    @Test
    void ignoresNonMetricsRequests() throws Exception {
        MetricsTokenFilter filter = new MetricsTokenFilter(TOKEN);
        MockHttpServletRequest request = new MockHttpServletRequest("GET", "/actuator/health");
        MockHttpServletResponse response = new MockHttpServletResponse();
        MockFilterChain chain = new MockFilterChain();

        filter.doFilter(request, response, chain);

        assertEquals(200, response.getStatus());
        assertEquals(request, chain.getRequest());
    }

    @Test
    void rejectsMissingMalformedAndIncorrectCredentials() throws Exception {
        MetricsTokenFilter filter = new MetricsTokenFilter(TOKEN);
        for (String authorization : new String[] {null, "Basic abc", "Bearer wrong"}) {
            MockHttpServletRequest request = new MockHttpServletRequest("GET", "/actuator/prometheus");
            if (authorization != null) {
                request.addHeader("Authorization", authorization);
            }
            MockHttpServletResponse response = new MockHttpServletResponse();
            filter.doFilter(request, response, new MockFilterChain());
            assertEquals(401, response.getStatus());
        }
    }

    @Test
    void acceptsTheDedicatedBearerCredential() throws Exception {
        MetricsTokenFilter filter = new MetricsTokenFilter("  " + TOKEN + "  ");
        MockHttpServletRequest request = new MockHttpServletRequest("GET", "/actuator/prometheus");
        request.addHeader("Authorization", "Bearer " + TOKEN);
        MockHttpServletResponse response = new MockHttpServletResponse();
        MockFilterChain chain = new MockFilterChain();

        filter.doFilter(request, response, chain);

        assertEquals(request, chain.getRequest());
    }
}
