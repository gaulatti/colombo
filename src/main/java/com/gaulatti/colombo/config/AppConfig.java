package com.gaulatti.colombo.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestTemplate;

/**
 * General Spring application configuration.
 *
 * <p>Defines shared infrastructure beans used across the application,
 * such as the {@link RestTemplate} for outbound HTTP calls.
 */
@Configuration
public class AppConfig {

    /**
     * Creates a {@link RestTemplate} bean for making synchronous HTTP requests
     * to external services such as the CMS validation and photo callback endpoints.
     *
     * @return a default {@link RestTemplate} instance
     */
    @Bean
    public RestTemplate restTemplate() {
        return new RestTemplate();
    }
}
