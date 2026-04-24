package com.gaulatti.colombo.config;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;

import org.junit.jupiter.api.Test;
import org.springframework.web.client.RestTemplate;

class AppConfigTest {

    private final AppConfig appConfig = new AppConfig();

    @Test
    void restTemplateBeanFactoryCreatesInstances() {
        RestTemplate first = appConfig.restTemplate();
        RestTemplate second = appConfig.restTemplate();

        assertNotNull(first);
        assertNotNull(second);
        assertNotSame(first, second);
    }
}
