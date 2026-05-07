package com.gaulatti.colombo.config;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
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

    @Test
    void s3UploadExecutorBeanFactoryCreatesConfiguredExecutor() {
        ThreadPoolTaskExecutor executor = appConfig.s3UploadExecutor();

        assertNotNull(executor);
        assertTrue(executor.getThreadNamePrefix().startsWith("colombo-s3-upload-"));
        assertTrue(executor.getCorePoolSize() == executor.getMaxPoolSize());
        assertTrue(executor.getThreadPoolExecutor().getQueue().remainingCapacity() > 1000);
        executor.shutdown();
    }

    @Test
    void cmsCallbackExecutorBeanFactoryCreatesConfiguredExecutor() {
        ThreadPoolTaskExecutor executor = appConfig.cmsCallbackExecutor();

        assertNotNull(executor);
        assertTrue(executor.getThreadNamePrefix().startsWith("colombo-cms-callback-"));
        assertTrue(executor.getCorePoolSize() == executor.getMaxPoolSize());
        assertTrue(executor.getThreadPoolExecutor().getQueue().remainingCapacity() > 1000);
        executor.shutdown();
    }
}
