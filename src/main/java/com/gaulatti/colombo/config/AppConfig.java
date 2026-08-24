package com.gaulatti.colombo.config;

import com.gaulatti.colombo.observability.ColomboMetrics;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
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

    /**
     * Creates the executor used for outbound S3 uploads after Colombo has
     * accepted the inbound FTP/HTTP upload.
     *
     * @return an unbounded-queue executor for background S3 upload processing
     */
    @Bean
    public ThreadPoolTaskExecutor s3UploadExecutor(ColomboMetrics metrics) {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setThreadNamePrefix("colombo-s3-upload-");
        executor.setCorePoolSize(8);
        executor.setMaxPoolSize(8);
        executor.setQueueCapacity(Integer.MAX_VALUE);
        executor.initialize();
        metrics.registerExecutor("s3_upload", executor.getThreadPoolExecutor());
        return executor;
    }

    public ThreadPoolTaskExecutor s3UploadExecutor() {
        return s3UploadExecutor(ColomboMetrics.noop());
    }

    /**
     * Creates the executor used for CMS photo callbacks after S3 upload succeeds.
     *
     * @return an unbounded-queue executor for CMS callback processing
     */
    @Bean
    public ThreadPoolTaskExecutor cmsCallbackExecutor(ColomboMetrics metrics) {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setThreadNamePrefix("colombo-cms-callback-");
        executor.setCorePoolSize(4);
        executor.setMaxPoolSize(4);
        executor.setQueueCapacity(Integer.MAX_VALUE);
        executor.initialize();
        metrics.registerExecutor("cms_callback", executor.getThreadPoolExecutor());
        return executor;
    }

    public ThreadPoolTaskExecutor cmsCallbackExecutor() {
        return cmsCallbackExecutor(ColomboMetrics.noop());
    }
}
