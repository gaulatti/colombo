package com.gaulatti.colombo.config;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import com.gaulatti.colombo.ftp.ColomboFtplet;
import com.gaulatti.colombo.ftp.ColomboUserManager;
import com.gaulatti.colombo.ftp.SessionData;
import com.gaulatti.colombo.repository.TenantRepository;
import com.gaulatti.colombo.service.UploadService;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ftpserver.FtpServer;
import org.junit.jupiter.api.Test;
import org.springframework.web.client.RestTemplate;

class FtpServerConfigTest {

    @Test
    void beanFactoriesReturnConfiguredInstances() {
        TenantRepository tenantRepository = mock(TenantRepository.class);
        RestTemplate restTemplate = new RestTemplate();
        FtpServerConfig config = new FtpServerConfig(tenantRepository, restTemplate);

        ConcurrentHashMap<String, SessionData> sessions = config.sessions();
        ColomboUserManager userManager = config.colomboUserManager(sessions, "master");
        UploadService uploadService = new UploadService(sessions, restTemplate, userManager);
        ColomboFtplet ftplet = config.colomboFtplet(sessions, uploadService);

        assertNotNull(sessions);
        assertTrue(sessions.isEmpty());
        assertNotNull(userManager);
        assertNotNull(ftplet);
    }

    @Test
    void ftpServerStartsAndDestroyStopsIt() throws Exception {
        TenantRepository tenantRepository = mock(TenantRepository.class);
        RestTemplate restTemplate = new RestTemplate();
        FtpServerConfig config = new FtpServerConfig(tenantRepository, restTemplate);

        ConcurrentHashMap<String, SessionData> sessions = config.sessions();
        ColomboUserManager userManager = config.colomboUserManager(sessions, "master");
        UploadService uploadService = new UploadService(sessions, restTemplate, userManager);
        ColomboFtplet ftplet = config.colomboFtplet(sessions, uploadService);

        FtpServer started = config.ftpServer(userManager, ftplet, 0, "60000-60002", null);
        assertNotNull(started);
        assertSame(started, started);

        config.destroy();
        config.destroy();
        assertTrue(started.isStopped());
    }

    @Test
    void ftpServerStartsWithPassiveExternalAddressConfigured() throws Exception {
        TenantRepository tenantRepository = mock(TenantRepository.class);
        RestTemplate restTemplate = new RestTemplate();
        FtpServerConfig config = new FtpServerConfig(tenantRepository, restTemplate);

        ConcurrentHashMap<String, SessionData> sessions = config.sessions();
        ColomboUserManager userManager = config.colomboUserManager(sessions, "master");
        UploadService uploadService = new UploadService(sessions, restTemplate, userManager);
        ColomboFtplet ftplet = config.colomboFtplet(sessions, uploadService);

        FtpServer started = config.ftpServer(userManager, ftplet, 0, "60000-60002", "127.0.0.1");
        assertNotNull(started);
        assertSame(started, started);

        config.destroy();
        config.destroy();
        assertTrue(started.isStopped());
    }

    @Test
    void destroyIsNoopWhenServerNeverStarted() {
        FtpServerConfig config = new FtpServerConfig(mock(TenantRepository.class), new RestTemplate());
        config.destroy();
    }
}
