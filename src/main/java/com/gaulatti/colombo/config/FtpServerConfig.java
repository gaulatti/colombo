package com.gaulatti.colombo.config;

import com.gaulatti.colombo.ftp.ColomboFtplet;
import com.gaulatti.colombo.ftp.ColomboUserManager;
import com.gaulatti.colombo.ftp.SessionData;
import com.gaulatti.colombo.repository.TenantRepository;
import com.gaulatti.colombo.service.UploadService;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ftpserver.ConnectionConfigFactory;
import org.apache.ftpserver.DataConnectionConfigurationFactory;
import org.apache.ftpserver.FtpServer;
import org.apache.ftpserver.FtpServerFactory;
import org.apache.ftpserver.ftplet.FtpException;
import org.apache.ftpserver.listener.ListenerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestTemplate;

/**
 * Spring configuration class that bootstraps and manages the embedded Apache FTP server.
 *
 * <p>Wires together the {@link ColomboUserManager}, {@link ColomboFtplet}, session map,
 * and listener configuration to produce a fully running {@link FtpServer} bean.
 * Implements {@link DisposableBean} to ensure the server is stopped gracefully on shutdown.
 */
@Configuration
public class FtpServerConfig implements DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(FtpServerConfig.class);

    /** 
     * Repository used to look up tenant configuration.
     */
    private final TenantRepository tenantRepository;

    /** 
     * HTTP client for outbound calls to the CMS.
     */
    private final RestTemplate restTemplate;

    /** 
     * Reference to the running FTP server, kept for lifecycle management.
     */
    private FtpServer ftpServer;

    /**
     * Creates a new {@code FtpServerConfig} with the required dependencies.
     *
     * @param tenantRepository repository for resolving tenant records
     * @param restTemplate     HTTP client used by child beans
     */
    public FtpServerConfig(
            TenantRepository tenantRepository,
            RestTemplate restTemplate
    ) {
        this.tenantRepository = tenantRepository;
        this.restTemplate = restTemplate;
    }

    /**
     * Creates the shared concurrent session map that tracks active FTP sessions
     * keyed by FTP username.
     *
     * @return an empty {@link ConcurrentHashMap} to hold active {@link SessionData} entries
     */
    @Bean
    public ConcurrentHashMap<String, SessionData> sessions() {
        return new ConcurrentHashMap<>();
    }

    /**
     * Creates and configures the {@link ColomboUserManager} bean responsible for
     * authenticating FTP users against the CMS and managing in-memory session state.
     *
     * @param sessions                 the shared session map
     * @param configuredMasterPassword optional master password for support bypass
     *                                 (resolved from {@code COLOMBO_MASTER_PASSWORD} or
     *                                 {@code COLOMBO_DEV_PASSWORD} environment variables)
     * @return a configured {@link ColomboUserManager}
     */
    @Bean
    public ColomboUserManager colomboUserManager(
            ConcurrentHashMap<String, SessionData> sessions,
            @Value("${COLOMBO_MASTER_PASSWORD:${COLOMBO_DEV_PASSWORD:}}") String configuredMasterPassword
    ) {
        return new ColomboUserManager(tenantRepository, restTemplate, sessions, configuredMasterPassword);
    }

    /**
     * Creates the {@link ColomboFtplet} bean that intercepts FTP lifecycle events
     * and coordinates file-upload processing.
     *
     * @param sessions      the shared session map
     * @param uploadService the upload service for S3 and CMS photo-callback operations
     * @return a configured {@link ColomboFtplet}
     */
    @Bean
    public ColomboFtplet colomboFtplet(
            ConcurrentHashMap<String, SessionData> sessions,
            UploadService uploadService
    ) {
        return new ColomboFtplet(sessions, uploadService);
    }

    /**
     * Builds, configures, and starts the embedded Apache FTP server.
     *
     * <p>Passive-mode data ports are controlled by {@code colombo.ftp.passive-ports}
     * (default: {@code 60000-60100}). Narrow this range to reduce the number of ports
     * you need to forward on a NAT router — one port per expected concurrent upload is
     * sufficient. Anonymous logins are disabled. The server begins listening on
     * {@code ftpPort} immediately after this bean is created.
     *
     * @param colomboUserManager the user manager bean
     * @param colomboFtplet      the FTP lifecycle interceptor bean
     * @param ftpPort            the port the server will listen on (default: {@code 21})
     * @param passivePorts       the passive port range expression (e.g. {@code 60000-60010})
     * @return the started {@link FtpServer} instance
     * @throws FtpException if the server fails to start
     */
    @Bean
    @ConditionalOnProperty(name = "colombo.ftp.enabled", havingValue = "true", matchIfMissing = true)
    public FtpServer ftpServer(
            ColomboUserManager colomboUserManager,
            ColomboFtplet colomboFtplet,
            @Value("${colombo.ftp.port:2121}") int ftpPort,
            @Value("${colombo.ftp.passive-ports:60000-60100}") String passivePorts
    ) throws FtpException {
        FtpServerFactory factory = new FtpServerFactory();
        factory.setUserManager(colomboUserManager);
        factory.setFtplets(new HashMap<>(Map.of("colombo", colomboFtplet)));

        ConnectionConfigFactory connectionConfigFactory = new ConnectionConfigFactory();
        connectionConfigFactory.setAnonymousLoginEnabled(false);
        connectionConfigFactory.setMaxLogins(Integer.MAX_VALUE);
        connectionConfigFactory.setMaxAnonymousLogins(Integer.MAX_VALUE);
        connectionConfigFactory.setMaxLoginFailures(0);
        factory.setConnectionConfig(connectionConfigFactory.createConnectionConfig());

        DataConnectionConfigurationFactory dataConnectionConfigurationFactory = new DataConnectionConfigurationFactory();
        dataConnectionConfigurationFactory.setPassivePorts(passivePorts);

        ListenerFactory listenerFactory = new ListenerFactory();
        listenerFactory.setPort(ftpPort);
        listenerFactory.setDataConnectionConfiguration(
                dataConnectionConfigurationFactory.createDataConnectionConfiguration()
        );

        org.apache.ftpserver.listener.Listener listener = listenerFactory.createListener();

        factory.addListener("default", listener);

        FtpServer server = factory.createServer();
        server.start();
        this.ftpServer = server;
        log.info("[FTP STARTUP] ftpHomeRoot='{}' ftpPort='{}' passivePorts='{}'",
                System.getProperty("java.io.tmpdir"), ftpPort, passivePorts);

        return server;
    }

    /**
     * Stops the embedded FTP server when the Spring application context is closed.
     */
    @Override
    public void destroy() {
        if (ftpServer != null && !ftpServer.isStopped()) {
            ftpServer.stop();
        }
    }
}
