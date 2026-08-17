package com.gaulatti.colombo.service;

import static com.gaulatti.colombo.TestFixtures.tenant;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.gaulatti.colombo.ftp.ColomboUserManager;
import com.gaulatti.colombo.ftp.SessionData;
import com.gaulatti.colombo.ftp.SessionUploadCredentials;
import com.gaulatti.colombo.ftp.UploadNamingPolicy;
import com.gaulatti.colombo.ftp.UploadNamingSegment;
import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

class UploadServiceNamingTest {
    private RestTemplate restTemplate;
    private UploadNameRenderer renderer;
    private CaptureTimeReader captureTimeReader;
    private UploadService service;
    private SessionData session;
    private File file;

    @BeforeEach
    void setUp() {
        restTemplate = mock(RestTemplate.class);
        renderer = mock(UploadNameRenderer.class);
        captureTimeReader = mock(CaptureTimeReader.class);
        service = new UploadService(new ConcurrentHashMap<>(), restTemplate, mock(ColomboUserManager.class),
                Runnable::run, Runnable::run, renderer, captureTimeReader);
        UploadNamingPolicy policy = new UploadNamingPolicy(1, "desk", List.of(),
                List.of(new UploadNamingSegment("placeholder", null, "sequence", null, 3)),
                "UTC", "uploadedTime", "preserve");
        SessionUploadCredentials credentials = new SessionUploadCredentials(
                "a", "b", "c", "us-east-1", "bucket", "prefix", "expiry",
                policy, "/api/colombo/sequence");
        session = new SessionData(tenant(), "assignment", credentials, "key");
        file = new File("photo.jpg");
    }

    @Test
    void resolvesNumberAndStringSequencesThroughCms() throws Exception {
        Instant captured = Instant.parse("2026-08-17T12:00:00Z");
        when(captureTimeReader.read(file)).thenReturn(captured);
        when(renderer.render(any(), any(), any(), any(), any(), anyLong())).thenReturn("target.jpg");
        when(restTemplate.exchange(any(String.class), any(), any(), any(Class.class)))
                .thenReturn(ResponseEntity.ok(Map.of("sequence", 42)))
                .thenReturn(ResponseEntity.ok(Map.of("sequence", "43")));

        assertEquals("target.jpg", invokeResolve(session));
        assertEquals("target.jpg", invokeResolve(session));
        verify(captureTimeReader, org.mockito.Mockito.times(2)).read(file);
    }

    @Test
    void rejectsInvalidSequenceResponsesAndMissingEndpoint() throws Exception {
        when(restTemplate.exchange(any(String.class), any(), any(), any(Class.class)))
                .thenReturn(ResponseEntity.ok(null))
                .thenReturn(ResponseEntity.ok(Map.of("sequence", true)))
                .thenReturn(ResponseEntity.ok(Map.of("sequence", 0)));
        assertThrows(InvocationTargetException.class, () -> invokeResolve(session));
        assertThrows(InvocationTargetException.class, () -> invokeResolve(session));
        assertThrows(InvocationTargetException.class, () -> invokeResolve(session));

        Method endpoint = UploadService.class.getDeclaredMethod("resolveCmsEndpoint", String.class, String.class);
        endpoint.setAccessible(true);
        assertThrows(InvocationTargetException.class,
                () -> endpoint.invoke(service, "https://cms.example.com/validate", null));
        assertThrows(InvocationTargetException.class,
                () -> endpoint.invoke(service, "https://cms.example.com/validate", " "));
    }

    @Test
    void legacyPolicyAndUploadResultHelpersCoverCompatibilityBranches() throws Exception {
        session.getUploadCredentials().setNamingPolicy(null);
        assertEquals("photo.jpg", invokeResolve(session));

        UploadService.UploadResult withUrl = UploadService.UploadResult.success(session, "s3://bucket/path/file.jpg");
        assertEquals("file.jpg", withUrl.originalFilename());
        UploadService.UploadResult withoutUrl = UploadService.UploadResult.success(session, null);
        assertEquals(null, withoutUrl.targetFilename());
    }

    private String invokeResolve(SessionData value) throws Exception {
        Method method = UploadService.class.getDeclaredMethod(
                "resolveTargetFilename", SessionData.class, String.class, File.class);
        method.setAccessible(true);
        return (String) method.invoke(service, value, "photo.jpg", file);
    }
}
