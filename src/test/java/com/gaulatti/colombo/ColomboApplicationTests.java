package com.gaulatti.colombo;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.servlet.autoconfigure.MultipartProperties;
import org.springframework.util.unit.DataSize;

@SpringBootTest
class ColomboApplicationTests {

    @Autowired
    private MultipartProperties multipartProperties;

    @Test
    void contextLoads() {
    }

    @Test
    void multipartUploadLimitIsOneHundredMegabytes() {
        assertEquals(DataSize.ofMegabytes(100), multipartProperties.getMaxFileSize());
        assertEquals(DataSize.ofMegabytes(100), multipartProperties.getMaxRequestSize());
    }

}
