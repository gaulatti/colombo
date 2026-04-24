package com.gaulatti.colombo;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mockStatic;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.springframework.boot.SpringApplication;

class ColomboApplicationMainTest {

    @Test
    void mainDelegatesToSpringApplicationRun() {
        String[] args = {"--x=y"};

        try (MockedStatic<SpringApplication> springApp = mockStatic(SpringApplication.class)) {
            ColomboApplication.main(args);
            springApp.verify(() -> SpringApplication.run(eq(ColomboApplication.class), eq(args)));
        }
    }
}
