package com.zakaria.eventflow;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class EventFlowApplication {
    public static void main(String[] args) {
        SpringApplication.run(EventFlowApplication.class, args);
    }
}
