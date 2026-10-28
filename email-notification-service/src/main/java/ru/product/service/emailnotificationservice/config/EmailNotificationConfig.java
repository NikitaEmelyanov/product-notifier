package ru.product.service.emailnotificationservice.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestTemplate;

/**
 * Конфигурация для Email Notification, позволяющая работать с RestTemplate
 */
@Configuration
public class EmailNotificationConfig {

    /**
     * Метод возвращающий RestTemplate
     */
    @Bean
    RestTemplate getRestTemplate() {
        return new RestTemplate();
    }

}
