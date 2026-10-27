package ru.product.service.emailnotificationservice.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import ru.product.service.core.event.ProductCreatedEvent;

/**
 * Класс для чтения и обработки эвентов. Является Kafka листенером, который может обрабатывать
 * все виды событий, связанных с топиком product-created-events-topic
 */
@Component
@KafkaListener(topics = "product-created-events-topic")
public class ProductCreatedEventHandler {

    private final Logger LOGGER = LoggerFactory.getLogger(ProductCreatedEventHandler.class);

    /**
     * Метод позволяющий мапить данные по типу входных аргументов
     * @param productCreatedEvent
     */
    @KafkaHandler
    public void handle(ProductCreatedEvent productCreatedEvent) {
        //Логируем событие, при чтении из топика
        LOGGER.info("Received event: {}", productCreatedEvent.getTitle());

    }

}
