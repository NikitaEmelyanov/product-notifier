package ru.product.service.emailnotificationservice.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;
import ru.product.service.core.event.ProductCreatedEvent;
import ru.product.service.emailnotificationservice.entity.ProcessedEventEntity;
import ru.product.service.emailnotificationservice.exception.NonRetryableException;
import ru.product.service.emailnotificationservice.exception.RetryableException;
import ru.product.service.emailnotificationservice.repository.ProcessEventRepository;

/**
 * Класс для чтения и обработки эвентов. Является Kafka листенером, который может обрабатывать все
 * виды событий, связанных с топиком product-created-events-topic
 */
@Component
@KafkaListener(topics = "product-created-events-topic")
public class ProductCreatedEventHandler {

    private final Logger LOGGER = LoggerFactory.getLogger(ProductCreatedEventHandler.class);

    private RestTemplate restTemplate;
    private ProcessEventRepository processEventRepository;


    public ProductCreatedEventHandler(RestTemplate restTemplate,
        ProcessEventRepository processEventRepository) {
        this.restTemplate = restTemplate;
        this.processEventRepository = processEventRepository;
    }

    /**
     * Метод позволяющий мапить данные по типу входных аргументов
     *
     * @param productCreatedEvent событие, которое хотим считывать (будет находиться в теле)
     * @param messageId уникальное Id сообщения, которое мы хотим считывать из header.
     * @param messageKey поле, которое мы хотим считывать из header.
     *                   Данное поле мы отправляем как ProductId на стороне Producer
     */
    @Transactional
    @KafkaHandler
    public void handle(@Payload ProductCreatedEvent productCreatedEvent,
                       @Header("messageId") String messageId,
                       @Header(KafkaHeaders.RECEIVED_KEY) String messageKey) {

        //Логируем событие, при чтении из топика
        LOGGER.info("Received event: {}", productCreatedEvent.getTitle(), messageId);

        ProcessedEventEntity processedEventEntity = processEventRepository.findByMessageId(messageId);

        //Проверяем нашли мы с таким messageId или нет
        if (processedEventEntity != null) {
            LOGGER.info("Dublicate message id: {}", messageId);
            return;
        }

        // Эмулируем работу со вторым микросервисом
        // Будем отправлять сообщение по restTemplate, по некоторому url, методом GET,
        // будем отправлять null и возвращать он нам будет стирнг.
        // Сам урл будет представлять из себе некоторый мок-микросервис
        // Будем анализировать response, чтобы понять какая ошибка произошла и в зависимости от
        //типа ошибки будем пробрасывать RetryableException или NonRetryableException
        String url = "http://localhost:8090/response/200"; // Для теста можно поставить статус 500
        try {
            ResponseEntity<String> response = restTemplate.exchange(url, HttpMethod.GET, null,
                String.class);
            if (response.getStatusCode().value() == HttpStatus.OK.value()) {
                LOGGER.info("Received response: {}", response.getBody());
            }
        } catch (ResourceAccessException e) {
            LOGGER.error(e.getMessage());
            throw new RetryableException(e);
        } catch (HttpServerErrorException e) {
            LOGGER.error(e.getMessage());
            throw new NonRetryableException(e);
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
            throw new NonRetryableException(e);
        }

        // Сохранение в бд в успешном случае
        try {
            processEventRepository.save(new ProcessedEventEntity(messageId,
                productCreatedEvent.getProductId()));
        } catch (DataIntegrityViolationException e) {
            LOGGER.error(e.getMessage());
            throw new NonRetryableException(e);
        }
    }

}
