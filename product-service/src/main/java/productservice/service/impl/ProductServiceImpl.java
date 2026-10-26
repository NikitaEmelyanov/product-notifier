package productservice.service.impl;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import productservice.dto.CreateProductDto;
import productservice.service.ProductService;
import productservice.service.event.ProductCreatedEvent;

/**
 * Имлементация интерфейса ProductService, содержит методы взаимодействия продукта через кафку
 */
@Service
public class ProductServiceImpl implements ProductService {

    //Создаем KafkaTemplate, где ключом будет String, а знаением ProductCreatedEvent
    // KafkaTemplate это Spring'овый враппер для Кафка продюсера
    private KafkaTemplate<String, ProductCreatedEvent> kafkaTemplate;
    private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

    public ProductServiceImpl(KafkaTemplate<String, ProductCreatedEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Метод позволяющий создать продукт асинхронно
     * @param createProductDto ДТО содержащее данные для создания продукта
     * @return productId
     */
    @Override
    public String createProductAsync(CreateProductDto createProductDto) {
        //TODO: save to DB
        //По идее мы бы сохранили в БД и получили бы id сгенреенную в БД для этого продукта, когда
        // новый продукт создается
        //Генерация случайного ключа
        String productId = UUID.randomUUID().toString();

        ProductCreatedEvent productCreatedEvent = new ProductCreatedEvent(
            productId,
            createProductDto.getTitle(),
            createProductDto.getPrice(),
            createProductDto.getQuantity());

        // Отправляем сообщения брокеру в асинхронном режиме, через KafkaTemplate
        // В конструктор для отправки передаем имя топика, ключ и евент
        // Для обработки ответа используем CompletableFuture c параметрами SendResult,
        // где в параметрах передаем String в качестве ключа и ProductCreatedEvent в качестве события
        CompletableFuture<SendResult<String, ProductCreatedEvent>> future = kafkaTemplate
            .send("product-created-events-topic", productId, productCreatedEvent);

        // whenComplete говорит о том что метод будет выполнен, когда придет ответ от брокера
        future.whenComplete((result, exception) -> {
            // Если ловим ошибку, то переменная exception будет не null
            if (exception != null) {
                // Обработка исключения
                LOGGER.error("Failed to send message: {}", exception.getMessage());
            }else {
                // Тут все отлично
                LOGGER.info("Message sent successfully {}", result.getRecordMetadata());
            }
        });

        // Проверка асинхронного режима, то есть сначала должен отработать этот логгер,
        // а потом уже должны отработать логгеры в колбеке
        LOGGER.info("Return: {}", productId);

        return productId;
    }

    /**
     * Метод, позволяющий создать продукт через кафку синхронно (ждем ответ)
     * @param createProductDto
     * @return productId
     */
    @Override
    public String createProductSync(CreateProductDto createProductDto)
        throws ExecutionException, InterruptedException {
        //TODO: save to DB
        String productId = UUID.randomUUID().toString();

        ProductCreatedEvent productCreatedEvent = new ProductCreatedEvent(productId,
            createProductDto.getTitle(),
            createProductDto.getPrice(),
            createProductDto.getQuantity());

        SendResult<String, ProductCreatedEvent> result = kafkaTemplate
            .send("product-created-events-topic", productId, productCreatedEvent).get();

        //Если бы работа была асинхронна, тут были бы пустые значения, так как мы бы
        // не получили информацию
        LOGGER.info("Topic: {}", result.getRecordMetadata().topic());
        LOGGER.info("Partirion: {}", result.getRecordMetadata().partition());
        LOGGER.info("Offset: {}", result.getRecordMetadata().offset());

        LOGGER.info("Return: {}", productId);

        return productId;
    }
}
