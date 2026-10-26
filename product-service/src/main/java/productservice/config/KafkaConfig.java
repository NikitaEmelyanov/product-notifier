package productservice.config;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import productservice.service.event.ProductCreatedEvent;

/**
 * Конфигурация для Кафки
 */
@Configuration
public class KafkaConfig {

    //TODO: Можно сделать покрасивее через env бин

    @Value("${spring.kafka.producer.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.producer.key-serializer}")
    private String keySerializer;

    @Value("${spring.kafka.producer.value-serializer}")
    private String valueSerializer;

    @Value("${spring.kafka.producer.acks}")
    private String acks;

    @Value("${spring.kafka.producer.properties.delivery.timeout.ms}")
    private String deliveryTimeout;

    @Value("${spring.kafka.producer.properties.linger.ms}")
    private String linger;

    @Value("${spring.kafka.producer.properties.request.timeout.ms}")
    private String requestTimeout;

    @Value("${spring.kafka.producer.properties.enable.idempotence}")
    private String idemopotence;

    @Value("${spring.kafka.producer.properties.max.in.flight.requests.per.connection}")
    private String maxInFlightRequestsPerConnection;


    // Составляем мапу проперти для кафки, где в качестве ключа будет константа из ProducerConfig,
    // а в качестве значения наш заинжекченый параметр из проперти
    Map<String, Object> producerConfigs() {
        Map<String, Object> config = new HashMap<>();

        // В ProducerConfig занесены все проперти кафка в константы,
        // это намного удобнее application.properties
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer);
        config.put(ProducerConfig.ACKS_CONFIG, acks);
        config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, deliveryTimeout);
        config.put(ProducerConfig.LINGER_MS_CONFIG, linger);
        config.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeout);
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, idemopotence);
        config.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, maxInFlightRequestsPerConnection);

        return config;
    }

    /**
     * Фабрика, которая выдает конфиги на основе карты кафка проперти
     */
    @Bean
    ProducerFactory<String, ProductCreatedEvent> producerFactory() {
        return new DefaultKafkaProducerFactory<>(producerConfigs());
    }

    /**
     * Создает новый KafkaTemplate на основе фабрики выдающей конфиги. Теперь Spring будет
     * инжектить этот бин в сервесном слое
     * @return KafkaTemplate на основе фабрики выдающей конфиги
     */
    @Bean
    KafkaTemplate<String, ProductCreatedEvent> kafkaTemplate() {
        return new KafkaTemplate<String, ProductCreatedEvent>(producerFactory());
    }

    /**
     * Бин, который создает топик в кафка при запуске контекста. Топик будет иметь 3 партиции,
     * 3 реплики и минимальное количество синхронных реплик с лидером 2
     * @return
     */
    @Bean
    NewTopic createTopic() {
        return TopicBuilder.name("product-created-events-topic")
            .partitions(3)                          // Количество партиций
            .replicas(3)                             // Количество реплик
            .configs(Map.of("min.insync.replicas", "2")) // Минимальное количество синхронизированных серверов
            .build();
    }

}
