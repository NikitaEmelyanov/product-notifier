package ru.product.service.emailnotificationservice.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;

/**
 * Конфиг для настройки Kafka-Consumer (email-notification-service)
 */
@Configuration
public class KafkaConfig {

    // В продюсере мы получали свойства через @Value и сеттили их по одному, здесь мы сделаем
    // через Environment bean. Это бин, который предоставляет нам Spring и в нем уже есть
    // все проперти заинжекчены из консумера
    @Autowired
    Environment environment;

    /**
     * Метод возвращающий ConsumerFactory, которая будет использоваться при создании
     * листенеров
     */
    @Bean
    ConsumerFactory<String, Object> consumerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
            environment.getProperty("spring.kafka.consumer.bootstrap-servers"));
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        // ErrorHandlingDeserializer - это класс враппер вокруг сериалайзеров, который
        // позволяет скипнуть сообщение, которые пришло с ексепшеном, то есть если
        // сообщение не прочитано указатель будет переведен на следующий оффсет в партиции
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        // И обзязательно нужно указать, чем десерелизуем в случае успеха
        config.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);
        config.put(JsonDeserializer.TRUSTED_PACKAGES,
            environment.getProperty("spring.kafka.consumer.properties.spring.json.trusted.packages"));
        config.put(ConsumerConfig.GROUP_ID_CONFIG,
            environment.getProperty("spring.kafka.consumer.group-id"));

        return new DefaultKafkaConsumerFactory<>(config);
    }

    /**
     * Метод, который будет добавлять созданную фабрику с настройками при создании листенера.
     * На вход будет ожидать ConsumerFactory<String, Object>
     * @return Фабрика Листенеров, с применением фабрики по настройке консумера
     */
    @Bean
    ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(
             ConsumerFactory<String, Object> consumerFactory,
                                     KafkaTemplate<String, Object> kafkaTemplate) {

        // Кофигарация ErrorHandler для проблемных сообщений. В конструктор нужно
        // передать кто будет слать в новый топик (в Spring KafkaTemplate отправляет,
        // будем инжектить KafkaTemplate при создании этого бина)
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
            new DeadLetterPublishingRecoverer(kafkaTemplate));
        // Стратегия DeadLetterPublishingRecoverer будет создавать новый топик и отправлять
        // туда проблемные сообщения, для того чтобы отсылать ему нужен KafkaTemplate

        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
            new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);

        // Указываем поведение консумера, когда он встречается с сообщениями,
        // которые не может прочитать
        factory.setCommonErrorHandler(errorHandler);
        return factory;
    }

    /**
     * Метод, который будет возвращать KafkaTemplate, для корректной работы Spring.
     * Ожидает фабрику, сконфигурурованную с помощью проперти
     */
    @Bean
    KafkaTemplate<String, Object> kafkaTemplate(ProducerFactory<String, Object> producerFactoryy) {
        return new KafkaTemplate<>(producerFactoryy);
    }

    /**
     * Конфигурация ProducerFactory для передачи ее в KafkaTemplate
     */
    @Bean
    ProducerFactory<String, Object> producerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
            environment.getProperty("spring.kafka.consumer.bootstrap-servers"));
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);

        return new DefaultKafkaProducerFactory<>(config);
    }
}
