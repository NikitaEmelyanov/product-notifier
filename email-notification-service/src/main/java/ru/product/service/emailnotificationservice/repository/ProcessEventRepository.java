package ru.product.service.emailnotificationservice.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import ru.product.service.emailnotificationservice.entity.ProcessedEventEntity;

/**
 * Репозиторий для работы с сущностью ProcessedEventEntity
 */
@Repository
public interface ProcessEventRepository extends JpaRepository<ProcessedEventEntity, Long> {

    /**
     * Метод для поиска messageId в БД
     * @param messageId проверяемый messageId
     * @return Сущность с полями messageId и productId
     */
    ProcessedEventEntity findByMessageId(String messageId);
}
