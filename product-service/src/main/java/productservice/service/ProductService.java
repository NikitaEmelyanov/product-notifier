package productservice.service;

import java.util.concurrent.ExecutionException;
import productservice.dto.CreateProductDto;

/**
 * Интерфейс для работы с Product через кафку
 */
public interface ProductService {

    /**
     * Метод, позволяющий создать продукт через кафку асинхронно
     * @param createProductDto ДТО содержащее данные для создания продукта
     * @return productId
     */
    String createProductAsync(CreateProductDto createProductDto);

    /**
     * Метод, позволяющий создать продукт через кафку синхронно (ждем ответ)
     * @param createProductDto ДТО содержащее данные для создания продукта
     * @return productId
     */
    String createProductSync(CreateProductDto createProductDto)
        throws ExecutionException, InterruptedException;
}
