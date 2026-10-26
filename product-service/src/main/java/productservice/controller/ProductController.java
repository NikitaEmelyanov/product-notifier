package productservice.controller;

import java.util.Date;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import productservice.dto.CreateProductDto;
import productservice.exception.ErrorMessage;
import productservice.service.ProductService;

@RestController
@RequestMapping("/product")
public class ProductController {

    private ProductService productService;
    private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

    public ProductController(ProductService productService) {
        this.productService = productService;
    }

    /**
     * Метод добавления нового продукта через кафку асинхронно
     * @param createProductDto Cодержащая информацию о создании продукта через кафку. На вход будет принимать
     *            JSON, который нужно смапить на ДТО
     * @return id продукта
     */
    @PostMapping("/async")
    public ResponseEntity<String> createProductAsync(@RequestBody CreateProductDto createProductDto) {
        String productId = productService.createProductAsync(createProductDto);
        return ResponseEntity.status(HttpStatus.CREATED).body(productId);
    }

    /**
     * Метод добавления нового продукта через кафку синхронно
     * @param createProductDto Cодержащая информацию о создании продукта через кафку. На вход будет принимать
     *            JSON, который нужно смапить на ДТО
     * @return id продукта
     */
    //TODO: Грязноватый метод, переписать с ControllerAdvice
    @PostMapping("/sync")
    public ResponseEntity<Object> createProductSync(@RequestBody CreateProductDto createProductDto) {
        String productId = null;
        try {
            productId = productService.createProductSync(createProductDto);
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).
                body(new ErrorMessage(new Date(), e.getMessage()));
        }
        return ResponseEntity.status(HttpStatus.CREATED).body(productId);
    }
}
