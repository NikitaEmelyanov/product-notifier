package ru.product.service.mockservice.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Контроллер-заглушка для mock-service
 */
@RestController
@RequestMapping("/response")
public class StatusCheckController {

    /**
     * Метод позволяющий вернуть код 200 при отправке 200 в этот эндпоинт
     * @return
     */
    @GetMapping("/200")
    ResponseEntity<String> response200String() {
        return ResponseEntity.ok().body("200");
    }

    /**
     * Метод позволяющий получить 500 ошибку при отправке 500 в этот эндпоинт
     */
    @GetMapping("/500")
    ResponseEntity<String> response500String() {
        return ResponseEntity.internalServerError().build();
    }

}
