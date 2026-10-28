package ru.product.service.emailnotificationservice.exception;

/**
 * Исключение, при котором, Consumer не будет ретраить сообщение, а будет
 * отправлять сообщение в DLT топик
 */

public class NonRetryableException extends RuntimeException {

    public NonRetryableException(String message) {
        super(message);
    }

    public NonRetryableException(Throwable cause) {
        super(cause);
    }
}
