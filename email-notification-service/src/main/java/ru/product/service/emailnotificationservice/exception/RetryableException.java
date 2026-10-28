package ru.product.service.emailnotificationservice.exception;

/**
 * Исключение, при котором Consumer должен будет ретраить сообщение и обрабатывать
 * его в штатном режиме
 */
public class RetryableException extends RuntimeException {


    public RetryableException(String message) {
        super(message);
    }

    public RetryableException(Throwable cause) {
        super(cause);
    }
}
