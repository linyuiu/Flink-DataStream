package org.linyu.validation;

public class OrderValidationException extends IllegalArgumentException {
    public OrderValidationException(String message) {
        super(message);
    }
}
