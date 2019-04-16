package com.company.exception;

/**
 * Ошибка переполнения hashmap
 */
public class HashmapOversizeException extends Exception {
    public HashmapOversizeException() {
        super();
    }
    public HashmapOversizeException(String message) {
        super(message);
    }
}
