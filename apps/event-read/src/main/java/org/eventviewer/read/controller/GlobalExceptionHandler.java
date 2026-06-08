package org.eventviewer.read.controller;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import org.eventviewer.api.error.ErrorResponse;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import jakarta.validation.ConstraintViolationException;
import org.eventviewer.read.domain.EventSearchException;
import java.lang.reflect.Field;
import java.util.List;
import java.util.UUID;

@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ErrorResponse> handleValidation(MethodArgumentNotValidException ex) {
        Class<?> targetClass = ex.getBindingResult().getTarget() != null
                ? ex.getBindingResult().getTarget().getClass()
                : null;

        List<String> errors = ex.getBindingResult().getFieldErrors().stream()
                .map(e -> formatFieldError(targetClass, e))
                .toList();
        return ResponseEntity.badRequest().body(new ErrorResponse(errors));
    }

    @ExceptionHandler(ConstraintViolationException.class)
    public ResponseEntity<ErrorResponse> handleConstraintViolation(ConstraintViolationException ex) {
        List<String> errors = ex.getConstraintViolations().stream()
                .map(v -> "'" + v.getPropertyPath() + "': " + v.getMessage())
                .toList();
        return ResponseEntity.badRequest().body(new ErrorResponse(errors));
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<ErrorResponse> handleUnreadableMessage(HttpMessageNotReadableException ex) {
        Throwable cause = ex.getCause();
        if (cause instanceof InvalidFormatException ife && !ife.getPath().isEmpty()) {
            var ref = ife.getPath().get(0);
            String javaFieldName = ref.getFieldName();
            Object from = ref.getFrom();
            Class<?> targetClass = (from instanceof Class<?> c) ? c : (from != null ? from.getClass() : null);
            String fieldName = resolveJsonPropertyName(targetClass, javaFieldName);
            Object rejected = ife.getValue();
            if (UUID.class.equals(ife.getTargetType())) {
                return ResponseEntity.badRequest().body(ErrorResponse.of(
                        String.format("'%s': must be a valid UUID (e.g. 550e8400-e29b-41d4-a716-446655440000) (received: '%s')", fieldName, rejected)
                ));
            }
            return ResponseEntity.badRequest().body(ErrorResponse.of(
                    String.format("'%s': invalid value (received: '%s')", fieldName, rejected)
            ));
        }
        return ResponseEntity.badRequest().body(ErrorResponse.of("Request body is missing or malformed"));
    }

    @ExceptionHandler(EventSearchException.class)
    public ResponseEntity<ErrorResponse> handleSearchException(EventSearchException ex) {
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(ErrorResponse.of("search unavailable"));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handleUnexpected(Exception ex) {
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(ErrorResponse.of("an unexpected error occurred"));
    }

    private String formatFieldError(Class<?> targetClass, FieldError e) {
        String fieldName = resolveJsonPropertyName(targetClass, e.getField());
        Object rejected = e.getRejectedValue();
        return rejected != null
                ? String.format("'%s': %s (received: '%s')", fieldName, e.getDefaultMessage(), rejected)
                : String.format("'%s': %s", fieldName, e.getDefaultMessage());
    }

    private String resolveJsonPropertyName(Class<?> targetClass, String javaFieldName) {
        if (targetClass == null) return javaFieldName;
        try {
            Field field = targetClass.getDeclaredField(javaFieldName);
            JsonProperty annotation = field.getAnnotation(JsonProperty.class);
            return (annotation != null && !annotation.value().isEmpty()) ? annotation.value() : javaFieldName;
        } catch (NoSuchFieldException e) {
            return javaFieldName;
        }
    }
}
