package org.eventviewer.ingest.controller;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import org.eventviewer.api.error.ErrorResponse;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.kafka.KafkaException;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.lang.reflect.Field;
import java.util.List;

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

    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<ErrorResponse> handleUnreadableMessage(HttpMessageNotReadableException ex) {
        String message = "Request body is missing or malformed";
        Throwable cause = ex.getCause();
        if (cause instanceof InvalidFormatException ife && !ife.getPath().isEmpty()) {
            String fieldName = ife.getPath().get(0).getFieldName();
            message = String.format("'%s': invalid value — %s", fieldName, ife.getOriginalMessage());
        }
        return ResponseEntity.badRequest().body(ErrorResponse.of(message));
    }

    @ExceptionHandler(KafkaException.class)
    public ResponseEntity<ErrorResponse> handleKafkaFailure(KafkaException ex) {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .body(ErrorResponse.of("ingest unavailable"));
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
