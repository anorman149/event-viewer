package org.eventviewer.api.validation;

import jakarta.validation.Constraint;
import jakarta.validation.Payload;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.FIELD, ElementType.PARAMETER})
@Retention(RetentionPolicy.RUNTIME)
@Constraint(validatedBy = UuidValidator.class)
public @interface ValidUUID {

    String message() default "must be a valid UUID (e.g. 550e8400-e29b-41d4-a716-446655440000)";

    Class<?>[] groups() default {};

    Class<? extends Payload>[] payload() default {};
}
