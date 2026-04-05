package software.spool.ingester.internal.control;

import software.spool.core.adapter.jackson.PayloadDeserializerFactory;
import software.spool.core.model.event.ItemPublished;
import software.spool.core.model.vo.EventMetadataKey;
import software.spool.validator.api.ValidationResult;
import software.spool.validator.engine.ValidatorRegistry;

import java.util.ArrayList;
import java.util.Objects;

public class ItemValidator {
    private final ValidatorRegistry registry;

    public ItemValidator(ValidatorRegistry registry) {
        this.registry = registry;
    }

    public ValidationResult validate(ItemPublished item) {
        try {
            if (Objects.isNull(item.metadata().get(EventMetadataKey.TYPE))) return ValidationResult.of(new ArrayList<>());
            Class<?> type = Class.forName(
                    item.metadata().get(EventMetadataKey.TYPE),
                    true,
                    Thread.currentThread().getContextClassLoader()
            );
            Object payload = PayloadDeserializerFactory.json().as(type).deserialize(item.payload());
            return registry.validateAll(payload);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Unknown payload type for item: " + item.idempotencyKey(), e);
        }
    }
}

