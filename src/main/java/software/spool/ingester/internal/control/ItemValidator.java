package software.spool.ingester.internal.control;

import software.spool.core.infrastructure.adapter.PayloadDeserializerFactory;
import software.spool.core.model.EventMetadataKey;
import software.spool.core.model.ItemPublished;
import software.spool.validator.api.ValidationResult;
import software.spool.validator.engine.ValidatorRegistry;

public class ItemValidator {
    private final ValidatorRegistry registry;

    public ItemValidator(ValidatorRegistry registry) {
        this.registry = registry;
    }

    public ValidationResult validate(ItemPublished item) {
        try {
            Class<?> type = Class.forName(
                    item.metadata().get(EventMetadataKey.TYPE),
                    true,
                    Thread.currentThread().getContextClassLoader()
            );
            Object payload = PayloadDeserializerFactory.jsonAs(type).deserialize(item.payload());
            return registry.validateAll(payload);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Unknown payload type for item: " + item.idempotencyKey(), e);
        }
    }
}

