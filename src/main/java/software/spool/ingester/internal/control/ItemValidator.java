package software.spool.ingester.internal.control;

import software.spool.core.adapter.jackson.PayloadDeserializerFactory;
import software.spool.core.model.event.ItemPublished;
import software.spool.core.model.vo.EventMetadataKey;
import software.spool.validator.api.ValidationResult;
import software.spool.validator.engine.ValidatorRegistry;

import java.util.ArrayList;
import java.util.Objects;
import java.util.Optional;

public class ItemValidator {
    private final ValidatorRegistry registry;

    public ItemValidator(ValidatorRegistry registry) {
        this.registry = registry;
    }

    public ValidationResult validate(ItemPublished item) {
        Optional<Class<?>> payloadTypeOptional = registry.resolveClass(item.metadata().get(EventMetadataKey.SOURCE));
        if (Objects.isNull(item.metadata().get(EventMetadataKey.TYPE)) || payloadTypeOptional.isEmpty())
            return ValidationResult.of(new ArrayList<>());
        Object payload = PayloadDeserializerFactory.json().as(payloadTypeOptional.get())
                .deserialize(item.payload());
        return registry.validateAll(item.metadata().get(EventMetadataKey.SOURCE), payload);
    }
}

