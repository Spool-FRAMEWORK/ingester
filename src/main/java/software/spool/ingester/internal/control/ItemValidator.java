package software.spool.ingester.internal.control;

import software.spool.core.adapter.jackson.PayloadDeserializerFactory;
import software.spool.core.model.vo.EventMetadataKey;
import software.spool.ingester.internal.model.PublishedInboxItem;
import software.spool.validator.api.ValidationResult;
import software.spool.validator.engine.ValidatorRegistry;

import java.util.ArrayList;
import java.util.Optional;

public class ItemValidator {
    private final ValidatorRegistry registry;

    public ItemValidator(ValidatorRegistry registry) {
        this.registry = registry;
    }

    public ValidationResult validate(PublishedInboxItem item) {
        String sourceId = item.inboxItem().metadata().get(EventMetadataKey.SOURCE);
        Optional<Class<?>> payloadTypeOptional = registry.resolveClass(sourceId);
        if (payloadTypeOptional.isEmpty() || !registry.hasValidatorsFor(sourceId, payloadTypeOptional.get()))
            return ValidationResult.of(new ArrayList<>());
        return registry.validateAll(sourceId,
                PayloadDeserializerFactory.json().as(payloadTypeOptional.get()).deserialize(item.payload()));
    }
}

