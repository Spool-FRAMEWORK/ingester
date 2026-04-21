package software.spool.ingester.internal.control;

import software.spool.core.adapter.jackson.PayloadDeserializerFactory;
import software.spool.core.adapter.logging.LoggerFactory;
import software.spool.core.model.event.ItemPublished;
import software.spool.core.model.vo.EventMetadataKey;
import software.spool.validator.api.ValidationResult;
import software.spool.validator.engine.ValidatorRegistry;

import java.util.ArrayList;
import java.util.Objects;
import java.util.Optional;
import java.util.logging.SocketHandler;

public class ItemValidator {
    private final ValidatorRegistry registry;

    public ItemValidator(ValidatorRegistry registry) {
        this.registry = registry;
    }

    public ValidationResult validate(ItemPublished item) {
        Optional<Class<?>> payloadTypeOptional = registry.resolveClass(item.metadata().get(EventMetadataKey.SOURCE));
        if (payloadTypeOptional.isEmpty() || !registry.hasValidatorsFor(item.metadata().get(EventMetadataKey.SOURCE), payloadTypeOptional.get()))
            return ValidationResult.of(new ArrayList<>());
        return registry.validateAll(item.metadata().get(EventMetadataKey.SOURCE),
                PayloadDeserializerFactory.json().as(payloadTypeOptional.get()).deserialize(item.payload()));
    }
}

