package software.spool.ingester.internal.control;

import software.spool.core.adapter.jackson.PayloadDeserializerFactory;
import software.spool.core.model.vo.Envelope;
import software.spool.core.model.vo.EventMetadataKey;
import software.spool.validator.api.ValidationResult;
import software.spool.validator.engine.ValidatorRegistry;

import java.util.ArrayList;
import java.util.Optional;

public class ItemValidator {
    private final ValidatorRegistry registry;

    public ItemValidator(ValidatorRegistry registry) {
        this.registry = registry;
    }

    public ValidationResult validate(Envelope envelope) {
        String sourceId = envelope.metadata().get(EventMetadataKey.SOURCE);
        Optional<Class<?>> payloadTypeOptional = registry.resolveClass(sourceId);
        if (payloadTypeOptional.isEmpty() || !registry.hasValidatorsFor(sourceId, payloadTypeOptional.get()))
            return ValidationResult.of(new ArrayList<>());
        return registry.validateAll(sourceId,
                PayloadDeserializerFactory.json().as(payloadTypeOptional.get()).deserialize(envelope.payload()));
    }
}

