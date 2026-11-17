package software.spool.ingester.internal.utils;

import software.spool.core.adapter.jackson.PayloadDeserializerFactory;
import software.spool.core.model.vo.Envelope;
import software.spool.core.model.vo.EventMetadataKey;
import software.spool.core.model.vo.PartitionKey;
import software.spool.core.model.vo.PartitionKeySchema;

public class MetadataSchemaPartitioner implements EnvelopePartitioner {

    @Override
    public PartitionKey partitionOf(Envelope envelope) {
        return PartitionKey.of(partitionKeySchema(envelope)).from(envelope.payload());
    }

    private static PartitionKeySchema partitionKeySchema(Envelope envelope) {
        return PayloadDeserializerFactory.json().as(PartitionKeySchema.class)
                .deserialize(envelope.metadata().get(EventMetadataKey.PARTITION_SCHEMA));
    }
}
