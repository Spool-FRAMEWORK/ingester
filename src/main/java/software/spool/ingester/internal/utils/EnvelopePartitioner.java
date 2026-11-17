package software.spool.ingester.internal.utils;

import software.spool.core.model.vo.Envelope;
import software.spool.core.model.vo.PartitionKey;

@FunctionalInterface
public interface EnvelopePartitioner {
    PartitionKey partitionOf(Envelope envelope);
}
