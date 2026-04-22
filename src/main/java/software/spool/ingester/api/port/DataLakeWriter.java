package software.spool.ingester.api.port;

import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.model.vo.InboxItem;

import java.util.Collection;
import java.util.stream.Stream;

@FunctionalInterface
public interface DataLakeWriter {
    Stream<IdempotencyKey> write(Collection<InboxItem> items);
}