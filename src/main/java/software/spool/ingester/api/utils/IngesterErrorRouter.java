package software.spool.ingester.api.utils;

import software.spool.core.adapter.logging.LoggerFactory;
import software.spool.core.exception.EventBrokerEmitException;
import software.spool.core.exception.InboxUpdateException;
import software.spool.core.exception.PartitionKeyException;
import software.spool.core.model.Event;
import software.spool.core.model.failure.DataLakePersistFailed;
import software.spool.core.model.failure.InboxItemStoreFailed;
import software.spool.core.port.bus.BrokerMessage;
import software.spool.core.port.bus.Destination;
import software.spool.core.port.bus.EventPublisher;
import software.spool.core.utils.routing.ErrorRouter;
import software.spool.ingester.internal.exception.DataLakeWriteException;

import java.util.Map;

public class IngesterErrorRouter {

    public static ErrorRouter defaults(EventPublisher publisher) {
        return new ErrorRouter()
                .on(DataLakeWriteException.class,
                        (e, cause) -> publish(DataLakePersistFailed.builder()
                                .errorMessage(e.getMessage()).build(), publisher))
                .on(PartitionKeyException.class,
                        (e, cause) -> publish(DataLakePersistFailed.builder()
                                .errorMessage(e.getMessage()).build(), publisher))
                .on(InboxUpdateException.class,
                        (e, cause) -> publish(InboxItemStoreFailed.builder()
                                .errorMessage(e.getMessage()).idempotencyKey(e.getIdempotencyKey()).build(), publisher))
                .on(EventBrokerEmitException.class, (e, cause) ->
                        LoggerFactory.getLogger(EventBrokerEmitException.class).error(e.getMessage()));
    }

    private static void publish(Event event, EventPublisher publisher) {
        publisher.publish(new Destination("spool." + event.getClass().getSimpleName()),
                new BrokerMessage<>(event, event.getClass().getSimpleName(), Map.of()));
    }
}
