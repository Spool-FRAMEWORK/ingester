package software.spool.ingester.api.utils;

import software.spool.core.adapter.logging.LoggerFactory;
import software.spool.core.exception.EventBusEmitException;
import software.spool.core.exception.PartitionKeyException;
import software.spool.core.model.SpoolFailedEvent;
import software.spool.core.model.failure.DataLakePersistFailed;
import software.spool.core.port.bus.EventPublisher;
import software.spool.core.utils.routing.ErrorRouter;
import software.spool.ingester.internal.exception.DataLakeWriteException;

public class IngesterErrorRouter {

    public static ErrorRouter defaults(EventPublisher publisher) {
        return new ErrorRouter()
                .on(DataLakeWriteException.class,
                        (e, cause) -> publishAndLog(DataLakePersistFailed.builder()
                                .errorMessage(e.getMessage()).build(), publisher))
                .on(PartitionKeyException.class,
                        (e, cause) -> publishAndLog(DataLakePersistFailed.builder()
                                .errorMessage(e.getMessage()).build(), publisher))
                .on(EventBusEmitException.class, (e, cause) ->
                        LoggerFactory.getLogger(EventBusEmitException.class).error(e.getMessage()));
    }

    private static void publishAndLog(SpoolFailedEvent event, EventPublisher publisher) {
        publisher.publish(event);
        LoggerFactory.getLogger(EventBusEmitException.class).error(event.errorMessage());
    }
}
