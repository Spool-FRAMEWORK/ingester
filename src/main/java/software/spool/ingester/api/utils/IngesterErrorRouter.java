package software.spool.ingester.api.utils;

import software.spool.core.model.*;
import software.spool.core.port.EventBusEmitter;
import software.spool.core.utils.ErrorRouter;
import software.spool.ingester.internal.exception.DataLakeWriteException;

/**
 * Provides the default {@link ErrorRouter} configuration for the ingester.
 *
 * <p>
 * The routing table maps {@link DataLakeWriteException} to a
 * {@link DataLakePersistFailed} event emitted on the {@link EventBusEmitter}.
 * </p>
 *
 * @see ErrorRouter
 */
public class IngesterErrorRouter {

    /**
     * Creates the default error router for ingester operations.
     *
     * @param bus the event bus emitter used to publish failure events;
     *            must not be {@code null}
     * @return a pre-configured {@link ErrorRouter}
     */
    public static ErrorRouter defaults(EventBusEmitter bus) {
        return new ErrorRouter()
                .on(DataLakeWriteException.class,
                        (e, cause) -> bus.emit(DataLakePersistFailed.builder()
                                .errorMessage(e.getMessage()).build()));
    }
}
