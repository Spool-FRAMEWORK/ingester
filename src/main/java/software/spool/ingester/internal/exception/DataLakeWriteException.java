package software.spool.ingester.internal.exception;

import software.spool.core.exception.SpoolException;

/**
 * Exception thrown when a write operation to the data lake fails.
 *
 * <p>
 * This is a typed {@link SpoolException} subclass used by the
 * {@link software.spool.ingester.internal.decorator.SafeDataLakeWriter}
 * to normalise unchecked exceptions, and by the
 * {@link software.spool.ingester.api.utils.IngesterErrorRouter}
 * to route errors to failure events.
 * </p>
 */
public class DataLakeWriteException extends SpoolException {
    public DataLakeWriteException(String message) {
        super("Error occurred while writing to DataLake: " + message);
    }

    public DataLakeWriteException(String message, Throwable cause) {
        super("Error occurred while writing to DataLake: " + message, cause);
    }
}
