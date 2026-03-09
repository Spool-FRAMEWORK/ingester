package software.spool.ingester.internal.exception;

import software.spool.core.exception.SpoolException;

public class DataLakeWriteException extends SpoolException {
    public DataLakeWriteException(String message) {
        super("Error occurred while writing to DataLake: " + message);
    }
    public DataLakeWriteException(String message, Throwable cause) {
        super("Error occurred while writing to DataLake: " + message, cause);
    }
}
