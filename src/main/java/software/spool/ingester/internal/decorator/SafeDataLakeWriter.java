package software.spool.ingester.internal.decorator;

import software.spool.core.exception.SpoolException;
import software.spool.core.model.event.ItemPublished;
import software.spool.ingester.api.port.DataLakeWriter;
import software.spool.ingester.internal.exception.DataLakeWriteException;

import java.util.Collection;

/**
 * Decorator for {@link DataLakeWriter} that normalises unchecked exceptions
 * into typed {@link DataLakeWriteException} instances.
 *
 * <p>
 * If the delegate throws a {@link SpoolException} subclass, it is re-thrown
 * as-is. Any other {@link Exception} is wrapped in a new
 * {@link DataLakeWriteException}.
 * </p>
 */
public class SafeDataLakeWriter implements DataLakeWriter {
    private final DataLakeWriter writer;

    public SafeDataLakeWriter(DataLakeWriter writer) {
        this.writer = writer;
    }

    /**
     * Creates a new {@code SafeDataLakeWriter} wrapping the given delegate.
     *
     * @param writer the writer to wrap; must not be {@code null}
     * @return a new {@code SafeDataLakeWriter} instance
     */
    public static DataLakeWriter of(DataLakeWriter writer) {
        return new SafeDataLakeWriter(writer);
    }

    @Override
    public void write(Collection<ItemPublished> items) {
        try {
            writer.write(items);
        } catch (SpoolException e) {
            throw e;
        } catch (Exception e) {
            throw new DataLakeWriteException(e.getMessage(), e);
        }
    }
}
