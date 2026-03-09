package software.spool.ingester.internal.decorator;

import software.spool.core.exception.SpoolException;
import software.spool.core.model.Event;
import software.spool.ingester.api.port.DataLakeWriter;
import software.spool.ingester.internal.exception.DataLakeWriteException;

import java.util.Collection;

public class SafeDataLakeWriter implements DataLakeWriter {
    private final DataLakeWriter writer;

    public SafeDataLakeWriter(DataLakeWriter writer) {
        this.writer = writer;
    }

    public static DataLakeWriter of(DataLakeWriter writer) {
        return new SafeDataLakeWriter(writer);
    }

    @Override
    public <E extends Event> void write(Collection<E> items) {
        try {
            writer.write(items);
        } catch (SpoolException e) { throw e; }
        catch (Exception e) {
            throw new DataLakeWriteException(e.getMessage(), e);
        }
    }
}
