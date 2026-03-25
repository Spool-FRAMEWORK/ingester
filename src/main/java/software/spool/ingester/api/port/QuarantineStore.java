package software.spool.ingester.api.port;

public interface QuarantineStore {
    void send(QuarantinedRecord record);
}
