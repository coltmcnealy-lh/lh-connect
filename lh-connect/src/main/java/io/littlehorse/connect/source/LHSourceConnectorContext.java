package io.littlehorse.connect.source;

import io.littlehorse.sdk.common.proto.TenantId;
import java.util.List;

public interface LHSourceConnectorContext {

    public TenantId getTenant();

    public LHSourceConnectorMode getMode();

    public void produce(LHSourceRecord record);

    /**
     * Flushes all produced `LHSourceRecord`s to LittleHorse and returns a list of failed
     * records, if any.
     * @return a list of all records that we failed to send to LittleHorse.
     */
    public List<FailedRecord> flushRecords();
}
