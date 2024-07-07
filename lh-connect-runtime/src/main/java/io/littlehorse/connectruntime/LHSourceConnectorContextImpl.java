package io.littlehorse.connectruntime;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Message;
import io.littlehorse.connect.source.FailedRecord;
import io.littlehorse.connect.source.LHSourceConnectorContext;
import io.littlehorse.connect.source.LHSourceConnectorMode;
import io.littlehorse.connect.source.LHSourceRecord;
import io.littlehorse.sdk.common.proto.ExternalEvent;
import io.littlehorse.sdk.common.proto.LittleHorseGrpc.LittleHorseFutureStub;
import io.littlehorse.sdk.common.proto.TenantId;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.tuple.Pair;

@Slf4j
public class LHSourceConnectorContextImpl implements LHSourceConnectorContext {

    private List<Pair<LHSourceRecord, ListenableFuture<? extends Message>>> futures;
    private Lock lock;
    private LittleHorseFutureStub lhClient;

    @Getter
    private final LHSourceConnectorConfigImpl config;

    public LHSourceConnectorContextImpl(LHSourceConnectorConfigImpl config) {
        this.config = config;
        this.futures = new ArrayList<>();
        this.lock = new ReentrantLock();
        this.lhClient = config.toLHConfig().getFutureStub();
    }

    @Override
    public LHSourceConnectorMode getMode() {
        return config.getMode();
    }

    @Override
    public TenantId getTenant() {
        return config.toLHConfig().getTenantId();
    }

    @Override
    public void produce(LHSourceRecord record) {
        ListenableFuture<? extends Message> future;

        if (config.getMode() == LHSourceConnectorMode.WF_RUN) {
            future = lhClient.runWf(config.toWfRunGenerator().toRequest(record));
        } else {
            future = lhClient.putExternalEvent(config.toEventGenerator().toRequest(record));
        }

        try {
            lock.lock();
            futures.add(Pair.of(record, future));
        } finally {
            lock.unlock();
        }
    }

    @Override
    public List<FailedRecord> flushRecords() {
        List<FailedRecord> failedRecords = new ArrayList<>();
        try {
            lock.lock();
            for (int i = futures.size() - 1; i >= 0; i--) {
                Pair<LHSourceRecord, ListenableFuture<? extends Message>> pair = futures.get(i);
                try {
                    pair.getRight().get();
                } catch (Exception exn) {
                    log.error("Failed processing record: ", exn);
                    failedRecords.add(new FailedRecord(pair.getLeft(), exn.getCause()));
                }
                futures.remove(i);
            }
        } finally {
            lock.unlock();
        }
        return failedRecords;
    }

    private ListenableFuture<ExternalEvent> postEvent(LHSourceRecord record) {
        throw new NotImplementedException();
    }
}

/*
- Store offsets in LH
- Clean futures in background if flushRecords() isn't called
- Timeout in flushRecords()
- Validations in Config
 */
