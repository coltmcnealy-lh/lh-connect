package io.littlehorse.connect.source;

public class FailedRecord {

    private final LHSourceRecord record;
    private final Throwable cause;

    public FailedRecord(LHSourceRecord record, Throwable cause) {
        this.record = record;
        this.cause = cause;
    }

    public Throwable getCause() {
        return this.cause;
    }

    public LHSourceRecord getRecord() {
        return record;
    }
}
