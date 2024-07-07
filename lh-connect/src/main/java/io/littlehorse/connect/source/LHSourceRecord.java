package io.littlehorse.connect.source;

import io.littlehorse.sdk.common.proto.VariableValue;
import java.util.Optional;

public abstract class LHSourceRecord {

    /**
     *
     * @param path is the field path to get
     * @return the value of that field path
     */
    public abstract Optional<VariableValue> getField(String path);

    /**
     * Allows specifying headers
     * @param key is the header key
     * @return the String value of the specified header
     */
    public abstract String getHeader(String key);

    // TODO: Allow support for Schema's once they are allowed in LittleHorse.

    /**
     * Record ID is used to allow idempotency.
     * @return Optional record ID if present.
     */
    public Optional<String> getRecordId() {
        return Optional.empty();
    }
}
