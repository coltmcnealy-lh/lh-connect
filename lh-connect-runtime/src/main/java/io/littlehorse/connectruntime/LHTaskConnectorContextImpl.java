package io.littlehorse.connectruntime;

import io.littlehorse.connect.task.LHTaskConnectorConfig;
import io.littlehorse.connect.task.LHTaskConnectorContext;
import io.littlehorse.sdk.common.proto.TaskDefId;
import io.littlehorse.sdk.common.proto.TenantId;

public class LHTaskConnectorContextImpl implements LHTaskConnectorContext {
    private final LHTaskConnectorConfig config;

    public LHTaskConnectorContextImpl(LHTaskConnectorConfig config) {
        this.config = config;
    }

    @Override
    public TaskDefId getTaskDef() {
        return TaskDefId.newBuilder().setName(config.getTaskDefName()).build();
    }

    @Override
    public TenantId getTenant() {
        return config.toLHConfig().getTenantId();
    }
}
