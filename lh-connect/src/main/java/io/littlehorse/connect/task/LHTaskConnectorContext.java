package io.littlehorse.connect.task;

import io.littlehorse.sdk.common.proto.TaskDefId;
import io.littlehorse.sdk.common.proto.TenantId;

public interface LHTaskConnectorContext {

    TaskDefId getTaskDef();

    TenantId getTenant();
}
