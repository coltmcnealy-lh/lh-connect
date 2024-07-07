package io.littlehorse.connect.task;

import io.littlehorse.connect.common.AbstractConnectorConfig;
import io.littlehorse.sdk.common.exception.LHMisconfigurationException;
import java.util.Properties;

public class LHTaskConnectorConfig extends AbstractConnectorConfig {

    public static final String TASKDEF_NAME_KEY = "TASKDEF_NAME";

    public String getTaskDefName() {
        String result = getLHConnectorConfig(TASKDEF_NAME_KEY);
        if (result == null) {
            throw new LHMisconfigurationException("Must configure lhct.taskdef.name");
        }

        return result;
    }

    public LHTaskConnectorConfig(Properties props) {
        super(props);
    }
}
