package io.littlehorse.connectruntime;

import io.littlehorse.connect.source.LHSourceConnectorConfig;
import io.littlehorse.connect.source.LHSourceConnectorMode;
import java.util.Properties;

public class LHSourceConnectorConfigImpl extends LHSourceConnectorConfig {

    private RunWfGenerator wfRunGenerator;
    private PostEventGenerator eventGenerator;

    public LHSourceConnectorConfigImpl(Properties props) {
        super(props);
    }

    public RunWfGenerator toWfRunGenerator() {
        if (getMode() != LHSourceConnectorMode.WF_RUN) {
            throw new IllegalStateException();
        }
        if (wfRunGenerator == null) {
            wfRunGenerator = new RunWfGenerator(this);
        }
        return wfRunGenerator;
    }

    public PostEventGenerator toEventGenerator() {
        if (getMode() != LHSourceConnectorMode.EXTERNAL_EVENT) {
            throw new IllegalStateException();
        }
        if (eventGenerator == null) {
            eventGenerator = new PostEventGenerator(this);
        }
        return eventGenerator;
    }

    public String getVarConfig(String config, String variable) {
        return getLHConnectorConfig(config.replace("{variable}", variable));
    }
}
