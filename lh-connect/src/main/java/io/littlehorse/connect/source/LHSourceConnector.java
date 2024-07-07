package io.littlehorse.connect.source;

import java.util.Properties;

public abstract class LHSourceConnector {

    public abstract void configure(Properties applicationConfig, LHSourceConnectorContext context);

    public abstract void start();
}
