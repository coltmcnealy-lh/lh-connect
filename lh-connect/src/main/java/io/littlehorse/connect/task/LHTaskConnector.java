package io.littlehorse.connect.task;

import java.util.Properties;

public abstract class LHTaskConnector {

    public static final String CONNECTOR_TASK_ANNOTATION = "${CONNECTOR_TASK}";

    /**
     * Called before starting the Task Connector. Allows configuring the Task Connector
     * with any properties passed in through the Connector configuraiton.
     * @param applicationConfig is a Properties object containing all the user-defined
     * configurations.
     * @param context is a LHTaskConnectorContext that may be used by the connector.
     */
    public abstract void configure(Properties applicationConfig, LHTaskConnectorContext context);
}
