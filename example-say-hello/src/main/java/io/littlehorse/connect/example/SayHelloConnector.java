package io.littlehorse.connect.example;

import io.littlehorse.connect.task.LHTaskConnector;
import io.littlehorse.connect.task.LHTaskConnectorContext;
import io.littlehorse.sdk.worker.LHTaskMethod;
import io.littlehorse.sdk.worker.WorkerContext;
import java.util.Properties;

public class SayHelloConnector extends LHTaskConnector {

    @Override
    public void configure(Properties config, LHTaskConnectorContext context) {
        // Nothing to do
    }

    @LHTaskMethod(LHTaskConnector.CONNECTOR_TASK_ANNOTATION)
    public String sayHello(String name, WorkerContext context) {
        String message = "Hello to " + name + " from workflow run " + context.getWfRunId();
        System.out.println(message);
        return message;
    }
}
