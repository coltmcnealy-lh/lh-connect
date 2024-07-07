package io.littlehorse.connectruntime;

import io.littlehorse.connect.task.LHTaskConnector;
import io.littlehorse.connect.task.LHTaskConnectorConfig;
import io.littlehorse.sdk.common.config.LHConfig;
import io.littlehorse.sdk.worker.LHTaskWorker;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LHTaskConnectorRuntime {

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: java LHTaskConnectorRuntime <full-class-name> <properties-file-path>");
            System.exit(1);
        }
        String connectorClassName = args[0];
        String propertiesFilePath = args[1];

        LHTaskConnector connector = null;
        Properties properties = null;

        try {
            @SuppressWarnings("unchecked")
            Class<? extends LHTaskConnector> connectorClass =
                    (Class<? extends LHTaskConnector>) Class.forName(connectorClassName);
            connector = connectorClass.getDeclaredConstructor().newInstance();

            // Load properties from the file
            properties = new Properties();
            try (FileInputStream input = new FileInputStream(propertiesFilePath)) {
                properties.load(input);
                log.info("Properties loaded from file: {}", propertiesFilePath);
            } catch (IOException e) {
                System.err.println("Error loading properties file: " + e.getMessage());
                System.exit(2);
            }
        } catch (ClassNotFoundException e) {
            System.err.println("Class not found: " + connectorClassName);
            System.exit(3);
        } catch (InstantiationException
                | IllegalAccessException
                | NoSuchMethodException
                | InvocationTargetException e) {
            System.err.println("Error instantiating class: " + e.getMessage());
            System.exit(4);
        }

        if (properties == null || connector == null) {
            throw new IllegalStateException("impossible");
        }

        LHTaskConnectorConfig config = new LHTaskConnectorConfig(properties);
        LHConfig lhConfig = config.toLHConfig();
        Properties applicationConfig = config.toApplicationConfig();
        LHTaskConnectorContextImpl context = new LHTaskConnectorContextImpl(config);
        String taskDefName = config.getTaskDefName();

        connector.configure(applicationConfig, context);

        Map<String, String> annotationPlacholders = Map.of("CONNECTOR_TASK", taskDefName);
        LHTaskWorker worker =
                new LHTaskWorker(connector, LHTaskConnector.CONNECTOR_TASK_ANNOTATION, lhConfig, annotationPlacholders);
        Runtime.getRuntime().addShutdownHook(new Thread(worker::close));
        worker.registerTaskDef();
        worker.start();
    }
}
