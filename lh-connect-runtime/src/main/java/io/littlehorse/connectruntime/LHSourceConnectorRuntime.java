package io.littlehorse.connectruntime;

import io.littlehorse.connect.source.LHSourceConnector;
import io.littlehorse.connect.source.LHSourceConnectorMode;
import io.littlehorse.sdk.common.config.LHConfig;
import io.littlehorse.sdk.common.proto.PutExternalEventDefRequest;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LHSourceConnectorRuntime {

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: java LHSourceConnectorRuntime <full-class-name> <properties-file-path>");
            System.exit(1);
        }
        String connectorClassName = args[0];
        String propertiesFilePath = args[1];

        LHSourceConnector connector = null;
        Properties properties = null;

        try {
            @SuppressWarnings("unchecked")
            Class<? extends LHSourceConnector> connectorClass =
                    (Class<? extends LHSourceConnector>) Class.forName(connectorClassName);
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

        LHSourceConnectorConfigImpl config = new LHSourceConnectorConfigImpl(properties);
        LHConfig lhConfig = config.toLHConfig();
        Properties applicationConfig = config.toApplicationConfig();
        LHSourceConnectorContextImpl context = new LHSourceConnectorContextImpl(config);

        if (config.getMode() == LHSourceConnectorMode.EXTERNAL_EVENT) {
            // Register external event. TODO: Make this configurable
            lhConfig.getBlockingStub()
                    .putExternalEventDef(PutExternalEventDefRequest.newBuilder()
                            .setName(config.getExternalEventDefName().get())
                            .build());
        }
        connector.configure(applicationConfig, context);

        connector.start();
    }
}
