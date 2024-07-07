package io.littlehorse.connect.common;

import io.littlehorse.sdk.common.config.LHConfig;
import java.util.Properties;

public abstract class AbstractConnectorConfig {

    public static final String LH_CONNECT_CONFIG_PREFIX = "LHCT_";

    private final Properties props;
    private Properties connectorProps;
    private LHConfig lhConfig;

    public AbstractConnectorConfig(Properties props) {
        this.props = props;
    }

    public LHConfig toLHConfig() {
        if (lhConfig == null) {
            Properties lhProps = new Properties();
            for (String key : props.stringPropertyNames()) {
                String formattedKey = formatKey(key);
                if (formattedKey.startsWith("LITTLEHORSE_")) {
                    lhProps.put(formattedKey.substring("LITTLEHORSE_".length()), String.valueOf(props.get(key)));
                }
            }
            lhConfig = new LHConfig(lhProps);
        }
        return lhConfig;
    }

    public Properties toApplicationConfig() {
        if (connectorProps == null) {
            connectorProps = new Properties();
            for (String key : props.stringPropertyNames()) {
                String formattedKey = formatKey(key);
                if (formattedKey.startsWith("LHCT_CONNECTOR_")) {
                    connectorProps.put(key.substring("LHCT_CONNECTOR_".length()), props.get(key));
                }
            }
        }
        return connectorProps;
    }

    private String formatKey(String key) {
        return key.toUpperCase().replace(".", "_");
    }

    public String getLHConnectorConfig(String unformattedKey) {
        unformattedKey = "lhct." + unformattedKey;
        if (props.containsKey(unformattedKey)) {
            return String.valueOf(props.get(unformattedKey));
        } else if (props.containsKey(formatKey(unformattedKey))) {
            return String.valueOf(props.get(formatKey(unformattedKey)));
        }
        return null;
    }
}
