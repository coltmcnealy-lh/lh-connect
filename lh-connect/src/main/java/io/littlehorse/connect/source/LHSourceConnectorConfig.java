package io.littlehorse.connect.source;

import io.littlehorse.connect.common.AbstractConnectorConfig;
import io.littlehorse.sdk.common.exception.LHMisconfigurationException;
import java.util.Optional;
import java.util.Properties;

public abstract class LHSourceConnectorConfig extends AbstractConnectorConfig {

    public LHSourceConnectorConfig(Properties props) {
        super(props);
    }

    public static final String MODE_KEY = "mode";

    // WfRun Connector Configurations
    public static final String WF_SPEC_NAME_KEY = "wfspec.name";
    public static final String WF_SPEC_MAJOR_VERSION_KEY = "wfspec.major.version";
    public static final String WF_SPEC_REVISION_KEY = "wfspec.revision";

    public static final String WFRUN_ID_FIELD_KEY = "wfrun.id.field";
    public static final String WFRUN_ID_HEADER_KEY = "wfrun.id.header";

    public static final String VARIABLES_KEY = "wfrun.variables";
    public static final String VARIABLE_FIELD_KEY = "wfrun.variables.{variable}.field";
    public static final String VARIABLE_HEADER_KEY = "wfrun.variables.{variable}.header";
    public static final String VARIABLE_TYPE_KEY = "wfrun.variables.{variable}.type";
    public static final String VARIABLE_LITERAL_KEY = "wfrun.variables.{variable}.literal";

    // ExternalEvent Connector Configurations
    public static final String EXT_EVT_DEF_NAME_KEY = "externaleventdef.name";

    public static final String EXT_EVT_GUID_FIELD_KEY = "externalevent.guid.field";
    public static final String EXT_EVT_GUID_HEADER_KEY = "externalevent.guid.header";

    public static final String EXT_EVT_CONTENT_TYPE_KEY = "externalevent.content.type";
    public static final String EXT_EVT_CONTENT_FIELD_KEY = "externalevent.content.field";
    public static final String EXT_EVT_CONTENT_HEADER_KEY = "externalevent.content.header";
    public static final String EXT_EVT_CONTENT_LITERAL_KEY = "externalevent.content.literal";

    public LHSourceConnectorMode getMode() {
        String mode = getLHConnectorConfig(MODE_KEY);
        if (mode != null) {
            switch (mode) {
                case "WF_RUN":
                    return LHSourceConnectorMode.WF_RUN;
                case "EXTERNAL_EVENT":
                    return LHSourceConnectorMode.EXTERNAL_EVENT;
            }
        }
        throw new LHMisconfigurationException("Must set lhct.mode to either WF_RUN or EXTERNAL_EVENT");
    }

    public Optional<String> getExternalEventDefName() {
        if (getMode() == LHSourceConnectorMode.EXTERNAL_EVENT) {
            String externalEventDefName = getLHConnectorConfig(EXT_EVT_DEF_NAME_KEY);
            if (externalEventDefName == null) {
                throw new LHMisconfigurationException("If mode = EXTERNAL_EVENT must set externaleventdef.name");
            }
            return Optional.of(externalEventDefName);
        } else {
            return Optional.empty();
        }
    }

    public String getWfSpecName() {
        return "";
    }
}
