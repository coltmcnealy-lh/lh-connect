package io.littlehorse.connectruntime;

import io.littlehorse.connect.source.LHSourceConnectorConfig;
import io.littlehorse.connect.source.LHSourceRecord;
import io.littlehorse.sdk.common.LHLibUtil;
import io.littlehorse.sdk.common.exception.LHMisconfigurationException;
import io.littlehorse.sdk.common.proto.RunWfRequest;
import io.littlehorse.sdk.common.proto.VariableType;
import io.littlehorse.sdk.common.proto.VariableValue;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RunWfGenerator {

    private String wfSpecName;
    private Integer majorVersion;
    private Integer revision;

    private ValueMapping wfRunIdMapper;
    private Map<String, ValueMapping> variableMappers;

    public RunWfGenerator(LHSourceConnectorConfigImpl config) {
        this.variableMappers = new HashMap<>();

        // Set the WfSpec Configurations
        this.wfSpecName = config.getLHConnectorConfig(LHSourceConnectorConfig.WF_SPEC_NAME_KEY);

        String majorVersionStr = config.getLHConnectorConfig(LHSourceConnectorConfig.WF_SPEC_MAJOR_VERSION_KEY);
        this.majorVersion = majorVersionStr == null ? null : Integer.valueOf(majorVersionStr);

        String revisionStr = config.getLHConnectorConfig(LHSourceConnectorConfig.WF_SPEC_REVISION_KEY);
        this.revision = revisionStr == null ? null : Integer.valueOf(revisionStr);

        // Set the WfRunId Provider
        String wfRunFieldPath = config.getLHConnectorConfig(LHSourceConnectorConfig.WFRUN_ID_FIELD_KEY);
        String wfRunHeader = config.getLHConnectorConfig(LHSourceConnectorConfig.WFRUN_ID_HEADER_KEY);
        if (wfRunFieldPath != null) {
            this.wfRunIdMapper = ValueMapping.fromFieldPath(VariableType.STR, wfRunFieldPath);
        } else if (wfRunHeader != null) {
            this.wfRunIdMapper = ValueMapping.fromFieldPath(VariableType.STR, wfRunHeader);
        } else {
            log.info("Not specifying wfrunid; therefore idempotency is not enabled!");
        }

        // Determine which variables we are setting
        String variableCfg = config.getLHConnectorConfig(LHSourceConnectorConfig.VARIABLES_KEY);
        List<String> variables = variableCfg == null ? List.of() : List.of(variableCfg.split(","));

        // Create ValueMapping's for each Variable
        for (String variable : variables) {
            String varTypeStr = config.getVarConfig(LHSourceConnectorConfig.VARIABLE_TYPE_KEY, variable);
            if (varTypeStr == null) {
                throw new LHMisconfigurationException("Must provide type config for variable " + variable);
            }
            VariableType type = VariableType.valueOf(varTypeStr);

            String fieldPath = config.getVarConfig(LHSourceConnectorConfig.VARIABLE_FIELD_KEY, variable);
            String header = config.getVarConfig(LHSourceConnectorConfig.VARIABLE_HEADER_KEY, variable);
            String literal = config.getVarConfig(LHSourceConnectorConfig.VARIABLE_LITERAL_KEY, variable);
            if (fieldPath != null) {
                variableMappers.put(variable, ValueMapping.fromFieldPath(type, variableCfg));
            } else if (header != null) {
                variableMappers.put(variable, ValueMapping.fromHeaderKey(type, header));
            } else if (literal != null) {
                VariableValue literalValue = null;
                switch (type) {
                    case INT:
                        literalValue = LHLibUtil.objToVarVal(Integer.valueOf(literal));
                        break;
                    case BOOL:
                        literalValue = LHLibUtil.objToVarVal(Boolean.valueOf(literal));
                        break;
                    case BYTES:
                        literalValue = LHLibUtil.objToVarVal(Base64.getDecoder().decode(literal));
                        break;
                    case DOUBLE:
                        literalValue = LHLibUtil.objToVarVal(Double.valueOf(literal));
                        break;
                    case STR:
                        literalValue = LHLibUtil.objToVarVal(literal);
                        break;
                    case JSON_ARR:
                        literalValue =
                                VariableValue.newBuilder().setJsonArr(literal).build();
                        break;
                    case JSON_OBJ:
                        literalValue =
                                VariableValue.newBuilder().setJsonObj(literal).build();
                        break;
                    case UNRECOGNIZED:
                }
                if (literalValue == null) {
                    throw new LHMisconfigurationException("Unrecognized variable type");
                }
                variableMappers.put(variable, ValueMapping.fromLiteral(type, literalValue));
            }
        }
    }

    public RunWfRequest toRequest(LHSourceRecord record) {
        RunWfRequest.Builder builder = RunWfRequest.newBuilder().setWfSpecName(wfSpecName);

        if (majorVersion != null) builder.setMajorVersion(majorVersion);
        if (revision != null) builder.setRevision(revision);

        if (wfRunIdMapper != null)
            builder.setId(wfRunIdMapper.fromRecord(record).getStr());

        for (Map.Entry<String, ValueMapping> mapper : variableMappers.entrySet()) {
            builder.putVariables(mapper.getKey(), mapper.getValue().fromRecord(record));
        }

        return builder.build();
    }
}
