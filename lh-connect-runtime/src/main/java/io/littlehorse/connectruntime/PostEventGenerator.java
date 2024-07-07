package io.littlehorse.connectruntime;

import io.littlehorse.connect.source.LHSourceConnectorConfig;
import io.littlehorse.connect.source.LHSourceRecord;
import io.littlehorse.sdk.common.LHLibUtil;
import io.littlehorse.sdk.common.exception.LHMisconfigurationException;
import io.littlehorse.sdk.common.proto.ExternalEventDefId;
import io.littlehorse.sdk.common.proto.PutExternalEventRequest;
import io.littlehorse.sdk.common.proto.VariableType;
import io.littlehorse.sdk.common.proto.VariableValue;
import java.util.Base64;

public class PostEventGenerator {

    private ExternalEventDefId eventDefId;
    private ValueMapping wfRunIdMapping;
    private ValueMapping contentMapping;
    private ValueMapping guidMapping;

    public PostEventGenerator(LHSourceConnectorConfigImpl config) {
        this.eventDefId = ExternalEventDefId.newBuilder()
                .setName(config.getExternalEventDefName().get())
                .build();

        String wfRunIdField = config.getLHConnectorConfig(LHSourceConnectorConfig.WFRUN_ID_FIELD_KEY);
        String wfRunIdHeader = config.getLHConnectorConfig(LHSourceConnectorConfig.WFRUN_ID_HEADER_KEY);
        if (wfRunIdField != null) {
            this.wfRunIdMapping = ValueMapping.fromFieldPath(VariableType.STR, wfRunIdField);
        } else if (wfRunIdHeader != null) {
            this.wfRunIdMapping = ValueMapping.fromHeaderKey(VariableType.STR, wfRunIdHeader);
        } else {
            throw new LHMisconfigurationException("For EXTERNAL_EVENT connectors you must set lhct.%s or lhct.%s"
                    .formatted(
                            LHSourceConnectorConfig.WFRUN_ID_FIELD_KEY, LHSourceConnectorConfig.WFRUN_ID_HEADER_KEY));
        }

        String contentField = config.getLHConnectorConfig(LHSourceConnectorConfig.EXT_EVT_CONTENT_FIELD_KEY);
        String contentHeader = config.getLHConnectorConfig(LHSourceConnectorConfig.EXT_EVT_CONTENT_HEADER_KEY);
        String contentLiteral = config.getLHConnectorConfig(LHSourceConnectorConfig.EXT_EVT_CONTENT_LITERAL_KEY);
        String contentTypeStr = config.getLHConnectorConfig(LHSourceConnectorConfig.EXT_EVT_CONTENT_TYPE_KEY);
        VariableType contentType = contentTypeStr == null ? null : VariableType.valueOf(contentTypeStr);

        if (contentField != null) {
            this.contentMapping = ValueMapping.fromFieldPath(contentType, contentField);
        } else if (contentHeader != null) {
            this.contentMapping = ValueMapping.fromFieldPath(contentType, contentHeader);
        } else if (contentLiteral != null) {
            VariableValue literalValue = null;
            switch (contentType) {
                case INT:
                    literalValue = LHLibUtil.objToVarVal(Integer.valueOf(contentLiteral));
                    break;
                case BOOL:
                    literalValue = LHLibUtil.objToVarVal(Boolean.valueOf(contentLiteral));
                    break;
                case BYTES:
                    literalValue = LHLibUtil.objToVarVal(Base64.getDecoder().decode(contentLiteral));
                    break;
                case DOUBLE:
                    literalValue = LHLibUtil.objToVarVal(Double.valueOf(contentLiteral));
                    break;
                case STR:
                    literalValue = LHLibUtil.objToVarVal(contentLiteral);
                    break;
                case JSON_ARR:
                    literalValue = VariableValue.newBuilder()
                            .setJsonArr(contentLiteral)
                            .build();
                    break;
                case JSON_OBJ:
                    literalValue = VariableValue.newBuilder()
                            .setJsonObj(contentLiteral)
                            .build();
                    break;
                case UNRECOGNIZED:
            }
            if (literalValue == null) {
                throw new LHMisconfigurationException("Unrecognized variable type");
            }
            this.contentMapping = ValueMapping.fromLiteral(contentType, literalValue);
        }
    }

    public PutExternalEventRequest toRequest(LHSourceRecord record) {
        PutExternalEventRequest.Builder builder = PutExternalEventRequest.newBuilder()
                .setExternalEventDefId(eventDefId)
                .setWfRunId(LHLibUtil.wfRunIdFromString(
                        wfRunIdMapping.fromRecord(record).getStr()));

        if (guidMapping != null) {
            builder.setGuid(guidMapping.fromRecord(record).getStr());
        }
        if (contentMapping != null) {
            builder.setContent(contentMapping.fromRecord(record));
        }
        return builder.build();
    }
}
