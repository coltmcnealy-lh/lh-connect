package io.littlehorse.connectruntime;

import io.littlehorse.connect.source.LHSourceRecord;
import io.littlehorse.connect.source.RecordConversionException;
import io.littlehorse.sdk.common.LHLibUtil;
import io.littlehorse.sdk.common.proto.VariableType;
import io.littlehorse.sdk.common.proto.VariableValue;
import java.util.Optional;

public class ValueMapping {

    private VariableType type;
    private String fieldPath;
    private String headerKey;
    private VariableValue literalValue;

    private ValueMapping(VariableType type, String fieldPath, String headerKey, VariableValue literalValue) {
        this.type = type;
        this.fieldPath = fieldPath;
        this.headerKey = headerKey;
        this.literalValue = literalValue;
    }

    public static ValueMapping fromFieldPath(VariableType type, String fieldPath) {
        return new ValueMapping(type, fieldPath, null, null);
    }

    public static ValueMapping fromHeaderKey(VariableType type, String headerKey) {
        return new ValueMapping(type, null, headerKey, null);
    }

    public static ValueMapping fromLiteral(VariableType type, VariableValue literal) {
        return new ValueMapping(type, null, null, literal);
    }

    public VariableValue fromRecord(LHSourceRecord record) {
        VariableValue out;

        if (fieldPath != null) {
            Optional<VariableValue> value = record.getField(fieldPath);
            if (value.isEmpty()) {
                throw new RecordConversionException("Field %s was not present".formatted(fieldPath));
            }
            out = value.get();
        } else if (headerKey != null) {
            String header = record.getHeader(headerKey);
            if (header != null) {
                out = VariableValue.newBuilder().setStr(header).build();
            } else {
                // NULL
                out = VariableValue.getDefaultInstance();
            }
        } else {
            out = LHLibUtil.objToVarVal(literalValue);
        }

        switch (out.getValueCase()) {
            case INT:
                if (type != VariableType.INT) throw new RecordConversionException("Expected type INT");
                break;
            case DOUBLE:
                if (type != VariableType.DOUBLE) throw new RecordConversionException("Expected type DOUBLE");
                break;
            case JSON_OBJ:
                if (type != VariableType.JSON_OBJ) throw new RecordConversionException("Expected type JSON_OBJ");
                break;
            case JSON_ARR:
                if (type != VariableType.JSON_ARR) throw new RecordConversionException("Expected type JSON_ARR");
                break;
            case STR:
                if (type != VariableType.STR) throw new RecordConversionException("Expected type STR");
                break;
            case BOOL:
                if (type != VariableType.BOOL) throw new RecordConversionException("Expected type BOOL");
                break;
            case BYTES:
                if (type != VariableType.BYTES) throw new RecordConversionException("Expected type BYTES");
                break;
            case VALUE_NOT_SET:
        }
        return out;
    }
}
