package io.littlehorse.connect.source;

import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import io.littlehorse.sdk.common.LHLibUtil;
import io.littlehorse.sdk.common.proto.VariableValue;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class JsonSourceRecord extends LHSourceRecord {

    private final String jsonContent;
    private final Map<String, String> headers;

    public JsonSourceRecord(String jsonContent, Map<String, String> headers) {
        this.jsonContent = jsonContent;
        this.headers = headers == null ? new HashMap<>() : headers;
    }

    @Override
    public String getHeader(String key) {
        return headers.get(key);
    }

    @Override
    public Optional<VariableValue> getField(String fieldPath) {
        try {
            Object value = JsonPath.parse(jsonContent).read(fieldPath);
            return Optional.of(LHLibUtil.objToVarVal(value));
        } catch (PathNotFoundException exn) {
            return Optional.empty();
        }
    }
}
