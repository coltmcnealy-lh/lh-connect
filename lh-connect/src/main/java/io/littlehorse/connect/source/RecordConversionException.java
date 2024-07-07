package io.littlehorse.connect.source;

public class RecordConversionException extends RuntimeException {

    public RecordConversionException(String message) {
        super(message);
    }

    public RecordConversionException(String message, Throwable cause) {
        super(message, cause);
    }
}
