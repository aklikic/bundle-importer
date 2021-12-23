package ba.bundleimporter.pipeline.component;

import ba.bundleimporter.datamodel.Bundle;
import lombok.Value;

public interface Error {
    @Value
    public static class LogAndSkip implements Error {
        byte[] message;
        String reason;
    }
    @Value
    public static class BusinessError implements Error {
        Bundle message;
        String reason;
    }
    @Value
    public static class InterruptError implements Error {
        RuntimeException throwable;
    }
}
