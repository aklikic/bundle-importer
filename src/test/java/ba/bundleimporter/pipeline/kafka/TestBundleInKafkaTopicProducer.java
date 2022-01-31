package ba.bundleimporter.pipeline.kafka;

import akka.actor.ActorSystem;
import ba.bundleimporter.datamodel.Bundle;
import ba.bundleimporter.kafka.AbstractKafkaTopicProducer;
import ba.bundleimporter.kafka.KafkaSharedProducer;
import ba.bundleimporter.pipeline.TestSupport;
import ba.bundleimporter.pipeline.component.serialization.Serializer;

import java.io.IOException;
import java.util.function.Function;

public class TestBundleInKafkaTopicProducer<C> extends AbstractKafkaTopicProducer<Bundle, C> {
    private final boolean serializationError;
    public TestBundleInKafkaTopicProducer(ActorSystem system, KafkaSharedProducer sharedProducer, String topicName, Function<String, Integer> partitionExtractor, boolean serializationError) {
        super(system, sharedProducer,topicName,partitionExtractor);
        this.serializationError = serializationError;
    }
    public TestBundleInKafkaTopicProducer(ActorSystem system, KafkaSharedProducer sharedProducer, String topicName, boolean serializationError) {
        super(system, sharedProducer,topicName);
        this.serializationError = serializationError;
    }
    @Override
    protected byte[] serialize(Bundle input) throws IOException {
        //logger.info("serializationError {}: {}",input.getBundleId(),serializationError);
        if(!serializationError)
            return Serializer.serializeBundle(input);
        else
            return TestSupport.getWrongBundleRawMessage();
    }

    @Override
    protected String extractKey(Bundle input) {
        return input.getBundleId().toString();
    }


}
