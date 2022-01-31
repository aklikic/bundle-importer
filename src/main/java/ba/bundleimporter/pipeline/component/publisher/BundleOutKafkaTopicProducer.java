package ba.bundleimporter.pipeline.component.publisher;

import akka.actor.ActorSystem;
import ba.bundleimporter.datamodel.BundleOut;
import ba.bundleimporter.kafka.AbstractKafkaTopicProducer;
import ba.bundleimporter.kafka.KafkaSharedProducer;
import ba.bundleimporter.pipeline.component.serialization.Serializer;

import java.io.IOException;
import java.util.function.Function;

public class BundleOutKafkaTopicProducer<C> extends AbstractKafkaTopicProducer<BundleOut, C> {

    public BundleOutKafkaTopicProducer(ActorSystem system, KafkaSharedProducer sharedProducer, String topicName, Function<String, Integer> partitionExtractor) {
        super(system, sharedProducer,topicName,partitionExtractor);
    }
    public BundleOutKafkaTopicProducer(ActorSystem system, KafkaSharedProducer sharedProducer, String topicName) {
        super(system, sharedProducer,topicName);
    }
    @Override
    protected byte[] serialize(BundleOut input) throws IOException {
        return Serializer.serializeBundleOut(input);
    }

    @Override
    protected String extractKey(BundleOut input) {
        return input.getBundleId().toString();
    }


}
