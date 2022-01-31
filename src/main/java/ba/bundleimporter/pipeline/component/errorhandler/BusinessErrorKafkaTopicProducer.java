package ba.bundleimporter.pipeline.component.errorhandler;

import akka.actor.ActorSystem;
import ba.bundleimporter.datamodel.ErrorBundle;
import ba.bundleimporter.kafka.AbstractKafkaTopicProducer;
import ba.bundleimporter.kafka.KafkaSharedProducer;
import ba.bundleimporter.pipeline.component.Error;
import ba.bundleimporter.pipeline.component.serialization.Serializer;

import java.io.IOException;
import java.util.function.Function;

public class BusinessErrorKafkaTopicProducer<C> extends AbstractKafkaTopicProducer<Error.BusinessError, C> {

    public BusinessErrorKafkaTopicProducer(ActorSystem system, KafkaSharedProducer sharedProducer, String topicName, Function<String, Integer> partitionExtractor) {
        super(system, sharedProducer,topicName,partitionExtractor);
    }
    public BusinessErrorKafkaTopicProducer(ActorSystem system, KafkaSharedProducer sharedProducer, String topicName) {
        super(system, sharedProducer,topicName);
    }
    @Override
    protected byte[] serialize(Error.BusinessError input) throws IOException {
        ErrorBundle errorBundle = ErrorBundle.newBuilder().setBundleId(input.getMessage().getBundleId()).setReason(input.getReason()).build();
        return Serializer.serializeErrorBundle(errorBundle);
    }

    @Override
    protected String extractKey(Error.BusinessError input) {
        return input.getMessage().getBundleId().toString();
    }


}
