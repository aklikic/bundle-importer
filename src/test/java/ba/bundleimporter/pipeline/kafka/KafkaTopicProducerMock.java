package ba.bundleimporter.pipeline.kafka;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.kafka.ProducerMessage;
import akka.stream.javadsl.Flow;
import ba.bundleimporter.datamodel.Bundle;
import ba.bundleimporter.kafka.KafkaTopicProducer;
import ba.bundleimporter.pipeline.component.serialization.Serializer;

import java.io.IOException;

public class KafkaTopicProducerMock<C> extends KafkaTopicProducer<Bundle,C> {

    public KafkaTopicProducerMock(ActorSystem system, String topicName) {
        super(system, topicName,key->null);
    }

    @Override
    protected Flow<ProducerMessage.Envelope<String, byte[], Bundle>, Bundle, NotUsed> producerFlow() {
        return Flow.<ProducerMessage.Envelope<String, byte[], Bundle>>create()
                .map(message->message.passThrough());
    }

    @Override
    protected byte[] serialize(Bundle input) throws IOException {
        return Serializer.serializeBundle(input);
    }

    @Override
    protected String extractKey(Bundle input) {
        return input.getBundleId().toString();
    }



}
