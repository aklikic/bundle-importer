package ba.bundleimporter.pipeline.kafka;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.kafka.ProducerMessage;
import akka.stream.javadsl.Flow;
import ba.bundleimporter.datamodel.Bundle;
import ba.bundleimporter.datamodel.BundleOut;
import ba.bundleimporter.kafka.KafkaTopicProducer;
import ba.bundleimporter.pipeline.component.serialization.Serializer;

import java.io.IOException;

public class KafkaTopicProducerMock<C> extends KafkaTopicProducer<BundleOut,C> {

    public KafkaTopicProducerMock(ActorSystem system, String topicName) {
        super(system, topicName,key->null);
    }

    @Override
    protected Flow<ProducerMessage.Envelope<String, byte[], BundleOut>, BundleOut, NotUsed> producerFlow() {
        return Flow.<ProducerMessage.Envelope<String, byte[], BundleOut>>create()
                .map(message->message.passThrough());
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
