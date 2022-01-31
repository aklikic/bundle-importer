package ba.bundleimporter.kafka;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.kafka.ProducerMessage;
import akka.kafka.ProducerSettings;
import akka.kafka.javadsl.Producer;
import akka.stream.javadsl.Flow;

import java.util.function.Function;

public abstract class AbstractKafkaTopicProducer<I,C> extends KafkaTopicProducer<I, C>{
    private final ProducerSettings<String,byte[]> producerSettings;

    public AbstractKafkaTopicProducer(ActorSystem system, KafkaSharedProducer sharedProducer, String topicName, Function<String, Integer> partitionExtractor) {
        super(system, topicName,partitionExtractor);
        this.producerSettings = sharedProducer.producerSettings();
    }
    public AbstractKafkaTopicProducer(ActorSystem system, KafkaSharedProducer sharedProducer, String topicName) {
        super(system, topicName,key->null);
        this.producerSettings = sharedProducer.producerSettings();
    }

    @Override
    protected Flow<ProducerMessage.Envelope<String, byte[], I>, I, NotUsed> producerFlow() {
        return Producer.<String,byte[],I>flexiFlow(producerSettings)
                .map(res -> res.passThrough());
    }
}
