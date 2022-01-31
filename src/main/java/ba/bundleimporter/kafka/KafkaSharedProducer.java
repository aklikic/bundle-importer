package ba.bundleimporter.kafka;

import akka.actor.ActorSystem;
import akka.kafka.ProducerSettings;
import org.apache.kafka.clients.producer.Producer;

import java.util.concurrent.CompletionStage;

public class KafkaSharedProducer {
    private final ProducerSettings<String, byte[]> producerSettings;

    public KafkaSharedProducer(ProducerSettings<String, byte[]> producerSettings, Producer<String, byte[]> producer) {
        this.producerSettings = producerSettings.withProducer(producer);
    }
    public static CompletionStage<KafkaSharedProducer> create(ActorSystem system, ProducerSettings<String, byte[]> settings){

        return settings.createKafkaProducerCompletionStage(system.dispatcher())
                       .<KafkaSharedProducer>thenApply(producer->new KafkaSharedProducer(settings,producer));
    }

    public ProducerSettings<String, byte[]> producerSettings(){
        return producerSettings;
    }
}
