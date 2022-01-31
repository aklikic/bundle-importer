package ba.bundleimporter.pipeline.kafka;

import akka.actor.ActorSystem;
import akka.kafka.ProducerSettings;
import akka.stream.Materializer;
import ba.bundleimporter.kafka.KafkaSharedProducer;
import com.typesafe.config.Config;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.UUID;
import java.util.concurrent.CompletionStage;

public class KafkaTestEnvironmentFactory {

    private static final String bundleInKafkaTopicName="bundleIn";
    private static final String bundleOutKafkaTopicName="bundleOut";
    private static final String bundleErrorKafkaTopicName="bundleError";
    private static String kafkaConsumerGroupId= "groupId";
    private static String kafkaConsumerTestGroupId= "testGroupId";
    private static int maxKafkaPartition= 10;

    private final ActorSystem system ;
    private final Materializer materializer ;
    private final String bootstrapServers;
    private final KafkaSharedProducer kafkaSharedProducer;

    public KafkaTestEnvironmentFactory(ActorSystem system, Materializer materializer, String bootstrapServers) throws Exception{
        this.system = system;
        this.materializer = materializer;
        this.bootstrapServers = bootstrapServers;
        Config producerConfig = system.settings().config().getConfig("my-producer");
        ProducerSettings producerSettings = ProducerSettings.create(producerConfig,new StringSerializer(), new ByteArraySerializer())
                .withBootstrapServers(bootstrapServers);
        CompletionStage<KafkaSharedProducer> kafkaSharedProducerCs = KafkaSharedProducer.create(system,producerSettings);
        kafkaSharedProducer = kafkaSharedProducerCs.toCompletableFuture().get();

    }

    public KafkaTestEnvironment create(){
        Config consumerConfig = system.settings().config().getConfig("my-consumer");
        String bundleInTopicName = bundleInKafkaTopicName+UUID.randomUUID().toString();
        String bundleOutTopicName = bundleOutKafkaTopicName+UUID.randomUUID().toString();;
        String bundleErrorTopicName = bundleErrorKafkaTopicName+UUID.randomUUID().toString();

        return new KafkaTestEnvironment(system,materializer,consumerConfig,bootstrapServers,kafkaSharedProducer,maxKafkaPartition,kafkaConsumerGroupId,kafkaConsumerTestGroupId,bundleInTopicName,bundleOutTopicName,bundleErrorTopicName);
    }



}
