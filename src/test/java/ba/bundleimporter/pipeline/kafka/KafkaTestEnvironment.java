package ba.bundleimporter.pipeline.kafka;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.kafka.CommitterSettings;
import akka.kafka.ConsumerMessage;
import akka.kafka.ConsumerSettings;
import akka.stream.Materializer;
import akka.stream.RestartSettings;
import akka.stream.javadsl.Flow;
import ba.bundleimporter.datamodel.Bundle;
import ba.bundleimporter.datamodel.BundleOut;
import ba.bundleimporter.datamodel.ErrorBundle;
import ba.bundleimporter.kafka.KafkaSharedProducer;
import ba.bundleimporter.kafka.KafkaTopicConsumer;
import ba.bundleimporter.kafka.KafkaTopicProducer;
import ba.bundleimporter.pipeline.MockErrorHandlerFlow;
import ba.bundleimporter.pipeline.PipelineFlow;
import ba.bundleimporter.pipeline.component.Error;
import ba.bundleimporter.pipeline.component.errorhandler.BusinessErrorKafkaTopicProducer;
import ba.bundleimporter.pipeline.component.errorhandler.KafkaTopicErrorHandlerFlow;
import ba.bundleimporter.pipeline.component.publisher.BundleOutKafkaTopicProducer;
import ba.bundleimporter.pipeline.component.publisher.PublisherFlowImpl;
import ba.bundleimporter.pipeline.component.serialization.SerializationFlowImpl;
import ba.bundleimporter.pipeline.component.serialization.Serializer;
import ba.bundleimporter.pipeline.component.validation.ValidationFlowImpl;
import com.typesafe.config.Config;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

public class KafkaTestEnvironment {

    private final ConsumerSettings testConsumerSettings;
    private final String bundleInTopicName;

    private final Function<byte[],BundleOut> bundleOutDeSerialize = m -> {
        try {
            return Serializer.deSerializeBundleOut(m);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    };
    private final Function<byte[], ErrorBundle> errorBundleDeSerialize = m -> {
        try {
            String s = new String(m);
            return Serializer.deSerializeErrorBundle(m);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    };

    public final KafkaConsumerTestkit<BundleOut> bundleOutTestConsumer;
    public final KafkaConsumerTestkit<ErrorBundle> bundleErrorTestConsumer;

    private final KafkaTopicProducer<BundleOut, ConsumerMessage.CommittableOffset> bundleOutProducer;
    private final KafkaTopicProducer<Error.BusinessError,ConsumerMessage.CommittableOffset> bundleErrorProducer;

    public final KafkaProducerTestkit<Bundle> bundleInTestProducer;
    public final KafkaProducerTestkit<Bundle> wrongBundleInTestProducer;

    private final int maxKafkaPartition;

    private final KafkaTopicConsumer consumer;

    public KafkaTestEnvironment(ActorSystem system, Materializer materializer, Config consumerConfig, String bootstrapServers, KafkaSharedProducer kafkaSharedProducer, int maxKafkaPartition, String consumerGroupId, String testConsumerGroupId, String bundleInTopicName, String bundleOutTopicName, String bundleErrorTopicName) {

        this.maxKafkaPartition = maxKafkaPartition;
        this.testConsumerSettings = ConsumerSettings.create(consumerConfig, new StringDeserializer(), new ByteArrayDeserializer())
                .withGroupId(testConsumerGroupId)
                .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                .withBootstrapServers(bootstrapServers);
        this.bundleInTopicName = bundleInTopicName;

        RestartSettings restartSettings=RestartSettings.create(Duration.ofSeconds(1),Duration.ofSeconds(10),0.4);
        CommitterSettings committerSettings=CommitterSettings.create(system);

        bundleOutTestConsumer = new KafkaConsumerTestkit(system,materializer,testConsumerSettings, bundleOutTopicName, bundleOutDeSerialize);
        bundleErrorTestConsumer = new KafkaConsumerTestkit(system,materializer,testConsumerSettings,bundleErrorTopicName, errorBundleDeSerialize);

        KafkaTopicProducer<Bundle, NotUsed> bundleInProducer = new TestBundleInKafkaTopicProducer(system,kafkaSharedProducer,bundleInTopicName,false);
        KafkaTopicProducer<Bundle, NotUsed> wrongBundleInProducer = new TestBundleInKafkaTopicProducer(system,kafkaSharedProducer,bundleInTopicName,true);

        bundleInTestProducer = new KafkaProducerTestkit(materializer,bundleInProducer);
        wrongBundleInTestProducer = new KafkaProducerTestkit(materializer,wrongBundleInProducer);


        ConsumerSettings consumerSettings = ConsumerSettings.create(consumerConfig, new StringDeserializer(), new ByteArrayDeserializer())
                .withGroupId(consumerGroupId)
                .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                .withBootstrapServers(bootstrapServers);

        bundleOutProducer = new BundleOutKafkaTopicProducer(system,kafkaSharedProducer,bundleOutTopicName, key->key.hashCode()%maxKafkaPartition);
        bundleErrorProducer = new BusinessErrorKafkaTopicProducer(system,kafkaSharedProducer,bundleErrorTopicName, key->key.hashCode()%maxKafkaPartition);

        consumer = new KafkaTopicConsumer(system,materializer,restartSettings,consumerSettings,committerSettings);
    }

    public void start(boolean simulateInterruptError){
        start(simulateInterruptError,true);
    }
    public void start(boolean simulateInterruptError, boolean consumerPerPartition){
        bundleOutTestConsumer.start();
        bundleErrorTestConsumer.start();
        if(consumerPerPartition)
            consumer.runConsumerPerPartition(pipelineFlow(simulateInterruptError,bundleOutProducer,bundleErrorProducer),bundleInTopicName,maxKafkaPartition);
        else
            consumer.runConsumer(pipelineFlow(simulateInterruptError,bundleOutProducer,bundleErrorProducer),bundleInTopicName);
    }

    public CompletionStage<Done> stop(){
        bundleOutTestConsumer.stop();
        bundleErrorTestConsumer.stop();
        return consumer.stop();
    }

    private Flow<Pair<byte[], ConsumerMessage.CommittableOffset>, Pair<Done,ConsumerMessage.CommittableOffset>, NotUsed> pipelineFlow(
            boolean simulateErrorHandlerError,
            KafkaTopicProducer<BundleOut, ConsumerMessage.CommittableOffset> bundleOutTopicProducer,
            KafkaTopicProducer<Error.BusinessError, ConsumerMessage.CommittableOffset> errorTopicProducer){
        return PipelineFlow.flow(new SerializationFlowImpl<ConsumerMessage.CommittableOffset>(),
                new ValidationFlowImpl<ConsumerMessage.CommittableOffset>(),
                new PublisherFlowImpl<ConsumerMessage.CommittableOffset>(bundleOutTopicProducer),
                new KafkaTopicErrorHandlerFlow<ConsumerMessage.CommittableOffset>(errorTopicProducer),
                new MockErrorHandlerFlow<NotUsed>(simulateErrorHandlerError));
    }


}
