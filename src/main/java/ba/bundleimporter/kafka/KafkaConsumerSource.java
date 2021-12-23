package ba.bundleimporter.kafka;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.kafka.*;
import akka.kafka.javadsl.Consumer;
import akka.stream.RestartSettings;
import akka.stream.javadsl.Flow;
import ba.bundleimporter.pipeline.component.Error;
import scala.util.Either;

import java.util.concurrent.atomic.AtomicReference;

public class KafkaConsumerSource {
    private final ActorSystem system;
    private final RestartSettings restartSettings = RestartSettings.create(java.time.Duration.ofSeconds(3),java.time.Duration.ofSeconds(30),0.2);
    private final ConsumerSettings<String,byte[]> consumerSettings;
    private final String topicName;
    private final String consumerGroupId;
    private final int maxPartitions;
    private final CommitterSettings committerSettings;
    private final ActorRef kafkaConsumerMonitor;
    private final AtomicReference<Consumer.Control> control = new AtomicReference<>(Consumer.createNoopControl());
    public KafkaConsumerSource(ActorSystem system,ConsumerSettings<String,byte[]> consumerSettings, String topicName, String consumerGroupId, int maxPartitions,ActorRef kafkaConsumerMonitor){
        this.system = system;
        this.consumerSettings = consumerSettings;
        this.topicName = topicName;
        this.consumerGroupId = consumerGroupId;
        this.maxPartitions = maxPartitions;
        this.committerSettings = CommitterSettings.create(system);
        this.kafkaConsumerMonitor = kafkaConsumerMonitor;
    }
    public void consume(Flow<Pair<byte[],ConsumerMessage.CommittableOffset>, Pair<Either<Done, Error>,ConsumerMessage.CommittableOffset>, NotUsed> flow){
        //ActorRef kafkaPartitionRebalanceListener = system.actorOf(Props.create(KafkaPartitionRebalanceListener.class));
        AutoSubscription subscription = Subscriptions.topics(topicName).withRebalanceListener(kafkaConsumerMonitor);

        /*Consumer.committablePartitionedSource(consumerSettings, subscription)
                .mapAsyncUnordered(
                        maxPartitions,
                        pair-> {
                            Source<ConsumerMessage.CommittableMessage<String, byte[]>, NotUsed> partitionSource = pair.second();
                            return partitionSource
                                    .map(m -> Pair.create(m.record().value(),m.committableOffset()))
                                    .via(flow)
                                    .map(message -> {
                                        //if(message.)
                                    })
                                    .runWith(Committer.sink(committerSettings), system);
                        });

        /*RestartSource.onFailuresWithBackoff(restartSettings,() -> source)
                .toMat(Committer.sink(committerSettings), Keep.both())
                .mapMaterializedValue(pair->
                        pair.second()
                );*/
    }
}
