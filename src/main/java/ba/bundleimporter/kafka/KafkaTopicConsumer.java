package ba.bundleimporter.kafka;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.kafka.*;
import akka.kafka.javadsl.Committer;
import akka.kafka.javadsl.Consumer;
import akka.stream.Materializer;
import akka.stream.OverflowStrategy;
import akka.stream.RestartSettings;
import akka.stream.javadsl.*;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;

@Getter
public class KafkaTopicConsumer {
    private static final Logger logger = LoggerFactory.getLogger(KafkaTopicConsumer.class);
    private final ActorSystem system;
    private final Materializer materializer;
    private final RestartSettings restartSettings;
    private final ConsumerSettings<String,byte[]> consumerSettings;
    private final CommitterSettings committerSettings;
    //private Pair<Consumer.Control,CompletionStage<Done>> runStream;
    private CompletionStage<Done> runStream;
    private final AtomicReference<Consumer.Control> control = new AtomicReference<>(Consumer.createNoopControl());

    public KafkaTopicConsumer(ActorSystem system,
                              Materializer materializer,
                              RestartSettings restartSettings,
                              ConsumerSettings<String, byte[]> consumerSettings,
                              CommitterSettings committerSettings) {
        this.system = system;
        this.materializer = materializer;
        this.restartSettings = restartSettings;
        this.consumerSettings = consumerSettings;
        this.committerSettings = committerSettings;

    }

    public CompletionStage<Done> runFromSource(Flow<Pair<byte[],ConsumerMessage.CommittableOffset>, Pair<Done,ConsumerMessage.CommittableOffset>, NotUsed> businessFlow,Source<ConsumerMessage.CommittableMessage<String, byte[]>,NotUsed> source){
        return runSourceWithBackoff(source,businessFlow);
    }

    public CompletionStage<Done> runConsumer(Flow<Pair<byte[],ConsumerMessage.CommittableOffset>, Pair<Done,ConsumerMessage.CommittableOffset>, NotUsed> businessFlow, String topicName){

        AutoSubscription subscription = Subscriptions.topics(topicName);
        Source<ConsumerMessage.CommittableMessage<String,byte[]>, NotUsed> source =
                Consumer.committableSource(consumerSettings,subscription)
                        .mapMaterializedValue(c -> {
                            control.set(c);
                            return NotUsed.getInstance();
                        });
        runStream = runSourceWithBackoff(source,businessFlow);
        return runStream;

    }

    public CompletionStage<Done> runConsumerPerPartition(Flow<Pair<byte[],ConsumerMessage.CommittableOffset>, Pair<Done,ConsumerMessage.CommittableOffset>, NotUsed> businessFlow, String topicName, int maxPartitions){

        AutoSubscription subscription = Subscriptions.topics(topicName);
        Pair<Consumer.Control,CompletionStage<Done>> p =
        Consumer.committablePartitionedSource(consumerSettings, subscription)
                .mapAsyncUnordered(
                        maxPartitions,
                        pair-> runSourceWithBackoff(pair.second(),businessFlow)
                ).toMat(Sink.ignore(),Keep.both())
                .run(materializer);
        control.set(p.first());
        runStream=p.second();
        return runStream;
    }

    public CompletionStage<Done> stop(){
        /*runStream.first().drainAndShutdown(runStream.second(),materializer.executionContext());
        return runStream.second();*/
        if(runStream!=null){
            control.get().drainAndShutdown(runStream, materializer.executionContext());
            return runStream;
        }
        return CompletableFuture.completedFuture(Done.getInstance());

    }

    private CompletionStage<Done> runSourceWithBackoff(Source<ConsumerMessage.CommittableMessage<String,byte[]>, NotUsed> source, Flow<Pair<byte[],ConsumerMessage.CommittableOffset>, Pair<Done,ConsumerMessage.CommittableOffset>, NotUsed> businessFlow){
        return
        handleSourceWithBackoff(source,businessFlow)
                .toMat(Committer.sink(committerSettings.withMaxBatch(1)), Keep.both())
                .mapMaterializedValue(Pair::second)
                .run(materializer);
    }

    private Source<ConsumerMessage.CommittableOffset, NotUsed> handleSourceWithBackoff(Source<ConsumerMessage.CommittableMessage<String,byte[]>, NotUsed> source, Flow<Pair<byte[],ConsumerMessage.CommittableOffset>, Pair<Done,ConsumerMessage.CommittableOffset>, NotUsed> businessFlow){

        return RestartSource.onFailuresWithBackoff(restartSettings,() -> handleSource(source,businessFlow));
    }

    private Source<ConsumerMessage.CommittableOffset, NotUsed> handleSource(Source<ConsumerMessage.CommittableMessage<String,byte[]>, NotUsed> source,
                                                                            Flow<Pair<byte[],ConsumerMessage.CommittableOffset>, Pair<Done,ConsumerMessage.CommittableOffset>, NotUsed> businessFlow) {
        logger.info("handleSourceWithBackoff");
        return source
                .buffer(1, OverflowStrategy.backpressure())
                .map(m -> Pair.create(m.record().value(),m.committableOffset()))
                .via(businessFlow)
                .map(Pair::second);
    }
}
