package ba.bundleimporter.pipeline.kafka;

import akka.actor.ActorSystem;
import akka.kafka.AutoSubscription;
import akka.kafka.ConsumerSettings;
import akka.kafka.Subscriptions;
import akka.kafka.javadsl.Consumer;
import akka.stream.Materializer;
import akka.stream.javadsl.Source;
import akka.stream.testkit.TestSubscriber;
import akka.stream.testkit.javadsl.TestSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.duration.FiniteDuration;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class KafkaConsumerTestkit<O> {

    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerTestkit.class);

    private final static FiniteDuration pullTimeout = FiniteDuration.apply(10, TimeUnit.SECONDS);
    private final static FiniteDuration pullNoMessageTimeout = FiniteDuration.apply(2, TimeUnit.SECONDS);

    private final ActorSystem system;
    private final Materializer materializer;
    private final Source<O,Consumer.Control> source;
    private TestSubscriber.Probe<O> probe;

    public KafkaConsumerTestkit(ActorSystem system, Materializer materializer, ConsumerSettings<String,byte[]> consumerSettings, String topicName, Function<byte[],O> deSerialize){
        this.system = system;
        this.materializer = materializer;
        AutoSubscription subscription = Subscriptions.topics(topicName);
        this.source =  Consumer.plainSource(consumerSettings,subscription)
                        .map(m -> m.value())
                        .map(deSerialize::apply);

    }

    public void start(){
        this.probe =  source.runWith(TestSink.probe(system),materializer);
    }

    public void stop(){
        this.probe.cancel();
        this.probe = null;
    }


    public O getNext(){
        return probe.request(1).requestNext(pullTimeout);
    }

    public void assertNoMessage(){
        probe.request(1).expectNoMessage(pullNoMessageTimeout);
    }

}
