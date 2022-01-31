package ba.bundleimporter.pipeline.kafka;

import akka.NotUsed;
import akka.japi.Pair;
import akka.stream.Materializer;
import akka.stream.OverflowStrategy;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.javadsl.SourceQueueWithComplete;
import ba.bundleimporter.kafka.KafkaTopicProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaProducerTestkit<I> {
    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerTestkit.class);
    private final SourceQueueWithComplete<Pair<I, NotUsed>> source;

    public KafkaProducerTestkit(Materializer materializer, KafkaTopicProducer<I,NotUsed> producer) {
        Source<Pair<I,NotUsed>, SourceQueueWithComplete<Pair<I,NotUsed>>> s = Source.queue(100, OverflowStrategy.backpressure());
        source = s.viaMat(producer.flow(), Keep.left())
                .toMat(Sink.ignore(),Keep.left())
                .run(materializer);
    }

    public void publish(I input){
       // logger.info("publish: {}",input);
        source.offer(Pair.create(input, NotUsed.getInstance()));
                //.thenAccept(res->logger.info("publish res:{}",res));
    }
}
