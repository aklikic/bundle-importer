package ba.bundleimporter.kafka;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.japi.pf.PFBuilder;
import akka.kafka.ProducerMessage;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Source;
import ba.bundleimporter.pipeline.component.Error;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.util.Either;
import scala.util.Left;
import scala.util.Right;

import java.io.IOException;
import java.util.function.Function;

public abstract class KafkaTopicProducer<I,C>{
    protected final Logger logger = LoggerFactory.getLogger(getClass());
    private final String topicName;
    private final Function<String, Integer> partitionExtractor;

    public KafkaTopicProducer(ActorSystem system, String topicName, Function<String, Integer> partitionExtractor) {
        this.topicName = topicName;
        this.partitionExtractor = partitionExtractor;
    }

    public Flow<Pair<I,C>, Pair<Either<Error,I>,C>, NotUsed> flow(){

        Flow<ProducerMessage.Envelope<String, byte[], I>, I, NotUsed> producerFlow = producerFlow();
        return
        Flow.<Pair<I,C>>create()
                .<Pair<Either<Error,ProducerMessage.Envelope<String,byte[], I>>,C>>map(input -> {
                    try {
                        byte[] value = serialize(input.first());
                        String key = extractKey(input.first());
                        Integer partition = partitionExtractor.apply(key);
                        ProducerMessage.Envelope<String,byte[], I> pm = ProducerMessage.single(new ProducerRecord<String,byte[]>(topicName,key,value),input.first());
                        //logger.info("to publish {}/{}: {}",topicName,key,input.first());
                        return Pair.create(Right.apply(pm),input.second());
                    }catch(IOException e){
                        return Pair.create(Left.apply(new Error.InterruptError(new RuntimeException(e))),input.second());
                    }
                })
                .flatMapConcat(message->{

                    if(message.first().isLeft())
                        return Source.<Pair<Either<Error,I>,C>>single(Pair.create(Left.apply(message.first().left().get()),message.second()));
                    else{
                        return Source.single(message.first().right().get())
                                .via(producerFlow)
                                /*.map(res->{
                                    logger.info("Res: {}",res);
                                    return res;
                                })*/
                                .<Pair<Either<Error,I>,C>>map(input->Pair.create(Right.apply(input),message.second()))
                                .recover(new PFBuilder<Throwable, Pair<Either<Error,I>,C>>()
                                        .match(RuntimeException.class, e -> Pair.create(Left.apply(new Error.InterruptError(e)), message.second()))
                                        .build());
                    }
                });
    }

    protected abstract Flow<ProducerMessage.Envelope<String, byte[], I>, I, NotUsed> producerFlow();
    protected abstract byte[] serialize(I input) throws IOException;
    protected abstract String extractKey(I input);

}
