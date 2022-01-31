package ba.bundleimporter.pipeline.component.errorhandler;

import akka.Done;
import akka.NotUsed;
import akka.japi.Pair;
import akka.stream.javadsl.Flow;
import ba.bundleimporter.kafka.KafkaTopicProducer;
import ba.bundleimporter.pipeline.component.Error;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.util.Either;
import scala.util.Left;
import scala.util.Right;

public class KafkaTopicErrorHandlerFlow<C> implements ErrorHandlerFlow {
    private static final Logger logger = LoggerFactory.getLogger(KafkaTopicErrorHandlerFlow.class);
    private final KafkaTopicProducer<Error.BusinessError,C> topicProducer;

    public KafkaTopicErrorHandlerFlow(KafkaTopicProducer<Error.BusinessError, C> topicProducer) {
        this.topicProducer = topicProducer;
    }

    @Override
    public Flow<Pair<Error.BusinessError,C>, Pair<Either<Error, Done>,C>, NotUsed> flow() {
        return
        Flow.<Pair<Error.BusinessError,C>>create()
                .map(error->{
                    logger.info("BundleError: {}",error.first().getMessage().getBundleId());
                    return error;
                })
                .via(topicProducer.flow())
                .<Pair<Either<Error, Done>,C>>map(res->{
                    if(res.first().isLeft())
                        return Pair.create(Left.apply(res.first().left().get()),res.second());
                    else
                        return Pair.create(Right.apply(Done.getInstance()),res.second());
                });
    }
}
