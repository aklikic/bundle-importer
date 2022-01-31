package ba.bundleimporter.pipeline.component.publisher;

import akka.japi.Pair;
import akka.stream.javadsl.Flow;
import ba.bundleimporter.datamodel.Bundle;
import ba.bundleimporter.datamodel.BundleOut;
import ba.bundleimporter.kafka.KafkaTopicProducer;
import ba.bundleimporter.pipeline.component.Error;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.util.Either;

public class PublisherFlowImpl<C> implements PublisherFlow{
    private static final Logger logger = LoggerFactory.getLogger(PublisherFlowImpl.class);
    private final KafkaTopicProducer<BundleOut,C> producer;

    public PublisherFlowImpl(KafkaTopicProducer<BundleOut, C> producer) {
        this.producer = producer;
    }

    @Override
    public Flow<Pair<Bundle,C>, Pair<Either<Error, BundleOut>,C>, ?> flow() {
        return Flow.<Pair<Bundle,C>>create()
                .map(bundle->Pair.create(BundleOut.newBuilder().setBundleId(bundle.first().getBundleId()).build(),bundle.second()))
                .map(out->{
                    logger.info("BundleOut: {}",out.first().getBundleId());
                    return out;
                })
                .via(producer.flow());
    }
}
