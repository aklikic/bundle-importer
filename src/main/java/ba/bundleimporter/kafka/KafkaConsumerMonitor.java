package ba.bundleimporter.kafka;

import akka.actor.AbstractLoggingActor;
import akka.kafka.TopicPartitionsAssigned;
import akka.kafka.TopicPartitionsRevoked;

public class KafkaConsumerMonitor extends AbstractLoggingActor {

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(
                        TopicPartitionsAssigned.class,
                        assigned -> {
                            log().info("Assigned: {}", assigned);
                        })
                .match(
                        TopicPartitionsRevoked.class,
                        revoked -> {
                            log().info("Revoked: {}", revoked);
                        })
                .build();
    }
}
