package ba.bundleimporter.pipeline;

import akka.Done;
import akka.actor.ActorSystem;
import akka.kafka.testkit.KafkaTestkitTestcontainersSettings;
import akka.kafka.testkit.internal.TestcontainersKafka;
import akka.stream.Materializer;
import akka.testkit.javadsl.TestKit;
import ba.bundleimporter.datamodel.BundleOut;
import ba.bundleimporter.datamodel.ErrorBundle;
import ba.bundleimporter.pipeline.kafka.KafkaTestEnvironment;
import ba.bundleimporter.pipeline.kafka.KafkaTestEnvironmentFactory;
import org.junit.Test;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.TestInstance;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@TestInstance(TestInstance.Lifecycle.PER_METHOD)
public class IntegrationTestPipelineFlowKafka {

    private static ActorSystem system = ActorSystem.create("test");
    private static Materializer materializer = Materializer.matFromSystem(system);
    private final KafkaTestEnvironmentFactory environmentFactory;

    public IntegrationTestPipelineFlowKafka() throws Exception{
        KafkaTestkitTestcontainersSettings testContainerSettings =
                KafkaTestkitTestcontainersSettings.create(system)
                // .withNumBrokers(3)
                //.withInternalTopicsReplicationFactor(2)
                ;
        String bootstrapServers = TestcontainersKafka.Singleton().startCluster(testContainerSettings);
        System.out.println("bootstrapServers:"+bootstrapServers);
        //DIRECT KAFKA TEST CONTAINERS
        //KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka").withTag("5.4.1")); // contains Kafka 2.4.x
        //kafka.start();
        //bootstrapServers = kafka.getBootstrapServers();

        //EXTERNAL KAFKA (docker compose)
        //bootstrapServers = "PLAINTEXT://localhost:29092";
        environmentFactory = new KafkaTestEnvironmentFactory(system,materializer,bootstrapServers);
    }

    @Test
    public void pipelineOk()throws Exception{
        final boolean simulateError = false;

        KafkaTestEnvironment test = environmentFactory.create();
        test.start(false);

        String bundleId = "1";
        test.bundleInTestProducer.publish(TestSupport.getBundle(bundleId));
        BundleOut bundle = test.bundleOutTestConsumer.getNext();

        assertTrue(bundle!=null);
        assertEquals(bundleId,bundle.getBundleId().toString());

        test.bundleErrorTestConsumer.assertNoMessage();

        bundleId = "2";
        test.bundleInTestProducer.publish(TestSupport.getBundle(bundleId));
        bundle = test.bundleOutTestConsumer.getNext();

        assertTrue(bundle!=null);
        assertEquals(bundleId,bundle.getBundleId().toString());

        test.bundleErrorTestConsumer.assertNoMessage();

        assertTrue(test.stop().toCompletableFuture().get(10,TimeUnit.SECONDS)==Done.getInstance());



    }
    @Test
    public void pipelineSerializationError()throws Exception{
        KafkaTestEnvironment test = environmentFactory.create();
        test.start(false);

        String bundleId = "1";

        test.bundleInTestProducer.publish(TestSupport.getBundle(bundleId));
        BundleOut bundle = test.bundleOutTestConsumer.getNext();

        assertTrue(bundle!=null);
        assertEquals(bundleId,bundle.getBundleId().toString());

        bundleId = "2";
        test.wrongBundleInTestProducer.publish(TestSupport.getBundle(bundleId));

        test.bundleOutTestConsumer.assertNoMessage();
        test.bundleErrorTestConsumer.assertNoMessage();

        bundleId = "3";
        test.bundleInTestProducer.publish(TestSupport.getBundle(bundleId));
        bundle = test.bundleOutTestConsumer.getNext();

        assertTrue(bundle!=null);
        assertEquals(bundleId,bundle.getBundleId().toString());

        assertTrue(test.stop().toCompletableFuture().get(10,TimeUnit.SECONDS)==Done.getInstance());

    }
    @Test
    public void pipelineValidationError()throws Exception{
        pipelineValidationErrorInternal(false);
    }

    @Test
    public void pipelineValidationErrorWithInterruptOnErrorToTopic()throws Exception{
        pipelineValidationErrorInternal(true);
    }

    private void pipelineValidationErrorInternal(boolean simulateInterruptError)throws Exception{
        KafkaTestEnvironment test = environmentFactory.create();
        test.start(simulateInterruptError);

        String bundleId = "1";

        test.bundleInTestProducer.publish(TestSupport.getBundle(bundleId));
        BundleOut bundle = test.bundleOutTestConsumer.getNext();

        assertTrue(bundle!=null);
        assertEquals(bundleId,bundle.getBundleId().toString());

        bundleId = "two";
        test.bundleInTestProducer.publish(TestSupport.getBundle(bundleId));
        test.bundleOutTestConsumer.assertNoMessage();

        Thread.sleep(35000);

        ErrorBundle errorBundle = test.bundleErrorTestConsumer.getNext();
        assertTrue(errorBundle!=null);
        assertEquals(bundleId,errorBundle.getBundleId().toString());

        bundleId = "3";
        test.bundleInTestProducer.publish(TestSupport.getBundle(bundleId));
        bundle = test.bundleOutTestConsumer.getNext();

        assertTrue(bundle!=null);
        assertEquals(bundleId,bundle.getBundleId().toString());

        assertTrue(test.stop().toCompletableFuture().get(10,TimeUnit.SECONDS)==Done.getInstance());
    }


    @AfterAll
    void afterClass() {
        System.out.println("afterClass");
        TestcontainersKafka.Singleton().stopCluster();
        TestKit.shutdownActorSystem(system);
    }
}
