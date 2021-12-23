package ba.bundleimporter.pipeline;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.stream.javadsl.Flow;
import akka.stream.testkit.TestPublisher;
import akka.stream.testkit.TestSubscriber;
import ba.bundleimporter.datamodel.Bundle;
import ba.bundleimporter.pipeline.component.serialization.SerializationFlowImpl;
import ba.bundleimporter.pipeline.component.validation.ValidationFlowImpl;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class TestPipelineFlow {
    private ActorSystem system = ActorSystem.create("test");

    private Flow<Pair<byte[],NotUsed>, Pair<Done,NotUsed>, NotUsed> pipelineFlow(boolean simulateErrorHandlerError){
        return PipelineFlow.flow(new SerializationFlowImpl<NotUsed>(),new ValidationFlowImpl<NotUsed>(),new MockErrorHandlerFlow<NotUsed>(simulateErrorHandlerError),new MockErrorHandlerFlow<NotUsed>(false));
    }

    @Test
    public void pipelineOk()throws Exception{

        final Pair<TestPublisher.Probe<Pair<byte[],NotUsed>>, TestSubscriber.Probe<Pair<Done,NotUsed>>> pubAndSub =
               TestSupport.testFlow(system,pipelineFlow(false));

        Bundle bundle = TestSupport.getBundle("1");
        byte [] rawMessage = TestSupport.getBundleRawMessage(bundle);

        pubAndSub.second().request(1);
        pubAndSub.first().sendNext(Pair.create(rawMessage,NotUsed.getInstance()));
        pubAndSub.second().expectNext(Pair.create(Done.getInstance(),NotUsed.getInstance()));
    }

    @Test
    public void pipelineSerializationError()throws Exception{
        final Pair<TestPublisher.Probe<Pair<byte[],NotUsed>>, TestSubscriber.Probe<Pair<Done,NotUsed>>> pubAndSub =
                TestSupport.testFlow(system,pipelineFlow(false));

        byte [] rawMessage = TestSupport.getWrongBundleRawMessage();

        pubAndSub.second().request(1);
        pubAndSub.first().sendNext(Pair.create(rawMessage,NotUsed.getInstance()));
        pubAndSub.second().expectNext(Pair.create(Done.getInstance(),NotUsed.getInstance()));
    }

    @Test
    public void pipelineValidationError()throws Exception{
        final Pair<TestPublisher.Probe<Pair<byte[],NotUsed>>, TestSubscriber.Probe<Pair<Done,NotUsed>>> pubAndSub =
                TestSupport.testFlow(system,pipelineFlow(false));

        Bundle bundle = TestSupport.getBundle("one");
        byte [] rawMessage = TestSupport.getBundleRawMessage(bundle);

        pubAndSub.second().request(1);
        pubAndSub.first().sendNext(Pair.create(rawMessage,NotUsed.getInstance()));
        pubAndSub.second().expectNext(Pair.create(Done.getInstance(),NotUsed.getInstance()));
    }

    @Test
    public void pipelineValidationErrorWithInterruptOnErrorToTopic()throws Exception{
        final Pair<TestPublisher.Probe<Pair<byte[],NotUsed>>, TestSubscriber.Probe<Pair<Done,NotUsed>>> pubAndSub =
                TestSupport.testFlow(system,pipelineFlow(true));

        Bundle bundle = TestSupport.getBundle("one");
        byte [] rawMessage = TestSupport.getBundleRawMessage(bundle);

        pubAndSub.second().request(1);
        pubAndSub.first().sendNext(Pair.create(rawMessage,NotUsed.getInstance()));
        pubAndSub.second().expectError();
    }
}
