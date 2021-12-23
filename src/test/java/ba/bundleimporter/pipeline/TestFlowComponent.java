package ba.bundleimporter.pipeline;
import static org.junit.Assert.assertTrue;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.stream.testkit.TestPublisher;
import akka.stream.testkit.TestSubscriber;
import ba.bundleimporter.pipeline.component.Error;
import ba.bundleimporter.datamodel.Bundle;
import ba.bundleimporter.pipeline.component.serialization.SerializationFlow;
import ba.bundleimporter.pipeline.component.serialization.SerializationFlowImpl;
import ba.bundleimporter.pipeline.component.validation.ValidationFlow;
import ba.bundleimporter.pipeline.component.validation.ValidationFlowImpl;
import org.junit.Test;
import scala.util.Either;
import scala.util.Left;

public class TestFlowComponent {

    private ActorSystem system = ActorSystem.create("test");
    @Test
    public void serializationOk()throws Exception{

        SerializationFlow<NotUsed> serializationFlow = new SerializationFlowImpl();

        final Pair<TestPublisher.Probe<Pair<byte[],NotUsed>>, TestSubscriber.Probe<Pair<Either<Bundle, Error>,NotUsed>>> pubAndSub
                = TestSupport.testFlow(system,serializationFlow.flow());

        Bundle bundle = TestSupport.getBundle("1");
        byte [] rawMessage = TestSupport.getBundleRawMessage(bundle);
        pubAndSub.second().request(1);
        pubAndSub.first().sendNext(Pair.create(rawMessage,NotUsed.getInstance()));
        pubAndSub.second().expectNext(Pair.create(Left.apply(bundle),NotUsed.getInstance()));
    }
    @Test
    public void serializationError()throws Exception{

        SerializationFlow serializationFlow = new SerializationFlowImpl();

        final Pair<TestPublisher.Probe<Pair<byte[],NotUsed>>, TestSubscriber.Probe<Pair<Either<Bundle, Error>,NotUsed>>> pubAndSub
                = TestSupport.testFlow(system,serializationFlow.flow());

        byte [] rawMessage = TestSupport.getWrongBundleRawMessage();
        pubAndSub.second().request(1);
        pubAndSub.first().sendNext(Pair.create(rawMessage,NotUsed.getInstance()));
        Either<Bundle,Error> next = pubAndSub.second().requestNext().first();
        assertTrue(next.isRight());
        assertTrue(next.right().get() instanceof Error.LogAndSkip);
    }

    @Test
    public void validationOk()throws Exception{

        ValidationFlow<NotUsed> validationFlow = new ValidationFlowImpl();

        final Pair<TestPublisher.Probe<Pair<Bundle,NotUsed>>, TestSubscriber.Probe<Pair<Either<Bundle, Error>,NotUsed>>> pubAndSub
                = TestSupport.testFlow(system,validationFlow.flow());

        Bundle bundle = TestSupport.getBundle("1");

        pubAndSub.second().request(1);
        pubAndSub.first().sendNext(Pair.create(bundle,NotUsed.getInstance()));
        pubAndSub.second().expectNext(Pair.create(Left.apply(bundle),NotUsed.getInstance()));
    }
    @Test
    public void validationError()throws Exception{

        ValidationFlow validationFlow = new ValidationFlowImpl();

        final Pair<TestPublisher.Probe<Pair<Bundle,NotUsed>>, TestSubscriber.Probe<Pair<Either<Bundle, Error>,NotUsed>>> pubAndSub
                = TestSupport.testFlow(system,validationFlow.flow());

        Bundle bundle = TestSupport.getBundle("one");

        pubAndSub.second().request(1);
        pubAndSub.first().sendNext(Pair.create(bundle,NotUsed.getInstance()));
        Either<Bundle,Error> next = pubAndSub.second().requestNext().first();
        assertTrue(next.isRight());
        assertTrue(next.right().get() instanceof Error.BusinessError);
    }


}
