package ba.bundleimporter.pipeline;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Keep;
import akka.stream.testkit.TestPublisher;
import akka.stream.testkit.TestSubscriber;
import akka.stream.testkit.javadsl.TestSink;
import akka.stream.testkit.javadsl.TestSource;
import ba.bundleimporter.datamodel.Bundle;
import ba.bundleimporter.pipeline.component.serialization.Serializer;

import java.io.IOException;

public class TestSupport {

    public static byte[] getWrongBundleRawMessage(){
        return "some wrong data".getBytes();
    }

    public static Bundle getBundle(String bundleId){
        return Bundle.newBuilder().setBundleId(bundleId).setPdfUrl("url").build();
    }
    public static byte[] getBundleRawMessage(Bundle bundle){
        try {
            return Serializer.serializeBundle(bundle);
        } catch (IOException e) {
            return null;
        }
    }
    public static <I,O> Pair<TestPublisher.Probe<I>, TestSubscriber.Probe<O>> testFlow(ActorSystem system,Flow<I, O, NotUsed> flow){
        return
        TestSource.<I>probe(system)
                .via(flow)
                .toMat(TestSink.<O>probe(system), Keep.both())
                .run(system);
    }
}
