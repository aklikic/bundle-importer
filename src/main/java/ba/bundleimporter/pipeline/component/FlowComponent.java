package ba.bundleimporter.pipeline.component;

import akka.NotUsed;
import akka.japi.Pair;
import akka.stream.javadsl.Flow;
import scala.util.Either;

public interface FlowComponent<I,O,C> {

    Flow<Pair<I,C>, Pair<Either<Error,O>,C>, NotUsed> flow();
}
