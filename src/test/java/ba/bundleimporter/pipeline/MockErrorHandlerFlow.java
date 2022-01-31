package ba.bundleimporter.pipeline;

import akka.Done;
import akka.NotUsed;
import akka.japi.Pair;
import akka.stream.javadsl.Flow;
import ba.bundleimporter.pipeline.component.Error;
import ba.bundleimporter.pipeline.component.errorhandler.ErrorHandlerFlow;
import scala.util.Either;
import scala.util.Left;
import scala.util.Right;

public class MockErrorHandlerFlow<C> implements ErrorHandlerFlow {
    private final boolean simulateError;

    //not thread safe but enough for testing
    private boolean simulated = false;

    public MockErrorHandlerFlow(boolean simulateError){
        this.simulateError = simulateError;
    }
    @Override
    public Flow<Pair<Error.BusinessError,C>, Pair<Either<Error,Done>,C>, NotUsed> flow() {
        return
        Flow.<Pair<Error.BusinessError,C>>create()
            .map(error-> {
                if(simulateError && !simulated) {
                    simulated = true;
                    return Pair.create(Left.apply(new Error.InterruptError(new RuntimeException("Simulated error"))), error.second());
                }else {
                    return Pair.create(Right.apply(Done.getInstance()), error.second());
                }
            });

    }
}
