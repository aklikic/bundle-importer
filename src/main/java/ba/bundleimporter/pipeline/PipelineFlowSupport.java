package ba.bundleimporter.pipeline;

import akka.Done;
import akka.NotUsed;
import akka.japi.Pair;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Source;
import ba.bundleimporter.pipeline.component.Error;
import scala.util.Either;
import scala.util.Left;
import scala.util.Right;

public class PipelineFlowSupport {

    public static <I,O,C> Source<Pair<Either<Error,O>,C>, NotUsed> applyFlowConditional(Flow<Pair<I,C>,Pair<Either<Error,O>,C>,NotUsed> flow, Pair<Either<Error,I>,C> message){
        if(message.first().isRight()){
            return
                    Source.single(Pair.create(message.first().right().get(),message.second()))
                            .via(flow);
        }else{
            return Source.single(Pair.create(Left.apply(message.first().left().get()),message.second()));
        }
    }

    public static <I,C> Source<Pair<Either<Error,I>,C>,NotUsed> applyErrorFlow(Flow<Pair<Error.BusinessError,C>, Pair<Either<Error,Done>,C>, NotUsed> flow,
                                                                               Pair<Either<Error,I>,C> message){
        if(message.first().isRight()){
            return
                    Source.single(Pair.create(Right.apply(message.first().right().get()),message.second()));
        }else{
            if(message.first().left().get() instanceof Error.BusinessError)
                return Source.single(Pair.create((Error.BusinessError)message.first().left().get(),message.second()))
                        .via(flow)
                        .map(m->{
                            if(m.first().isRight())
                                return Pair.create(Left.apply(message.first().left().get()),message.second());
                            else
                                return Pair.create(Left.apply(m.first().left().get()),message.second());
                        });
            else
                return Source.single(Pair.create(Left.apply(message.first().left().get()),message.second()));
        }
    }

    public static <I,C> Pair<Done,C> resultHandler(Pair<Either<Error,I>,C> res){
        if(res.first().isRight()){
            return Pair.create(Done.getInstance(),res.second());
        }else{
            Error error = res.first().left().get();
            if(error instanceof  Error.LogAndSkip)
                return Pair.create(Done.getInstance(),res.second());
            else if(error instanceof Error.BusinessError)
                return Pair.create(Done.getInstance(),res.second());
            else if(error instanceof Error.InterruptError)
                throw ((Error.InterruptError)error).getThrowable();
            else
                throw new RuntimeException("Unhandled error type: "+error.getClass());
        }
    }
}
