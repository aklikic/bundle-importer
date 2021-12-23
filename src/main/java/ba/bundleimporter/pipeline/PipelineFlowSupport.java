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

    public static <I,O,C> Source<Pair<Either<O, Error>,C>, NotUsed> applyFlowConditional(Flow<Pair<I,C>,Pair<Either<O,Error>,C>,NotUsed> flow, Pair<Either<I,Error>,C> message){
        if(message.first().isLeft()){
            return
                    Source.single(Pair.create(message.first().left().get(),message.second()))
                            .via(flow);
        }else{
            return Source.single(Pair.create(Right.apply(message.first().right().get()),message.second()));
        }
    }

    public static <I,C> Source<Pair<Either<I,Error>,C>,NotUsed> applyErrorFlow(Flow<Pair<Error.BusinessError,C>, Pair<Either<Done,Error>,C>, NotUsed> flow, Pair<Either<I,Error>,C> message){
        if(message.first().isLeft()){
            return
                    Source.single(Pair.create(Left.apply(message.first().left().get()),message.second()));
        }else{
            if(message.first().right().get() instanceof Error.BusinessError)
                return Source.single(Pair.create((Error.BusinessError)message.first().right().get(),message.second()))
                        .via(flow)
                        .map(m->{
                            if(m.first().isLeft())
                                return Pair.create(Right.apply(message.first().right().get()),message.second());
                            else
                                return Pair.create(Right.apply(m.first().right().get()),message.second());
                        });
            else
                return Source.single(Pair.create(Right.apply(message.first().right().get()),message.second()));
        }
    }

    public static <I,C> Pair<Done,C> resultHandler(Pair<Either<I,Error>,C> res){
        if(res.first().isLeft()){
            return Pair.create(Done.getInstance(),res.second());
        }else{
            Error error = res.first().right().get();
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
