package ba.bundleimporter.pipeline.component.serialization;

import akka.NotUsed;
import akka.japi.Pair;
import akka.stream.javadsl.Flow;
import ba.bundleimporter.datamodel.Bundle;
import ba.bundleimporter.pipeline.component.Error;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.util.Either;
import scala.util.Left;
import scala.util.Right;

import java.io.IOException;

public class SerializationFlowImpl<C> implements SerializationFlow{
    public static Logger log = LoggerFactory.getLogger(SerializationFlowImpl.class);
    @Override
    public Flow<Pair<byte[],C>, Pair<Either<Bundle, Error>,C>, NotUsed> flow() {
        return
                Flow.<Pair<byte[],C>>create()
                        .map(message->{
                            try {
                                return Pair.create(Left.<Bundle,Error>apply(Serializer.serializeBundle(message.first())),message.second());
                            }catch (IOException e){
                                return Pair.create(Right.<Bundle,Error>apply(new Error.LogAndSkip(message.first(),e.getMessage())),message.second());
                            }
                        });
    }
}
