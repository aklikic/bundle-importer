package ba.bundleimporter.pipeline.component.serialization;

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
    public static Logger logger = LoggerFactory.getLogger(SerializationFlowImpl.class);
    @Override
    public Flow<Pair<byte[],C>, Pair<Either<Error,Bundle>,C>, ?> flow() {
        return
                Flow.<Pair<byte[],C>>create()
                        .<Pair<Either<Error,Bundle>,C>>map(message->{
                            Bundle bundle = null;
                            String error = null;
                            try {
                                bundle = Serializer.deSerializeBundle(message.first());
                            }catch (IOException e){
                                error = e.getMessage();
                            }
                            if(bundle != null) {
                                logger.info("DeSerialization [{}] OK",new String(message.first()));
                                return Pair.create(Right.apply(bundle), message.second());
                            }else {
                                logger.error("DeSerialization [{}] ERROR",new String(message.first()));
                                return Pair.create(Left.apply(new Error.LogAndSkip(message.first(), error)), message.second());
                            }
                        });
    }
}
