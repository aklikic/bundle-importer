package ba.bundleimporter.pipeline;

import akka.Done;
import akka.NotUsed;
import akka.japi.Pair;
import akka.stream.javadsl.Flow;
import ba.bundleimporter.pipeline.component.errorhandler.ErrorHandlerFlow;
import ba.bundleimporter.pipeline.component.serialization.SerializationFlow;
import ba.bundleimporter.pipeline.component.validation.ValidationFlow;

public class PipelineFlow extends PipelineFlowSupport {

    public static <C> Flow<Pair<byte[],C>, Pair<Done,C>, NotUsed> flow(SerializationFlow<C> serializationFlow,
                                                                       ValidationFlow<C> validationFlow,
                                                                       ErrorHandlerFlow<C> errorToTopic,
                                                                       ErrorHandlerFlow<C> errorToDb){
        return
        Flow.<Pair<byte[],C>>create()
                .via(serializationFlow.flow())
                .flatMapConcat(m->applyFlowConditional(validationFlow.flow(),m))
                .flatMapConcat(m->applyErrorFlow(errorToTopic.flow(),m))
                .flatMapConcat(m->applyErrorFlow(errorToDb.flow(),m))
                .map(m->resultHandler(m));

    }
}
