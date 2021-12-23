package ba.bundleimporter.pipeline.component.errorhandler;

import akka.Done;
import ba.bundleimporter.pipeline.component.Error;
import ba.bundleimporter.pipeline.component.FlowComponent;

public interface ErrorHandlerFlow<C> extends FlowComponent<Error.BusinessError,Done,C> {}
