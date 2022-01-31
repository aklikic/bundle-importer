package ba.bundleimporter.pipeline.component.validation;

import akka.japi.Pair;
import akka.stream.javadsl.Flow;
import ba.bundleimporter.datamodel.Bundle;
import ba.bundleimporter.pipeline.component.Error;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.util.Either;
import scala.util.Left;
import scala.util.Right;

public class ValidationFlowImpl<C> implements ValidationFlow{
    private static final Logger logger = LoggerFactory.getLogger(ValidationFlowImpl.class);
    public Flow<Pair<Bundle,C>, Pair<Either<Error,Bundle>,C>, ?> flow() {
        return
                Flow.<Pair<Bundle,C>>create()
                       // .throttle(1, Duration.ofSeconds(1))
                        .map(bundle->{
                            boolean valid = validateBundleId(bundle.first().getBundleId().toString());
                            logger.info("validating bundleId {}: {} ",bundle.first().getBundleId(),valid);
                            if(valid)
                                return Pair.create(Right.apply(bundle.first()),bundle.second());
                            else
                                return Pair.create(Left.apply(new Error.BusinessError(bundle.first(),"BundleId is not a number!")),bundle.second());
                        });
    }

    private boolean validateBundleId(String bundleId){
        try {
            Integer.parseInt(bundleId);
            return true;
        }catch (NumberFormatException e){
            return false;
        }
    }
}
