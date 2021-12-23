package ba.bundleimporter.pipeline.component.validation;

import akka.NotUsed;
import akka.japi.Pair;
import akka.stream.javadsl.Flow;
import ba.bundleimporter.datamodel.Bundle;
import ba.bundleimporter.pipeline.component.Error;
import scala.util.Either;
import scala.util.Left;
import scala.util.Right;

public class ValidationFlowImpl<C> implements ValidationFlow{
    public Flow<Pair<Bundle,C>, Pair<Either<Bundle, Error>,C>, NotUsed> flow() {
        return
                Flow.<Pair<Bundle,C>>create()
                        .map(bundle->{
                            if(validateBundleId(bundle.first().getBundleId().toString()))
                                return Pair.create(Left.apply(bundle.first()),bundle.second());
                            else
                                return Pair.create(Right.apply(new Error.BusinessError(bundle.first(),"BundleId is not a number!")),bundle.second());
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
