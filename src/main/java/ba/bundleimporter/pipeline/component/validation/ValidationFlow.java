package ba.bundleimporter.pipeline.component.validation;

import ba.bundleimporter.datamodel.Bundle;
import ba.bundleimporter.pipeline.component.FlowComponent;

public interface ValidationFlow<C> extends FlowComponent<Bundle,Bundle,C> {}
