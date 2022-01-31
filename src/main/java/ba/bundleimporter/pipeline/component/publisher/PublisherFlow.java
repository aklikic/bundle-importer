package ba.bundleimporter.pipeline.component.publisher;

import ba.bundleimporter.datamodel.Bundle;
import ba.bundleimporter.datamodel.BundleOut;
import ba.bundleimporter.pipeline.component.FlowComponent;

public interface PublisherFlow<C> extends FlowComponent<Bundle, BundleOut,C> {}

