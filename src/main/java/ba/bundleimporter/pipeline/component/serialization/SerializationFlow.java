package ba.bundleimporter.pipeline.component.serialization;

import ba.bundleimporter.datamodel.Bundle;
import ba.bundleimporter.pipeline.component.FlowComponent;

public interface SerializationFlow<C> extends FlowComponent<byte[],Bundle,C> {}
