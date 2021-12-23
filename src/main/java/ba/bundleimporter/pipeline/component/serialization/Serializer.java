package ba.bundleimporter.pipeline.component.serialization;

import ba.bundleimporter.datamodel.Bundle;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class Serializer {

    public static Bundle serializeBundle (byte[] rawMessage) throws IOException{
        DatumReader<Bundle> reader
                = new SpecificDatumReader<>(Bundle.class);
        Decoder decoder = DecoderFactory.get().jsonDecoder(
                    Bundle.getClassSchema(), new String(rawMessage));
        return reader.read(null, decoder);

    }
    public static byte [] deSerializeBundle(Bundle bundle) throws IOException{
        DatumWriter<Bundle> writer = new SpecificDatumWriter<>(
                Bundle.class);
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        Encoder jsonEncoder = EncoderFactory.get().jsonEncoder(
                Bundle.getClassSchema(), stream);
        writer.write(bundle, jsonEncoder);
        jsonEncoder.flush();
        return stream.toByteArray();

    }
}
