package ba.bundleimporter.pipeline.component.serialization;

import ba.bundleimporter.datamodel.Bundle;
import ba.bundleimporter.datamodel.BundleOut;
import ba.bundleimporter.datamodel.ErrorBundle;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class Serializer {

    //private static final Logger logger = LoggerFactory.getLogger(Serializer.class);

    public static Bundle deSerializeBundle (byte[] rawMessage) throws IOException{
        DatumReader<Bundle> reader
                = new SpecificDatumReader<>(Bundle.class);
        Decoder decoder = DecoderFactory.get().jsonDecoder(
                    Bundle.getClassSchema(), new String(rawMessage));
        return reader.read(null, decoder);

    }
    public static byte [] serializeBundle(Bundle bundle) throws IOException{
        DatumWriter<Bundle> writer = new SpecificDatumWriter<>(
                Bundle.class);
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        Encoder jsonEncoder = EncoderFactory.get().jsonEncoder(
                Bundle.getClassSchema(), stream);
        writer.write(bundle, jsonEncoder);
        jsonEncoder.flush();
        return stream.toByteArray();

    }

    public static BundleOut deSerializeBundleOut (byte[] rawMessage) throws IOException{
        DatumReader<BundleOut> reader
                = new SpecificDatumReader<>(BundleOut.class);
        Decoder decoder = DecoderFactory.get().jsonDecoder(
                BundleOut.getClassSchema(), new String(rawMessage));
        return reader.read(null, decoder);

    }
    public static byte [] serializeBundleOut(BundleOut bundle) throws IOException{
        DatumWriter<BundleOut> writer = new SpecificDatumWriter<>(
                BundleOut.class);
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        Encoder jsonEncoder = EncoderFactory.get().jsonEncoder(
                BundleOut.getClassSchema(), stream);
        writer.write(bundle, jsonEncoder);
        jsonEncoder.flush();
        return stream.toByteArray();

    }

    public static ErrorBundle deSerializeErrorBundle (byte[] rawMessage) throws IOException{
        //logger.info("deSerializeErrorBundle: {}",new String (rawMessage));
        DatumReader<ErrorBundle> reader
                = new SpecificDatumReader<>(ErrorBundle.class);
        Decoder decoder = DecoderFactory.get().jsonDecoder(
                ErrorBundle.getClassSchema(), new String(rawMessage));
        return reader.read(null, decoder);

    }
    public static byte [] serializeErrorBundle(ErrorBundle bundle) throws IOException{
        //logger.info("serializeErrorBundle: {}",bundle);
        DatumWriter<ErrorBundle> writer = new SpecificDatumWriter<>(
                ErrorBundle.class);
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        Encoder jsonEncoder = EncoderFactory.get().jsonEncoder(
                ErrorBundle.getClassSchema(), stream);
        writer.write(bundle, jsonEncoder);
        jsonEncoder.flush();
        return stream.toByteArray();

    }
}
