package dbuccola.experiments.avro;

import org.apache.avro.Schema;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.junit.Test;

import java.io.ByteArrayOutputStream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class AvroTest {

    @Test
    public void testEverythingTheSame() throws Exception {
        SampleRecord record = SampleData.generateSampleRecord();
        SampleRecord recordAfterTrip = decode(encode(record), SampleRecord.getClassSchema());

        assertThat(recordAfterTrip, equalTo(record));
    }

    @Test
    public void testDecoderHasMoreFieldsAndKnowsBothSchemas() throws Exception {
        SampleRecord record = SampleData.generateSampleRecord();
        CompatibleSampleRecord recordAfterTrip = decode(encode(record), SampleRecord.getClassSchema(), CompatibleSampleRecord.getClassSchema());

        assertThat(recordAfterTrip.getRequiredString(), equalTo(record.getRequiredString()));
        assertThat(recordAfterTrip.getNestedRecords(), equalTo(record.getNestedRecords()));
    }

    @Test(expected = java.io.EOFException.class)
    public void testDecoderHasMoreFieldsAndDoesntKnowWriterSchema() throws Exception {
        SampleRecord record = SampleData.generateSampleRecord();
        CompatibleSampleRecord recordAfterTrip = decode(encode(record), CompatibleSampleRecord.getClassSchema());

        assertThat(recordAfterTrip.getRequiredString(), equalTo(record.getRequiredString()));
        assertThat(recordAfterTrip.getNestedRecords(), equalTo(record.getNestedRecords()));
    }

    @Test
    public void testDecoderHasLessFieldsAndKnowsBothSchemas() throws Exception {
        CompatibleSampleRecord record = SampleData.generateCompatibleSampleRecord();
        SampleRecord recordAfterTrip = decode(encode(record), CompatibleSampleRecord.getClassSchema(), SampleRecord.getClassSchema());

        assertThat(recordAfterTrip.getRequiredString(), equalTo(record.getRequiredString()));
        assertThat(recordAfterTrip.getNestedRecords(), equalTo(record.getNestedRecords()));
    }

    @Test
    public void testDecoderHasLessFieldsAndDoesntKnowWriterSchema() throws Exception {
        CompatibleSampleRecord record = SampleData.generateCompatibleSampleRecord();
        SampleRecord recordAfterTrip = decode(encode(record), SampleRecord.getClassSchema());

        assertThat(recordAfterTrip.getRequiredString(), equalTo(record.getRequiredString()));
        assertThat(recordAfterTrip.getNestedRecords(), equalTo(record.getNestedRecords()));
    }

    private static <T extends SpecificRecord> byte[] encode(T record) throws Exception {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
        DatumWriter<T> writer = new SpecificDatumWriter<>(record.getSchema());
        writer.write(record, encoder);
        encoder.flush();
        outputStream.close();
        return outputStream.toByteArray();
    }

    private <T extends SpecificRecord> T decode(byte[] bytes, Schema readerSchema) throws Exception {
        return decode(bytes, readerSchema, readerSchema); // Standard default assumes reader and writer using same schema
    }

    private <T extends SpecificRecord> T decode(byte[] bytes, Schema writerSchema, Schema readerSchema) throws Exception {
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
        DatumReader<T> reader = new SpecificDatumReader<>(writerSchema, readerSchema);
        return reader.read(null, decoder);
    }
}
