package dbuccola.experiments.avro;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * A generator of random sample data.
 */
class SampleData {

    private static final int DEFAULT_MAX_STRING_LENGTH = 255;
    private static final int DEFAULT_MAX_BYTES_LENGTH = 256;
    private static final String LETTERS_AND_NUMBERS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

    private static final Random random = new Random();

    static SampleRecord generateSampleRecord() {
        return new SampleRecord(nextString(), generateSampleNestedRecords(random.nextInt(20)));
    }

    static CompatibleSampleRecord generateCompatibleSampleRecord() {
        return new CompatibleSampleRecord(nextString(), generateSampleNestedRecords(random.nextInt(20)), nextString(), random.nextBoolean());
    }

    private static List<SampleNestedRecord> generateSampleNestedRecords(int count) {
        List<SampleNestedRecord> records = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            records.add(new SampleNestedRecord(
                random.nextBoolean(),
                random.nextDouble(),
                random.nextLong(),
                nextString(),
                ByteBuffer.wrap(nextBytes())));
        }
        return records;
    }

    private static byte[] nextBytes() {
        return nextBytes(DEFAULT_MAX_BYTES_LENGTH);
    }

    private static byte[] nextBytes(int maximumLength) {
        byte[] bytes = new byte[random.nextInt(maximumLength)];
        random.nextBytes(bytes);
        return bytes;
    }

    private static String nextString() {
        return nextString(DEFAULT_MAX_STRING_LENGTH);
    }

    private static String nextString(int maximumLength) {
        int length = random.nextInt(maximumLength);
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < length; i++) {
            builder.append(LETTERS_AND_NUMBERS.charAt(random.nextInt(LETTERS_AND_NUMBERS.length())));
        }
        return builder.toString();
    }
}