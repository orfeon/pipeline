package com.mercari.solution.module.sink.fileio;

import com.mercari.solution.module.MElement;
import com.mercari.solution.util.domain.file.FileUtil;
import com.mercari.solution.util.schema.converter.AvroToArrowConverter;
import com.mercari.solution.util.schema.converter.ElementToAvroConverter;
import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.values.KV;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;

/**
 * Writes elements as an Arrow IPC File (Feather V2). Elements are buffered into a
 * {@link VectorSchemaRoot} and emitted as one record batch every {@code batchSize} rows.
 * Buffers are allocated off-heap: each open writer holds up to batchSize x row-width
 * direct memory, so batchSize bounds the per-writer footprint under numShards / dynamic
 * destinations.
 */
public class ArrowSink implements FileIO.Sink<KV<String, MElement>> {

    private final String jsonSchema;
    private final FileUtil.CodecName codecName;
    private final int batchSize;

    private transient Schema avroSchema;
    private transient BufferAllocator allocator;
    private transient VectorSchemaRoot root;
    private transient ArrowFileWriter writer;
    private transient int rows;

    public static ArrowSink of(
            final com.mercari.solution.module.Schema schema,
            final FileUtil.CodecName codecName,
            final int batchSize) {

        return new ArrowSink(schema.getAvroSchema().toString(), codecName, batchSize);
    }

    ArrowSink(
            final String jsonSchema,
            final FileUtil.CodecName codecName,
            final int batchSize) {

        this.jsonSchema = jsonSchema;
        this.codecName = codecName;
        this.batchSize = batchSize;
    }

    @Override
    public void open(WritableByteChannel channel) throws IOException {
        this.avroSchema = new Schema.Parser().parse(jsonSchema);
        final org.apache.arrow.vector.types.pojo.Schema arrowSchema = AvroToArrowConverter.convertSchema(avroSchema);
        this.allocator = new RootAllocator();
        this.root = VectorSchemaRoot.create(arrowSchema, allocator);
        this.root.allocateNew();
        // Arrow IPC buffer compression supports only LZ4_FRAME and ZSTD;
        // any other codec (including the module default SNAPPY) writes uncompressed buffers
        this.writer = switch (this.codecName) {
            case ZSTD -> new ArrowFileWriter(root, null, channel, null, new IpcOption(),
                    CommonsCompressionFactory.INSTANCE, CompressionUtil.CodecType.ZSTD);
            case LZ4 -> new ArrowFileWriter(root, null, channel, null, new IpcOption(),
                    CommonsCompressionFactory.INSTANCE, CompressionUtil.CodecType.LZ4_FRAME);
            default -> new ArrowFileWriter(root, null, channel);
        };
        this.writer.start();
        this.rows = 0;
    }

    @Override
    public void write(KV<String, MElement> element) throws IOException {
        final MElement input = element.getValue();
        final GenericRecord record = ElementToAvroConverter.convert(avroSchema, input);
        if(record == null) {
            return;
        }
        AvroToArrowConverter.setRecord(root, rows, record);
        rows++;
        if(rows >= batchSize) {
            writeBatch();
        }
    }

    @Override
    public void flush() throws IOException {
        try {
            if(rows > 0) {
                writeBatch();
            }
            writer.end();
        } finally {
            root.close();
            allocator.close();
        }
    }

    private void writeBatch() throws IOException {
        root.setRowCount(rows);
        writer.writeBatch();
        root.clear();
        root.allocateNew();
        rows = 0;
    }

}
