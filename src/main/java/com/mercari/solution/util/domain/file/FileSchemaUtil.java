package com.mercari.solution.util.domain.file;

import com.google.common.io.ByteStreams;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.util.List;

/**
 * Samples record schemas from avro / parquet files through Beam {@link FileSystems}, so scheme
 * and glob resolution behave exactly like the runtime IOs (AvroIO / ParquetIO) for gs://, s3://
 * and local paths. Only the file header (avro) or footer (parquet) is read — files are never
 * downloaded in full when the filesystem supports seeking.
 */
public class FileSchemaUtil {

    private static final Logger LOG = LoggerFactory.getLogger(FileSchemaUtil.class);

    // PlainParquetConfiguration keeps schema sampling free of hadoop Configuration, which is not
    // fully available on every launcher classpath (e.g. shaded hadoop-client-api without runtime)
    private static final ParquetReadOptions READ_OPTIONS = ParquetReadOptions
            .builder(new PlainParquetConfiguration())
            .build();

    public static Schema getAvroSchema(final String pathOrPattern) {
        final List<MatchResult.Metadata> files = matchNonEmptyFiles(pathOrPattern, "avro");
        for(final MatchResult.Metadata metadata : files) {
            try(final InputStream is = Channels.newInputStream(FileSystems.open(metadata.resourceId()));
                final DataFileStream<GenericRecord> reader = new DataFileStream<>(is, new GenericDatumReader<>())) {
                return reader.getSchema();
            } catch (final Exception e) {
                LOG.warn("Failed to read avro schema from file: {} cause: {}", metadata.resourceId(), e.toString());
            }
        }
        throw new IllegalStateException(
                "Avro schema could not be read from any of the " + files.size() + " files matching: " + pathOrPattern);
    }

    public static Schema getParquetSchema(final String pathOrPattern) {
        final List<MatchResult.Metadata> files = matchNonEmptyFiles(pathOrPattern, "parquet");
        for(final MatchResult.Metadata metadata : files) {
            try {
                return readParquetSchema(metadata);
            } catch (final Exception e) {
                LOG.warn("Failed to read parquet schema from file: {} cause: {}", metadata.resourceId(), e.toString());
            }
        }
        throw new IllegalStateException(
                "Parquet schema could not be read from any of the " + files.size() + " files matching: " + pathOrPattern);
    }

    public static Schema getParquetSchema(final byte[] bytes) {
        try(final ParquetFileReader reader = ParquetFileReader.open(new ByteArrayInputFile(bytes), READ_OPTIONS)) {
            return new AvroSchemaConverter().convert(reader.getFooter().getFileMetaData().getSchema());
        } catch (final IOException e) {
            throw new RuntimeException("Failed to read parquet schema from bytes", e);
        }
    }

    private static Schema readParquetSchema(final MatchResult.Metadata metadata) throws IOException {
        try(final ReadableByteChannel channel = FileSystems.open(metadata.resourceId())) {
            // ParquetFileReader only reads the footer when the input is seekable
            // (gs:// / s3:// / local all return a SeekableByteChannel)
            final InputFile inputFile;
            if(channel instanceof SeekableByteChannel seekable) {
                inputFile = new SeekableChannelInputFile(seekable, metadata.sizeBytes());
            } else {
                inputFile = new ByteArrayInputFile(ByteStreams.toByteArray(Channels.newInputStream(channel)));
            }
            try(final ParquetFileReader reader = ParquetFileReader.open(inputFile, READ_OPTIONS)) {
                return new AvroSchemaConverter().convert(reader.getFooter().getFileMetaData().getSchema());
            }
        }
    }

    private static List<MatchResult.Metadata> matchNonEmptyFiles(final String pathOrPattern, final String format) {
        final List<MatchResult.Metadata> metadata;
        try {
            final MatchResult match = FileSystems.match(pathOrPattern);
            if(!MatchResult.Status.OK.equals(match.status())) {
                throw new IllegalStateException(
                        "No files found to sample " + format + " schema for input: " + pathOrPattern
                                + " (match status: " + match.status() + ")");
            }
            metadata = match.metadata();
        } catch (final IOException e) {
            throw new RuntimeException("Failed to match files for input: " + pathOrPattern, e);
        }
        final List<MatchResult.Metadata> files = metadata.stream()
                .filter(m -> m.sizeBytes() > 0)
                .toList();
        if(files.isEmpty()) {
            throw new IllegalStateException(
                    "No non-empty files found to sample " + format + " schema for input: " + pathOrPattern);
        }
        return files;
    }

    private static class SeekableChannelInputFile implements InputFile {

        private final SeekableByteChannel channel;
        private final long length;

        SeekableChannelInputFile(final SeekableByteChannel channel, final long length) {
            this.channel = channel;
            this.length = length;
        }

        @Override
        public long getLength() {
            return length;
        }

        @Override
        public SeekableInputStream newStream() {
            return new DelegatingSeekableInputStream(Channels.newInputStream(channel)) {

                @Override
                public long getPos() throws IOException {
                    return channel.position();
                }

                @Override
                public void seek(long newPos) throws IOException {
                    channel.position(newPos);
                }
            };
        }
    }

    public static class ByteArrayInputFile implements InputFile {

        private final byte[] data;

        public ByteArrayInputFile(final byte[] data) {
            this.data = data;
        }

        @Override
        public long getLength() {
            return data.length;
        }

        @Override
        public SeekableInputStream newStream() {
            final SeekableByteArrayInputStream stream = new SeekableByteArrayInputStream(data);
            return new DelegatingSeekableInputStream(stream) {

                @Override
                public long getPos() {
                    return stream.getPos();
                }

                @Override
                public void seek(long newPos) {
                    stream.setPos((int) newPos);
                }
            };
        }

        private static class SeekableByteArrayInputStream extends ByteArrayInputStream {

            SeekableByteArrayInputStream(byte[] buf) {
                super(buf);
            }

            void setPos(int pos) {
                this.pos = pos;
            }

            int getPos() {
                return this.pos;
            }
        }
    }

}
