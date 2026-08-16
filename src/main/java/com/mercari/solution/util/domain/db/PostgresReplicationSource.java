package com.mercari.solution.util.domain.db;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.joda.time.Instant;
import org.postgresql.PGConnection;
import org.postgresql.PGProperty;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Unbounded source reading a PostgreSQL logical replication slot with the {@code pgoutput}
 * plugin (binary tuple mode) via the pgjdbc replication protocol API.
 *
 * <p>A logical replication slot is inherently a single-consumer stream, so the source never
 * splits; callers redistribute downstream work with a {@code Reshuffle}. The checkpoint mark
 * carries the last received LSN — on checkpoint finalization it is confirmed to the server
 * ({@code setFlushedLSN}), which lets the server recycle WAL and defines the restart point.
 * Confirming mid-transaction is safe: on restart the server resends every transaction whose
 * commit follows the confirmed position, so delivery is at-least-once and downstream
 * consumers deduplicate by the envelope sequence.</p>
 */
public class PostgresReplicationSource extends UnboundedSource<PgOutput.ChangeEvent, PostgresReplicationSource.LsnCheckpointMark> {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresReplicationSource.class);

    // margin subtracted from the wall clock for the idle watermark: while the stream is idle
    // and no transaction is open, commits older than this are assumed to have been delivered
    private static final long IDLE_WATERMARK_MARGIN_MILLIS = 10_000L;
    // bookkeeping (non-row) messages consumed per advance() call before yielding control
    private static final int MAX_MESSAGES_PER_ADVANCE = 1000;

    private final String url;
    private final String user;
    private final String password;
    private final String slot;
    private final String publication;
    private final int statusIntervalSeconds;
    // schema.table -> primary key column names, resolved from the catalog at pipeline launch
    private final Map<String, List<String>> keyColumns;

    public PostgresReplicationSource(
            final String url,
            final String user,
            final String password,
            final String slot,
            final String publication,
            final int statusIntervalSeconds,
            final Map<String, List<String>> keyColumns) {

        this.url = url;
        this.user = user;
        this.password = password;
        this.slot = slot;
        this.publication = publication;
        this.statusIntervalSeconds = statusIntervalSeconds;
        this.keyColumns = keyColumns;
    }

    @Override
    public List<PostgresReplicationSource> split(final int desiredNumSplits, final PipelineOptions options) {
        // a logical replication slot has a single consumer
        return List.of(this);
    }

    @Override
    public UnboundedReader<PgOutput.ChangeEvent> createReader(
            final PipelineOptions options,
            final LsnCheckpointMark checkpointMark) {

        return new Reader(this, checkpointMark == null ? 0L : checkpointMark.lsn);
    }

    @Override
    public Coder<LsnCheckpointMark> getCheckpointMarkCoder() {
        return SerializableCoder.of(LsnCheckpointMark.class);
    }

    @Override
    public Coder<PgOutput.ChangeEvent> getOutputCoder() {
        return SerializableCoder.of(PgOutput.ChangeEvent.class);
    }

    public static class LsnCheckpointMark implements CheckpointMark, Serializable {

        private final long lsn;
        private final transient Reader reader;

        private LsnCheckpointMark(final long lsn, final Reader reader) {
            this.lsn = lsn;
            this.reader = reader;
        }

        @Override
        public void finalizeCheckpoint() {
            // the reader reference does not survive serialization; a mark restored from a
            // checkpoint is finalized implicitly by the restart position instead
            if(reader != null) {
                reader.enqueueConfirm(lsn);
            }
        }
    }

    private static class Reader extends UnboundedReader<PgOutput.ChangeEvent> {

        private final PostgresReplicationSource source;
        private final long startLsn;

        private Connection connection;
        private PGReplicationStream stream;
        private PgOutput.Decoder decoder;

        private final Deque<PgOutput.ChangeEvent> pending = new ArrayDeque<>();
        private PgOutput.ChangeEvent current;
        private long lastReceiveLsn;
        private long watermarkMillis = Long.MIN_VALUE;
        private boolean inTransaction;

        // LSN confirmations enqueued by checkpoint finalization (possibly from another
        // thread); applied to the stream on the reader thread only
        private final AtomicLong confirmedLsn = new AtomicLong(0L);
        private long flushedLsn = 0L;

        private Reader(final PostgresReplicationSource source, final long startLsn) {
            this.source = source;
            this.startLsn = startLsn;
        }

        @Override
        public boolean start() throws IOException {
            final Properties properties = new Properties();
            if(source.user != null) {
                PGProperty.USER.set(properties, source.user);
            }
            if(source.password != null) {
                PGProperty.PASSWORD.set(properties, source.password);
            }
            PGProperty.REPLICATION.set(properties, "database");
            PGProperty.ASSUME_MIN_SERVER_VERSION.set(properties, "9.4");
            PGProperty.PREFER_QUERY_MODE.set(properties, "simple");
            try {
                this.connection = DriverManager.getConnection(source.url, properties);
                final PGConnection pgConnection = connection.unwrap(PGConnection.class);
                var builder = pgConnection.getReplicationAPI()
                        .replicationStream()
                        .logical()
                        .withSlotName(source.slot)
                        .withSlotOption("proto_version", 1)
                        .withSlotOption("publication_names", source.publication)
                        .withSlotOption("binary", true)
                        .withStatusInterval(source.statusIntervalSeconds, TimeUnit.SECONDS);
                if(startLsn > 0L) {
                    builder = builder.withStartPosition(LogSequenceNumber.valueOf(startLsn));
                }
                this.stream = builder.start();
                this.decoder = new PgOutput.Decoder(source.keyColumns);
                LOG.info("Started logical replication stream. slot: {}, publication: {}, startLsn: {}",
                        source.slot, source.publication, LogSequenceNumber.valueOf(startLsn));
            } catch (final SQLException e) {
                throw new IOException("Failed to start logical replication stream. slot: " + source.slot, e);
            }
            return advance();
        }

        @Override
        public boolean advance() throws IOException {
            applyPendingConfirm();
            if(!pending.isEmpty()) {
                current = pending.poll();
                return true;
            }
            try {
                for(int i = 0; i < MAX_MESSAGES_PER_ADVANCE; i++) {
                    final ByteBuffer buffer = stream.readPending();
                    if(buffer == null) {
                        onIdle();
                        current = null;
                        return false;
                    }
                    final long lsn = stream.getLastReceiveLSN().asLong();
                    final char messageType = (char) buffer.get(buffer.position());
                    final List<PgOutput.ChangeEvent> events = decoder.decode(buffer, lsn);
                    this.lastReceiveLsn = Math.max(lastReceiveLsn, lsn);
                    trackTransaction(messageType);
                    if(!events.isEmpty()) {
                        pending.addAll(events);
                        current = pending.poll();
                        advanceWatermark(current.commitTimestampMicros / 1000L);
                        return true;
                    }
                }
            } catch (final SQLException e) {
                throw new IOException("Failed to read logical replication stream. slot: " + source.slot, e);
            }
            current = null;
            return false;
        }

        private void trackTransaction(final char messageType) {
            if(messageType == 'B') {
                inTransaction = true;
            } else if(messageType == 'C') {
                inTransaction = false;
            }
        }

        private void onIdle() {
            if(!inTransaction) {
                advanceWatermark(System.currentTimeMillis() - IDLE_WATERMARK_MARGIN_MILLIS);
            }
        }

        private void advanceWatermark(final long millis) {
            if(millis > watermarkMillis) {
                watermarkMillis = millis;
            }
        }

        private void enqueueConfirm(final long lsn) {
            confirmedLsn.accumulateAndGet(lsn, Math::max);
        }

        private void applyPendingConfirm() {
            final long lsn = confirmedLsn.get();
            if(lsn > flushedLsn && stream != null && !stream.isClosed()) {
                final LogSequenceNumber logSequenceNumber = LogSequenceNumber.valueOf(lsn);
                stream.setFlushedLSN(logSequenceNumber);
                stream.setAppliedLSN(logSequenceNumber);
                flushedLsn = lsn;
            }
        }

        @Override
        public PgOutput.ChangeEvent getCurrent() throws NoSuchElementException {
            if(current == null) {
                throw new NoSuchElementException();
            }
            return current;
        }

        @Override
        public Instant getCurrentTimestamp() throws NoSuchElementException {
            if(current == null) {
                throw new NoSuchElementException();
            }
            return Instant.ofEpochMilli(current.commitTimestampMicros / 1000L);
        }

        @Override
        public Instant getWatermark() {
            if(watermarkMillis == Long.MIN_VALUE) {
                return org.apache.beam.sdk.transforms.windowing.BoundedWindow.TIMESTAMP_MIN_VALUE;
            }
            return Instant.ofEpochMilli(watermarkMillis);
        }

        @Override
        public CheckpointMark getCheckpointMark() {
            return new LsnCheckpointMark(lastReceiveLsn, this);
        }

        @Override
        public UnboundedSource<PgOutput.ChangeEvent, ?> getCurrentSource() {
            return source;
        }

        @Override
        public void close() throws IOException {
            try {
                if(stream != null && !stream.isClosed()) {
                    applyPendingConfirm();
                    stream.forceUpdateStatus();
                    stream.close();
                }
            } catch (final SQLException e) {
                LOG.warn("Failed to close logical replication stream. slot: {}, cause: {}", source.slot, e.getMessage());
            } finally {
                try {
                    if(connection != null && !connection.isClosed()) {
                        connection.close();
                    }
                } catch (final SQLException e) {
                    LOG.warn("Failed to close replication connection. cause: {}", e.getMessage());
                }
            }
        }
    }

}
