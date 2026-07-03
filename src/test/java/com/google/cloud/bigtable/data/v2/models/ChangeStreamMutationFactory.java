package com.google.cloud.bigtable.data.v2.models;

import com.google.protobuf.ByteString;

import java.time.Instant;

/**
 * Test helper to build {@link ChangeStreamMutation} instances in-memory.
 * The factory methods of ChangeStreamMutation are package-private in the Bigtable client library,
 * so this helper is placed in the same package (test scope only).
 */
public class ChangeStreamMutationFactory {

    public static ChangeStreamMutation createUserMutation(
            final ByteString rowKey,
            final String sourceClusterId,
            final Instant commitTime,
            final int tieBreaker,
            final String token,
            final Instant estimatedLowWatermark,
            final String setCellFamily,
            final ByteString setCellQualifier,
            final long setCellTimestampMicros,
            final ByteString setCellValue,
            final String deleteFamilyName,
            final String deleteCellsFamily,
            final ByteString deleteCellsQualifier,
            final long deleteCellsStartTimestampMicros,
            final long deleteCellsEndTimestampMicros) {

        return ChangeStreamMutation.createUserMutation(rowKey, sourceClusterId, commitTime, tieBreaker)
                .setCell(setCellFamily, setCellQualifier, setCellTimestampMicros, setCellValue)
                .deleteFamily(deleteFamilyName)
                .deleteCells(
                        deleteCellsFamily,
                        deleteCellsQualifier,
                        Range.TimestampRange.create(deleteCellsStartTimestampMicros, deleteCellsEndTimestampMicros))
                .setToken(token)
                .setEstimatedLowWatermarkTime(estimatedLowWatermark)
                .build();
    }
}
