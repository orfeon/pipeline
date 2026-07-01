package com.mercari.solution.util.domain.file;

public class FileUtil {

    public enum Format {
        csv,
        json,
        avro,
        parquet
    }

    public enum CodecName {
        // common
        SNAPPY,
        ZSTD,
        UNCOMPRESSED,
        // avro only
        BZIP2,
        DEFLATE,
        XZ,
        // parquet only
        LZO,
        LZ4,
        LZ4_RAW,
        BROTLI,
        GZIP
    }

}
