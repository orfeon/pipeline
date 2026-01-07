package com.mercari.solution.util.domain.text.tokenizer;

import ai.djl.huggingface.tokenizers.Encoding;
import ai.djl.huggingface.tokenizers.HuggingFaceTokenizer;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.StorageObject;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.gcp.StorageUtil;
import org.apache.hadoop.hbase.exceptions.IllegalArgumentIOException;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public class MHuggingFaceTokenizer implements MTokenizer, Serializable {

    private static final String TOKENIZER_BASE_DIR = "/huggingface-tokenizers/";

    private static final Map<String, HuggingFaceTokenizer> tokenizers = new HashMap<>();

    private final String path;
    private final String field;
    private final MTokenizerConfig config;

    private MHuggingFaceTokenizer(
            final String path,
            final String field,
            final MTokenizerConfig config) {

        this.path = path;
        this.field = field;
        this.config = config;
    }

    public static MHuggingFaceTokenizer create(
            final String path,
            final String field,
            final MTokenizerConfig config) {
        return new MHuggingFaceTokenizer(path, field, config);
    }

    @Override
    public List<Schema.Field> getOutputFields() {
        return List.of(
                Schema.Field.of("ids", Schema.FieldType.array(Schema.FieldType.INT64)),
                Schema.Field.of("attentionMask", Schema.FieldType.array(Schema.FieldType.INT64)),
                Schema.Field.of("tokens", Schema.FieldType.array(Schema.FieldType.STRING))
        );
    }


    @Override
    public List<String> validate() {
        return new ArrayList<>();
    }

    @Override
    public void setup() {
        getOrLoadTokenizer(tokenizers, path);
    }

    @Override
    public Map<String, Object> encode(String text) {
        final HuggingFaceTokenizer tokenizer = getTokenizer();
        if(tokenizer == null) {
            return new HashMap<>();
        }
        final Encoding encoding = tokenizer.encode(text);
        return convert(encoding);
    }

    @Override
    public String decode(long[] ids) {
        final HuggingFaceTokenizer tokenizer = getTokenizer();
        if(tokenizer == null) {
            return null;
        }
        return tokenizer.decode(ids);
    }

    @Override
    public List<Map<String, Object>> batchEncode(List<String> text) {
        final HuggingFaceTokenizer tokenizer = getTokenizer();
        if(tokenizer == null) {
            return null;
        }
        final Encoding[] encodings = tokenizer.batchEncode(text);
        final List<Map<String, Object>> valuesList = new ArrayList<>();
        for(final Encoding encoding : encodings) {
            final Map<String, Object> values = convert(encoding);
            valuesList.add(values);
        }
        return valuesList;
    }

    @Override
    public List<String> batchDecode(long[][] ids) {
        final HuggingFaceTokenizer tokenizer = getTokenizer();
        if(tokenizer == null) {
            return null;
        }
        final String[] strings = tokenizer.batchDecode(ids);
        return List.of(strings);
    }

    @Override
    public List<String> tokenize(String text) {
        final HuggingFaceTokenizer tokenizer = getTokenizer();
        if(tokenizer == null) {
            return null;
        }
        return tokenizer.tokenize(text);
    }

    private Map<String, Object> convert(Encoding encoding) {
        final Map<String, Object> values = new HashMap<>();
        values.put("ids", MTokenizer.toList(encoding.getIds()));
        values.put("attentionMask", MTokenizer.toList(encoding.getAttentionMask()));
        values.put("typeIds", MTokenizer.toList(encoding.getTypeIds()));
        values.put("sequenceIds", MTokenizer.toList(encoding.getSequenceIds()));
        values.put("tokens", MTokenizer.toList(encoding.getTokens()));
        return values;
    }

    private HuggingFaceTokenizer getTokenizer() {
        return Optional
                .ofNullable(tokenizers.get(path))
                .orElseGet(() -> getOrLoadTokenizer(tokenizers, path));
    }

    public synchronized static HuggingFaceTokenizer getOrLoadTokenizer(
            final Map<String, HuggingFaceTokenizer> tokenizers,
            final String path) {

        if(tokenizers.containsKey(path)) {
            final HuggingFaceTokenizer tokenizer = tokenizers.get(path);
            if(tokenizer != null) {
                return tokenizer;
            }
        }
        try {
            loadTokenizer(tokenizers, path);
            return tokenizers.get(path);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized static void loadTokenizer(
            final Map<String, HuggingFaceTokenizer> tokenizers,
            final String path) throws IOException {

        if(tokenizers.containsKey(path)) {
            return;
        }

        if (path.startsWith("gs://")) {
            final String localDirPath = TOKENIZER_BASE_DIR + UUID.randomUUID() + "/";
            Files.createDirectories(Path.of(localDirPath));

            final Storage storage = StorageUtil.storage();
            if(path.endsWith("/")) {
                final List<StorageObject> objects = StorageUtil.listFiles(path);
                for(final StorageObject object : objects) {
                    final String fileName = getFileName(object.getName());
                    final File file = new File(localDirPath + fileName);
                    if(!file.createNewFile()) {
                        throw new IllegalStateException("Failed to create file: " + file);
                    }
                    try (final FileOutputStream fos = new FileOutputStream(file);
                         final BufferedOutputStream bos = new BufferedOutputStream(fos)) {

                        StorageUtil.downloadTo(storage, object, bos);
                        bos.flush();
                    }
                }
            } else if(path.endsWith(".json")) {
                final String fileName = getFileName(path);
                final File file = new File(localDirPath + fileName);
                if(!file.createNewFile()) {
                    throw new IllegalStateException("Failed to create file: " + file);
                }
                try (final FileOutputStream fos = new FileOutputStream(file);
                     final BufferedOutputStream bos = new BufferedOutputStream(fos)) {

                    StorageUtil.downloadTo(storage, path, bos);
                    bos.flush();
                }
            } else {
                throw new IllegalArgumentException("");
            }
        } else if (!Files.exists(Path.of(path))) {
            throw new IllegalArgumentIOException();
        }

        final HuggingFaceTokenizer.Builder builder = HuggingFaceTokenizer
                .builder()
                .optTokenizerPath(Path.of(path));
        /*
        if(config.truncation != null) {
            builder.optTruncation(config.truncation);
        }
        if(config.maxLength != null) {
            builder.optMaxLength(config.maxLength);
        }
         */
        try {
            final HuggingFaceTokenizer tokenizer = builder.build();
            tokenizers.put(path, tokenizer);
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }

    }

    public static String getFileName(String path) {
        final String[] paths = path.split("/");
        return paths[paths.length - 1];
    }

}
