package com.mercari.solution.util.domain.text.tokenizer;

import ai.djl.huggingface.tokenizers.Encoding;
import ai.djl.huggingface.tokenizers.HuggingFaceTokenizer;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.StorageObject;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.cloud.google.StorageUtil;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class MHuggingFaceTokenizer implements MTokenizer, Serializable {

    private static final Map<String, HuggingFaceTokenizer> tokenizers = new ConcurrentHashMap<>();

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
        getOrLoadTokenizer(tokenizers, path, config);
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
    public String decode(long[] ids, boolean skipSpecialTokens) {
        final HuggingFaceTokenizer tokenizer = getTokenizer();
        if(tokenizer == null) {
            return null;
        }
        return tokenizer.decode(ids, skipSpecialTokens);
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
                .ofNullable(tokenizers.get(cacheKey(path, config)))
                .orElseGet(() -> getOrLoadTokenizer(tokenizers, path, config));
    }

    // the same file loaded with different options is a different tokenizer
    private static String cacheKey(final String path, final MTokenizerConfig config) {
        if(config == null) {
            return path;
        }
        return String.join("|", path, String.valueOf(config.addSpecialTokens), String.valueOf(config.truncation),
                String.valueOf(config.maxLength), String.valueOf(config.padding), String.valueOf(config.stride),
                String.valueOf(config.withOverflowingTokens));
    }

    public static HuggingFaceTokenizer getOrLoadTokenizer(
            final Map<String, HuggingFaceTokenizer> tokenizers,
            final String path,
            final MTokenizerConfig config) {

        final HuggingFaceTokenizer tokenizer = tokenizers.get(cacheKey(path, config));
        if(tokenizer != null) {
            return tokenizer;
        }
        try {
            return loadTokenizer(tokenizers, path, config);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load tokenizer: " + path, e);
        }
    }

    public synchronized static HuggingFaceTokenizer loadTokenizer(
            final Map<String, HuggingFaceTokenizer> tokenizers,
            final String path,
            final MTokenizerConfig config) throws IOException {

        final String key = cacheKey(path, config);
        if(tokenizers.containsKey(key)) {
            return tokenizers.get(key);
        }

        final Path localPath;
        if (path.startsWith("gs://")) {
            final Path localDirPath = Files.createTempDirectory("huggingface-tokenizers-");
            final Storage storage = StorageUtil.storage();
            if(path.endsWith("/")) {
                for(final StorageObject object : StorageUtil.listFiles(path)) {
                    if(object.getName().endsWith("/")) {
                        continue;
                    }
                    final Path file = localDirPath.resolve(getFileName(object.getName()));
                    try (final OutputStream os = new BufferedOutputStream(Files.newOutputStream(file))) {
                        StorageUtil.downloadTo(storage, object, os);
                    }
                }
                localPath = localDirPath;
            } else if(path.endsWith(".json")) {
                final Path file = localDirPath.resolve(getFileName(path));
                try (final OutputStream os = new BufferedOutputStream(Files.newOutputStream(file))) {
                    StorageUtil.downloadTo(storage, path, os);
                }
                localPath = file;
            } else {
                throw new IllegalArgumentException("Tokenizer path must be a directory ending with '/' or a .json file: " + path);
            }
        } else if (Files.exists(Path.of(path))) {
            localPath = Path.of(path);
        } else {
            throw new FileNotFoundException("Tokenizer path not found: " + path);
        }

        final HuggingFaceTokenizer.Builder builder = HuggingFaceTokenizer
                .builder()
                .optTokenizerPath(localPath);
        if(config != null) {
            if(config.addSpecialTokens != null) {
                builder.optAddSpecialTokens(config.addSpecialTokens);
            }
            if(config.truncation != null) {
                builder.optTruncation(config.truncation);
            }
            if(config.maxLength != null) {
                builder.optMaxLength(config.maxLength);
            }
            if(config.padding != null) {
                builder.optPadding(config.padding);
            }
            if(config.stride != null) {
                builder.optStride(config.stride);
            }
            if(config.withOverflowingTokens != null) {
                builder.optWithOverflowingTokens(config.withOverflowingTokens);
            }
        }
        try {
            final HuggingFaceTokenizer tokenizer = builder.build();
            tokenizers.put(key, tokenizer);
            return tokenizer;
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }

    }

    public static String getFileName(String path) {
        final String[] paths = path.split("/");
        return paths[paths.length - 1];
    }

}
