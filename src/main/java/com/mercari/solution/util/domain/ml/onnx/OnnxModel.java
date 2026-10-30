package com.mercari.solution.util.domain.ml.onnx;

import ai.djl.ndarray.NDArray;
import ai.onnxruntime.*;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.StorageObject;
import com.google.gson.JsonObject;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.cloud.google.StorageUtil;
import com.mercari.solution.util.schema.converter.OnnxToElementConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

public class OnnxModel implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(OnnxModel.class);

    // shared by every onnx module in the JVM (OrtSession.run is thread-safe), keyed by Config.sessionKey
    private static final Map<String, OrtSession> sessions = new ConcurrentHashMap<>();

    private final Config config;

    public static class Config implements Serializable {

        private String path;
        private String file;
        private Integer intraOpThreads;
        private Integer interOpThreads;
        private OrtSession.SessionOptions.OptLevel optLevel;
        private OrtSession.SessionOptions.ExecutionMode executionMode;
        private String configPath;
        private transient JsonObject schema;

        public List<String> validate() {
            final List<String> errorMessages = new ArrayList<>();
            if(this.path == null) {
                errorMessages.add("onnx model.path must not be null");
            } else if(file != null && path.startsWith("gs://") && !path.endsWith("/")) {
                errorMessages.add("onnx model.file requires model.path to be a directory ending with '/': " + path);
            }
            if(intraOpThreads != null && intraOpThreads < 1) {
                errorMessages.add("onnx model.intraOpThreads must be over than zero");
            }
            if(interOpThreads != null && interOpThreads < 1) {
                errorMessages.add("onnx model.interOpThreads must be over than zero");
            }
            if(schema != null) {
                if(Schema.parse(schema) == null) {
                    errorMessages.add("onnx model.schema is illegal: " + schema);
                }
            }
            return errorMessages;
        }

        public void setDefaults() {
            if(optLevel == null) {
                optLevel = OrtSession.SessionOptions.OptLevel.BASIC_OPT;
            }
            if(executionMode == null) {
                executionMode = OrtSession.SessionOptions.ExecutionMode.SEQUENTIAL;
            }
            // one thread per inference: worker threads share the session and run inferences in parallel
            if(intraOpThreads == null) {
                intraOpThreads = 1;
            }
        }

        public static Config create(String path) {
            final Config config = new Config();
            config.path = path;
            config.setDefaults();
            return config;
        }

        private String sessionKey() {
            return String.join("|", path, String.valueOf(file), String.valueOf(optLevel), String.valueOf(executionMode),
                    String.valueOf(intraOpThreads), String.valueOf(interOpThreads));
        }
    }

    private OnnxModel(Config config) {
        this.config = config;
    }

    public static OnnxModel create(final Config config) {
        return new OnnxModel(config);
    }

    public Schema outputSchema() {
        return Optional
                .ofNullable(Schema.parse(config.schema))
                .orElseGet(() -> {
                    final OrtSession session = getOrCreateSession(config);
                    try {
                        return OnnxToElementConverter.convertOutputSchema(session.getOutputInfo(), null);
                    } catch (OrtException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    public Schema inputSchema() {
        final OrtSession session = getOrCreateSession(config);
        try {
            return OnnxToElementConverter.convertOutputSchema(session.getInputInfo(), null);
        } catch (OrtException e) {
            throw new RuntimeException(e);
        }
    }

    public Map<String,NodeInfo> getInputInfo() {
        final OrtSession session = getOrCreateSession(config);
        try {
            return session.getInputInfo();
        } catch (OrtException e) {
            throw new RuntimeException(e);
        }
    }

    public Map<String,NodeInfo> getOutputInfo() {
        final OrtSession session = getOrCreateSession(config);
        try {
            return session.getOutputInfo();
        } catch (OrtException e) {
            throw new RuntimeException(e);
        }
    }

    public OnnxModel setup() {
        getOrCreateSession(config);
        return this;
    }

    public OrtSession.Result run(final Map<String, ? extends OnnxTensorLike> input) {
        return run(input, null, null);
    }

    public OrtSession.Result run(
            final Map<String, ? extends OnnxTensorLike> input,
            final OrtSession.RunOptions runOptions,
            Set<String> requestedOutputs) {

        final OrtSession session = getOrCreateSession(config);

        if(requestedOutputs == null) {
            requestedOutputs = session.getOutputNames();
        }

        try {
            return session.run(input, requestedOutputs, runOptions);
        } catch (final OrtException e) {
            throw new RuntimeException(e);
        }
    }

    public void close() {
        closeSession(config.sessionKey());
    }

    private static OrtSession getOrCreateSession(final Config config) {
        final OrtSession session = sessions.get(config.sessionKey());
        if(session != null) {
            return session;
        }
        return createSession(config);
    }

    synchronized static private OrtSession createSession(
            final Config config) {

        final String key = config.sessionKey();
        if(sessions.containsKey(key)) {
            LOG.info("setup onnx module: {} skipped creating session", key);
            return sessions.get(key);
        }

        try (final OrtSession.SessionOptions sessionOptions = new OrtSession.SessionOptions()) {
            sessionOptions.setOptimizationLevel(config.optLevel);
            sessionOptions.setExecutionMode(config.executionMode);
            if(config.intraOpThreads != null) {
                sessionOptions.setIntraOpNumThreads(config.intraOpThreads);
            }
            if(config.interOpThreads != null) {
                sessionOptions.setInterOpNumThreads(config.interOpThreads);
            }
            try {
                sessionOptions.registerCustomOpLibrary(getExtensionsLibraryPath());
            } catch (Throwable e) {
                LOG.warn("setup onnx module: {} is creating session. skipped to load extensions library", key);
            }

            final OrtEnvironment environment = OrtEnvironment.getEnvironment();
            final OrtSession session = loadOnnxFiles(environment, sessionOptions, config.path, config.file);
            sessions.put(key, session);
            LOG.info("setup onnx module: {} created session", key);
            LOG.info("onnx model: {}", description(session));
            return session;
        } catch (final OrtException e) {
            throw new RuntimeException("Failed to create onnxruntime session", e);
        }
    }

    private static OrtSession loadOnnxFiles(
            final OrtEnvironment environment,
            final OrtSession.SessionOptions sessionOptions,
            final String path,
            final String file) {

        try {
            if (path.startsWith("gs://")) {
                final Storage storage = StorageUtil.storage();
                if (path.endsWith("/")) {
                    // a directory keeps its layout so that external data files (*.onnx_data) resolve next to the model
                    final Path localDir = Files.createTempDirectory("onnxruntime-");
                    final String prefix = StorageUtil.parseGcsPath(path)[1];
                    for (final StorageObject object : StorageUtil.listFiles(path)) {
                        if (object.getName().endsWith("/")) {
                            continue;
                        }
                        final Path localFile = localDir.resolve(object.getName().substring(prefix.length()));
                        Files.createDirectories(localFile.getParent());
                        try (final OutputStream os = new BufferedOutputStream(Files.newOutputStream(localFile))) {
                            StorageUtil.downloadTo(storage, object, os);
                        }
                    }
                    return environment.createSession(resolveModelFile(localDir, file).toString(), sessionOptions);
                } else {
                    final byte[] bytes = StorageUtil.readBytes(storage, path);
                    return environment.createSession(bytes, sessionOptions);
                }
            } else if (Files.isDirectory(Path.of(path))) {
                return environment.createSession(resolveModelFile(Path.of(path), file).toString(), sessionOptions);
            } else if (Files.exists(Path.of(path))) {
                if (file != null) {
                    throw new IllegalArgumentException("onnx model.file requires model.path to be a directory: " + path);
                }
                return environment.createSession(path, sessionOptions);
            } else {
                throw new IllegalArgumentException("onnx file path is illegal: " + path);
            }
        } catch (IOException | OrtException e) {
            throw new RuntimeException("Failed to load onnx model: " + path, e);
        }
    }

    /** The model file in a directory: {@code file} relative to it, else the only {@code .onnx} file under it. */
    static Path resolveModelFile(final Path dir, final String file) throws IOException {
        if (file != null) {
            final Path modelFile = dir.resolve(file);
            if (!Files.isRegularFile(modelFile)) {
                throw new IllegalArgumentException("onnx model.file: " + file + " is not found in model.path directory");
            }
            return modelFile;
        }
        final List<Path> candidates;
        try (final Stream<Path> stream = Files.walk(dir)) {
            candidates = stream
                    .filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString().endsWith(".onnx"))
                    .sorted()
                    .toList();
        }
        if (candidates.size() != 1) {
            throw new IllegalArgumentException("onnx model.path directory must contain exactly one .onnx file when model.file is not specified, but found: "
                    + candidates.stream().map(p -> dir.relativize(p).toString().replace('\\', '/')).toList());
        }
        return candidates.getFirst();
    }

    synchronized static private void closeSession(final String key) {
        if(sessions.containsKey(key)) {
            try(final OrtSession session = sessions.remove(key)) {
                LOG.info("teardown onnx module: {} closed session", key);
            } catch (final OrtException e) {
                LOG.error("teardown onnx module: {} failed to close session cause: {}", key, e.getMessage());
            }
        }
    }

    public static String description(final OrtSession session) {
        try {
            final JsonObject message = new JsonObject();
            message.addProperty("numInputs", session.getNumInputs());
            message.addProperty("numOutputs", session.getNumOutputs());

            final JsonObject inputInfo = new JsonObject();
            for (final Map.Entry<String, NodeInfo> entry : session.getInputInfo().entrySet()) {
                final JsonObject nodeInfo = new JsonObject();
                nodeInfo.addProperty("name", entry.getValue().getName());
                nodeInfo.addProperty("info", entry.getValue().getInfo().toString());
                inputInfo.add(entry.getKey(), nodeInfo);
            }
            message.add("inputInfo", inputInfo);

            final JsonObject outputInfo = new JsonObject();
            for (final Map.Entry<String, NodeInfo> entry : session.getOutputInfo().entrySet()) {
                final JsonObject nodeInfo = new JsonObject();
                nodeInfo.addProperty("name", entry.getValue().getName());
                nodeInfo.addProperty("info", entry.getValue().getInfo().toString());
                outputInfo.add(entry.getKey(), nodeInfo);
            }
            message.add("outputInfo", outputInfo);

            final JsonObject metadata = new JsonObject();
            metadata.addProperty("domain", session.getMetadata().getDomain());
            metadata.addProperty("description", session.getMetadata().getDescription());
            metadata.addProperty("graphName", session.getMetadata().getGraphName());
            metadata.addProperty("graphDescription", session.getMetadata().getGraphDescription());
            metadata.addProperty("producerName", session.getMetadata().getProducerName());
            metadata.addProperty("version", session.getMetadata().getVersion());
            final JsonObject customMetadata = new JsonObject();
            for (final Map.Entry<String, String> entry : session.getMetadata().getCustomMetadata().entrySet()) {
                customMetadata.addProperty(entry.getKey(), entry.getValue());
            }
            metadata.add("customMetadata", customMetadata);

            message.add("metadata", metadata);

            return message.toString();
        } catch (OrtException e) {
            throw new IllegalStateException(e);
        }
    }

    private static String getExtensionsLibraryPath() {
        try {
            final Object result = Class.forName("ai.onnxruntime.extensions.OrtxPackage")
                    .getMethod("getLibraryPath")
                    .invoke(null);
            return (String) result;
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new IllegalStateException("Failed to refer onnxruntime extensions package", e);
        }
    }

    public static OnnxTensor convertNDArrayToOnnxTensor(
            final OrtEnvironment env,
            final NDArray array) throws OrtException {
        long[] shape = array.getShape().getShape();
        final ByteBuffer byteBuffer = array.toByteBuffer();
        return switch (array.getDataType()) {
            case FLOAT32 -> OnnxTensor.createTensor(env, byteBuffer.asFloatBuffer(), shape);
            case FLOAT64 -> OnnxTensor.createTensor(env, byteBuffer.asDoubleBuffer(), shape);
            case INT32 -> OnnxTensor.createTensor(env, byteBuffer.asIntBuffer(), shape);
            case INT64 -> OnnxTensor.createTensor(env, byteBuffer.asLongBuffer(), shape);
            /*
            case INT64 -> {
                int[] intArray = new int[(int) array.size()];
                long[] longArray = array.toLongArray();
                for (int i = 0; i < longArray.length; i++) {
                    intArray[i] = (int) longArray[i];
                }
                yield OnnxTensor.createTensor(env, IntBuffer.wrap(intArray), shape);
            }

             */
            default -> throw new IllegalArgumentException("Unsupported data type: " + array.getDataType());
        };
    }

    public static int argmax(final OnnxTensor tensor) {
        int[] index = new int[tensor.getInfo().getShape().length-1];
        for(int i=0; i<index.length; i++) {
            index[i] = Long.valueOf(tensor.getInfo().getShape()[i] - 1).intValue();
        }
        return argmax(tensor, index);
    }

    public static int argmax(final OnnxTensor tensor, int[] index_) {
        int[] index = new int[index_.length];
        System.arraycopy(index_, 0, index, 0, index_.length);
        if(index.length != tensor.getInfo().getShape().length - 1) {
            throw new IllegalArgumentException();
        }
        for(int i=0; i<index.length; i++) {
            if(index[i] == -1) {
                index[i] = Long.valueOf(tensor.getInfo().getShape()[i] - 1).intValue();
            } else if(index[i] >= tensor.getInfo().getShape()[i]) {
                throw new IllegalArgumentException();
            }
        }
        return switch (tensor.getInfo().type) {
            case INT8 -> {
                final Object object = OrtUtil.reshape(tensor.getByteBuffer().array(), tensor.getInfo().getShape());
                byte[] values = switch (tensor.getInfo().getShape().length) {
                    case 1 -> (byte[]) object;
                    case 2 -> ((byte[][]) object)[index[0]];
                    case 3 -> ((byte[][][]) object)[index[0]][index[1]];
                    case 4 -> ((byte[][][][]) object)[index[0]][index[1]][index[2]];
                    case 5 -> ((byte[][][][][]) object)[index[0]][index[1]][index[2]][index[3]];
                    default -> throw new IllegalArgumentException();
                };
                yield argmax(values);
            }
            case INT16 -> {
                final Object object = OrtUtil.reshape(tensor.getShortBuffer().array(), tensor.getInfo().getShape());
                short[] values = switch (tensor.getInfo().getShape().length) {
                    case 1 -> (short[]) object;
                    case 2 -> ((short[][]) object)[index[0]];
                    case 3 -> ((short[][][]) object)[index[0]][index[1]];
                    case 4 -> ((short[][][][]) object)[index[0]][index[1]][index[2]];
                    case 5 -> ((short[][][][][]) object)[index[0]][index[1]][index[2]][index[3]];
                    default -> throw new IllegalArgumentException();
                };
                yield argmax(values);
            }
            case INT32 -> {
                final Object object = OrtUtil.reshape(tensor.getIntBuffer().array(), tensor.getInfo().getShape());
                int[] values = switch (tensor.getInfo().getShape().length) {
                    case 1 -> (int[]) object;
                    case 2 -> ((int[][]) object)[index[0]];
                    case 3 -> ((int[][][]) object)[index[0]][index[1]];
                    case 4 -> ((int[][][][]) object)[index[0]][index[1]][index[2]];
                    case 5 -> ((int[][][][][]) object)[index[0]][index[1]][index[2]][index[3]];
                    default -> throw new IllegalArgumentException();
                };
                yield argmax(values);
            }
            case INT64 -> {
                final Object object = OrtUtil.reshape(tensor.getLongBuffer().array(), tensor.getInfo().getShape());
                long[] values = switch (tensor.getInfo().getShape().length) {
                    case 1 -> (long[]) object;
                    case 2 -> ((long[][]) object)[index[0]];
                    case 3 -> ((long[][][]) object)[index[0]][index[1]];
                    case 4 -> ((long[][][][]) object)[index[0]][index[1]][index[2]];
                    case 5 -> ((long[][][][][]) object)[index[0]][index[1]][index[2]][index[3]];
                    default -> throw new IllegalArgumentException();
                };
                yield argmax(values);
            }
            case FLOAT, FLOAT16, BFLOAT16 -> {
                final Object object = OrtUtil.reshape(tensor.getFloatBuffer().array(), tensor.getInfo().getShape());
                float[] values = switch (tensor.getInfo().getShape().length) {
                    case 1 -> (float[]) object;
                    case 2 -> ((float[][]) object)[index[0]];
                    case 3 -> ((float[][][]) object)[index[0]][index[1]];
                    case 4 -> ((float[][][][]) object)[index[0]][index[1]][index[2]];
                    case 5 -> ((float[][][][][]) object)[index[0]][index[1]][index[2]][index[3]];
                    default -> throw new IllegalArgumentException();
                };
                yield argmax(values);
            }
            case DOUBLE -> {
                final Object object = OrtUtil.reshape(tensor.getDoubleBuffer().array(), tensor.getInfo().getShape());
                double[] values = switch (tensor.getInfo().getShape().length) {
                    case 1 -> (double[]) object;
                    case 2 -> ((double[][]) object)[index[0]];
                    case 3 -> ((double[][][]) object)[index[0]][index[1]];
                    case 4 -> ((double[][][][]) object)[index[0]][index[1]][index[2]];
                    case 5 -> ((double[][][][][]) object)[index[0]][index[1]][index[2]][index[3]];
                    default -> throw new IllegalArgumentException();
                };
                yield argmax(values);
            }
            default -> throw new IllegalArgumentException("Not supported type: " + tensor.getInfo().type);
        };
    }

    public static int argmax(byte[] values) {
        if(values == null || values.length == 0) {
            return -1;
        }
        int maxIndex = 0;
        byte maxValue = values[0];
        for(int i=1; i<values.length; i++) {
            if(values[i] > maxValue) {
                maxValue = values[i];
                maxIndex = i;
            }
        }
        return maxIndex;
    }

    public static int argmax(short[] values) {
        if(values == null || values.length == 0) {
            return -1;
        }
        int maxIndex = 0;
        short maxValue = values[0];
        for(int i=1; i<values.length; i++) {
            if(values[i] > maxValue) {
                maxValue = values[i];
                maxIndex = i;
            }
        }
        return maxIndex;
    }

    public static int argmax(int[] values) {
        if(values == null || values.length == 0) {
            return -1;
        }
        int maxIndex = 0;
        int maxValue = values[0];
        for(int i=1; i<values.length; i++) {
            if(values[i] > maxValue) {
                maxValue = values[i];
                maxIndex = i;
            }
        }
        return maxIndex;
    }

    public static int argmax(long[] values) {
        if(values == null || values.length == 0) {
            return -1;
        }
        int maxIndex = 0;
        long maxValue = values[0];
        for(int i=1; i<values.length; i++) {
            if(values[i] > maxValue) {
                maxValue = values[i];
                maxIndex = i;
            }
        }
        return maxIndex;
    }

    public static int argmax(float[] values) {
        if(values == null || values.length == 0) {
            return -1;
        }
        int maxIndex = 0;
        float maxValue = values[0];
        for(int i=1; i<values.length; i++) {
            if(values[i] > maxValue) {
                maxValue = values[i];
                maxIndex = i;
            }
        }
        return maxIndex;
    }

    public static int argmax(double[] values) {
        if(values == null || values.length == 0) {
            return -1;
        }
        int maxIndex = 0;
        double maxValue = values[0];
        for(int i=1; i<values.length; i++) {
            if(values[i] > maxValue) {
                maxValue = values[i];
                maxIndex = i;
            }
        }
        return maxIndex;
    }

}
