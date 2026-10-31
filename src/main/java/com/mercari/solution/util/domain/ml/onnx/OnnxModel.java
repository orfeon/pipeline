package com.mercari.solution.util.domain.ml.onnx;

import ai.djl.ndarray.NDArray;
import ai.onnxruntime.*;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.StorageObject;
import com.google.gson.JsonObject;
import com.mercari.solution.util.cloud.google.StorageUtil;
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
            return errorMessages;
        }

        public void setDefaults() {
            if(optLevel == null) {
                // as ONNX Runtime does: with a lower one, a float16 quantized model on a CPU gave wrong logits without an error
                optLevel = OrtSession.SessionOptions.OptLevel.ALL_OPT;
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

    /**
     * The inputs and outputs of the model, without creating a session or reading the weights, unless the graph leaves out
     * the type or the shape of a value: only a session infers those.
     */
    public OnnxGraphInfo readGraphInfo() {
        final OnnxGraphInfo graph = readDeclaredGraphInfo();
        return graph.isComplete() ? graph : OnnxGraphInfo.of(getInputInfo(), getOutputInfo());
    }

    private OnnxGraphInfo readDeclaredGraphInfo() {
        final String path = config.path;
        final String file = config.file;
        try {
            if (path.startsWith("gs://")) {
                return readGraphInfo(StorageUtil.storage(), path, file);
            }
            final Path localPath = Path.of(path);
            final Path modelFile;
            if (Files.isDirectory(localPath)) {
                modelFile = resolveModelFile(localPath, file);
            } else if (Files.exists(localPath)) {
                if (file != null) {
                    throw new IllegalArgumentException("onnx model.file requires model.path to be a directory: " + path);
                }
                modelFile = localPath;
            } else {
                throw new IllegalArgumentException("onnx file path is illegal: " + path);
            }
            try (final InputStream stream = new BufferedInputStream(Files.newInputStream(modelFile))) {
                return OnnxGraphInfo.read(stream);
            }
        } catch (final IOException e) {
            throw new IllegalArgumentException("Failed to read onnx model: " + path, e);
        }
    }

    static OnnxGraphInfo readGraphInfo(final Storage storage, final String path, final String file) throws IOException {
        final String modelPath;
        if (path.endsWith("/")) {
            final String[] paths = StorageUtil.parseGcsPath(path);
            final List<String> names = new ArrayList<>();
            for (final StorageObject object : StorageUtil.listFiles(storage, paths[0], paths[1], null)) {
                if (!object.getName().endsWith("/")) {
                    names.add(object.getName().substring(paths[1].length()));
                }
            }
            modelPath = path + selectModelFile(names, file);
        } else {
            modelPath = path;
        }
        try (final InputStream stream = new BufferedInputStream(StorageUtil.readStream(storage, modelPath))) {
            return OnnxGraphInfo.read(stream);
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
                    StorageUtil.downloadDirectory(storage, path, localDir, name -> true);
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
        final List<String> names;
        try (final Stream<Path> stream = Files.walk(dir)) {
            names = stream
                    .filter(Files::isRegularFile)
                    .map(p -> dir.relativize(p).toString().replace('\\', '/'))
                    .toList();
        }
        return dir.resolve(selectModelFile(names, file));
    }

    private static String selectModelFile(final List<String> names, final String file) {
        if (file != null) {
            if (!names.contains(file)) {
                throw new IllegalArgumentException("onnx model.file: " + file + " is not found in model.path directory");
            }
            return file;
        }
        final List<String> candidates = names.stream().filter(name -> name.endsWith(".onnx")).sorted().toList();
        if (candidates.size() != 1) {
            throw new IllegalArgumentException("onnx model.path directory must contain exactly one .onnx file when model.file is not specified, but found: " + candidates);
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

}
