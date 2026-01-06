package com.mercari.solution.util.domain.ml.onnx;

import ai.onnxruntime.genai.*;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.StorageObject;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.TemplateUtil;
import com.mercari.solution.util.gcp.StorageUtil;
import freemarker.template.Template;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public class OnnxGenModel implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(OnnxGenModel.class);

    private static final String ONNX_BASE_DIR = "/onnxruntime-genai/";

    private static final Map<String, SimpleGenAI> genAIs = new HashMap<>();

    private final String path;
    private final Map<String, Double> searchOptions;
    private final Map<String, Boolean> searchFlags;
    private final String prompt;

    private final Schema inputSchema;
    private final List<String> templateArgs;

    private transient Template promptTemplate;

    OnnxGenModel(
            final String path,
            final String prompt,
            final Map<String, Double> searchOptions,
            final Map<String, Boolean> searchFlags,
            final Schema inputSchema) {

        this.path = path + (path.endsWith("/") ? "" : "/");
        this.prompt = prompt;
        this.inputSchema = inputSchema;
        this.templateArgs = TemplateUtil.extractTemplateArgs(prompt, inputSchema);

        this.searchOptions = Optional.ofNullable(searchOptions).orElseGet(HashMap::new);
        this.searchFlags = Optional.ofNullable(searchFlags).orElseGet(HashMap::new);
    }

    public static OnnxGenModel of(
            final String path,
            final String prompt,
            final Schema inputSchema) {

        return new OnnxGenModel(path, prompt, null, null, inputSchema);
    }

    public static OnnxGenModel of(
            final String path,
            final String prompt,
            final Map<String, Double> searchOptions,
            final Map<String, Boolean> searchFlags,
            final Schema inputSchema) {

        return new OnnxGenModel(path, prompt, searchOptions, searchFlags, inputSchema);
    }

    public void setup() {
        getOrLoadGen(genAIs, path);
        this.promptTemplate = TemplateUtil.createStrictTemplate("OnnxRuntimeGen", prompt);
    }

    public void teardown() {

    }

    public String process(final MElement input) {
        final Map<String, Object> values = input.asStandardMap(inputSchema, templateArgs);
        final String s = TemplateUtil.executeStrictTemplate(this.promptTemplate, values);
        LOG.info("template: {}", s);
        return process(s);
    }

    public String process(final String prompt) {
        final SimpleGenAI simpleGen = Optional
                .ofNullable(genAIs.get(path))
                .orElseGet(() -> getOrLoadGen(genAIs, path));
        try(final GeneratorParams params = simpleGen.createGeneratorParams()) {
            for(final Map.Entry<String, Double> entry : searchOptions.entrySet()) {
                params.setSearchOption(entry.getKey(), entry.getValue());
            }
            for(final Map.Entry<String, Boolean> entry : searchFlags.entrySet()) {
                params.setSearchOption(entry.getKey(), entry.getValue());
            }

            //var r = new MultiModalProcessor(simpleGen.getModel()).processImages("", new Images(""));

            //new NamedTensors()
            //simpleGen.createGeneratorParams().set
            //Tensor te = new Tensor(ByteBuffer.wrap(new byte[1]), new long[0], Tensor.ElementType.float16);

            //params.setInput("", te);
            //MultiModalProcessor processor = new MultiModalProcessor(simpleGen).processImages();
            //processor.processImages()

            return simpleGen.generate(params, prompt, token -> {});
        } catch (final GenAIException e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized static SimpleGenAI getOrLoadGen(
            final Map<String, SimpleGenAI> genAIs,
            final String path) {

        if(genAIs.containsKey(path)) {
            final SimpleGenAI genAI = genAIs.get(path);
            if(genAI != null) {
                return genAI;
            }
        }
        loadOnnxGenFiles(genAIs, path);
        return genAIs.get(path);
    }

    public synchronized static void loadOnnxGenFiles(
            final Map<String, SimpleGenAI> genAIs,
            final String path) {

        //if(genAIs.containsKey(path) && genAIs.get(path) == null) {
        //    genAIs.remove(path);
        //}

        if(genAIs.containsKey(path)) {
            return;
        }

        try {
            final SimpleGenAI genAI;
            if (path.startsWith("gs://")) {
                final String localDirPath = "/onnxruntime-genai/" + UUID.randomUUID() + "/";
                Files.createDirectories(Path.of(localDirPath));

                final Storage storage = StorageUtil.storage();
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
                genAI = new SimpleGenAI(localDirPath);
                //Files.deleteIfExists(Path.of(localDirPath));
            } else if (Files.exists(Path.of(path))) {
                genAI = new SimpleGenAI(path);
            } else {
                throw new RuntimeException("onnx file path is illegal: " + path);
            }
            genAIs.put(path, genAI);

            LOG.info("loaded onnx genai files: {} loaded", path);
        } catch (IOException e) {
            throw new RuntimeException("failed to load onnx file: " + path, e);
        } catch (GenAIException e) {
            throw new RuntimeException("Failed to init genAI", e);
        }
    }

    public static String createLocalPath(final String modelPath) {
        final String fileName = getFileName(modelPath);
        return String.format("%s%s/", ONNX_BASE_DIR, fileName);
    }

    public static String getFileName(String path) {
        final String[] paths = path.split("/");
        return paths[paths.length - 1];
    }

}
