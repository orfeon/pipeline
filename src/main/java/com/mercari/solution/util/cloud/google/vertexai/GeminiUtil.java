package com.mercari.solution.util.cloud.google.vertexai;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.schema.OpenApiSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class GeminiUtil {

    private static final Logger LOG = LoggerFactory.getLogger(GeminiUtil.class);

    private static final String ENDPOINT_BATCH_PREDICTION = "https://%s-aiplatform.googleapis.com/v1/projects/%s/locations/%s/batchPredictionJobs";

    public static class BatchPredictionJobsRequest implements Serializable {
        public String displayName;
        public String model;
        public InputConfig inputConfig;
        public OutputConfig outputConfig;

        public List<String> validate() {
            final List<String> errorMessages = new ArrayList<>();
            if(inputConfig == null) {
                errorMessages.add("batchPredictionJobsRequest.inputConfig must not be null");
            } else {
                errorMessages.addAll(inputConfig.validate());
            }
            if(outputConfig == null) {
                errorMessages.add("batchPredictionJobsRequest.outputConfig must not be null");
            } else {
                errorMessages.addAll(outputConfig.validate());
            }
            return errorMessages;
        }

        public void setDefaults() {
            if(inputConfig != null) {
                inputConfig.setDefaults();
            }
            if(outputConfig != null) {
                outputConfig.setDefaults();
            }
        }

        public JsonObject toJson() {
            final JsonObject requestJson = new JsonObject();
            requestJson.addProperty("displayName", this.displayName);
            requestJson.addProperty("model", this.model);
            requestJson.add("inputConfig", inputConfig.toJson());
            requestJson.add("outputConfig", outputConfig.toJson());
            return requestJson;
        }
    }

    public static class InputConfig implements Serializable {

        public String instancesFormat;
        public GcsSource gcsSource;
        public BigQuerySource bigquerySource;

        public List<String> validate() {
            final List<String> errorMessages = new ArrayList<>();

            return errorMessages;
        }

        public void setDefaults() {
            if(this.instancesFormat == null) {
                if(gcsSource != null) {
                    this.instancesFormat = "jsonl";
                } else if(bigquerySource != null) {
                    this.instancesFormat = "bigquery";
                } else {
                    throw new IllegalArgumentException("The both gcsSource and bigquerySource are null");
                }
            }
            if(gcsSource != null) {
                gcsSource.setDefaults();
            }
            if(bigquerySource != null) {
                bigquerySource.setDefaults();
            }
        }

        public JsonObject toJson() {
            final JsonObject inputConfigJson = new JsonObject();
            inputConfigJson.addProperty("instancesFormat", this.instancesFormat);
            if(gcsSource != null) {
                inputConfigJson.add("gcsSource", this.gcsSource.toJson());
            } else if(bigquerySource != null) {
                inputConfigJson.add("bigquerySource", this.bigquerySource.toJson());
            }
            return inputConfigJson;
        }

    }

    public static class GcsSource implements Serializable {

        public String inputUris;

        public void setDefaults() {
        }

        public JsonObject toJson() {
            final JsonObject gcsSourceJson = new JsonObject();
            gcsSourceJson.addProperty("inputUris", this.inputUris);
            return gcsSourceJson;
        }

    }

    public static class BigQuerySource implements Serializable {

        public String inputUri;

        public void setDefaults() {
        }

        public JsonObject toJson() {
            final JsonObject bigquerySourceJson = new JsonObject();
            bigquerySourceJson.addProperty("inputUri", this.inputUri);
            return bigquerySourceJson;
        }

    }

    public static class OutputConfig implements Serializable {

        public String predictionsFormat;
        public GcsDestination gcsDestination;
        public BigQueryDestination bigqueryDestination;

        public List<String> validate() {
            final List<String> errorMessages = new ArrayList<>();

            return errorMessages;
        }

        public void setDefaults() {
            if(predictionsFormat == null) {
                if(gcsDestination != null) {
                    predictionsFormat = "jsonl";
                } else if(bigqueryDestination != null) {
                    predictionsFormat = "bigquery";
                }
            }
            if(gcsDestination != null) {
                gcsDestination.setDefaults();
            }
            if(bigqueryDestination != null) {
                bigqueryDestination.setDefaults();
            }
        }

        public JsonObject toJson() {
            final JsonObject outputConfigJson = new JsonObject();
            outputConfigJson.addProperty("predictionsFormat", this.predictionsFormat);
            if(gcsDestination != null) {
                outputConfigJson.add("gcsDestination", this.gcsDestination.toJson());
            } else if(bigqueryDestination != null) {
                outputConfigJson.add("bigqueryDestination", this.bigqueryDestination.toJson());
            }
            return outputConfigJson;
        }

    }

    public static class GcsDestination implements Serializable {

        public String outputUriPrefix;

        public void setDefaults() {
        }

        public JsonObject toJson() {
            final JsonObject gcsDestinationJson = new JsonObject();
            gcsDestinationJson.addProperty("outputUriPrefix", this.outputUriPrefix);
            return gcsDestinationJson;
        }

    }

    public static class BigQueryDestination implements Serializable {

        public String outputUri;

        public void setDefaults() {
        }

        public JsonObject toJson() {
            final JsonObject bigqueryDestinationJson = new JsonObject();
            bigqueryDestinationJson.addProperty("outputUri", this.outputUri);
            return bigqueryDestinationJson;
        }

    }

    public static class BatchPredictionJobsResponse implements Serializable {
        public List<Content> contents;
        public Content systemInstruction;
        public GenerationConfig generationConfig;
        public SafetySetting safetySetting;
    }

    public static class Content implements Serializable {

        public String role;
        public List<Part> parts;

        public List<String> validate(String prefix) {
            final List<String> errorMessages = new ArrayList<>();
            if(role == null) {
                errorMessages.add(prefix + ".role must not be null");
            } else if(!"user".equals(role) && !"model".equals(role)) {
                errorMessages.add(prefix + ".role must be 'user' or 'model'");
            }

            if(parts == null || parts.isEmpty()) {
                errorMessages.add(prefix + ".parts must not be empty");
            } else {
                for(int i=0; i<parts.size(); i++) {
                    final String prefix2 = prefix + ".parts[" + i + "]";
                    errorMessages.addAll(parts.get(i).validate(prefix2));
                }
            }

            return errorMessages;
        }

        public JsonObject toJson() {
            final JsonObject contentJson = new JsonObject();
            contentJson.addProperty("role", role);
            final JsonArray partArray = new JsonArray();
            for(final Part part : parts) {
                final JsonObject partJson = part.toJson();
                partArray.add(partJson);
            }
            contentJson.add("parts", partArray);
            return contentJson;
        }

        public static Schema createSchema() {
            return Schema.builder()
                    .withField("role", Schema.FieldType.STRING)
                    .withField("parts", Schema.FieldType.array(Schema.FieldType.element(Part.createSchema())))
                    .build();
        }

    }

    public static class Part implements Serializable {
        public String text;
        public Blob inlineData;
        public FileData fileData;
        public FunctionCall functionCall;
        public FunctionResponse functionResponse;
        public VideoMetadata videoMetadata;

        public List<String> validate(String prefix) {
            final List<String> errorMessages = new ArrayList<>();
            if(text == null && inlineData == null && fileData == null) {
                errorMessages.add(prefix + ".text/inlineData/fileData must not be null");
            }

            if(inlineData != null) {
                errorMessages.addAll(inlineData.validate(prefix + ".inlineData"));
            }

            if(fileData != null) {
                errorMessages.addAll(fileData.validate(prefix + ".fileData"));
            }

            return errorMessages;
        }

        public JsonObject toJson() {
            final JsonObject partJson = new JsonObject();
            if(text != null && !text.isEmpty()) {
                partJson.addProperty("text", text);
            } else if(inlineData != null) {
                partJson.add("inlineData", inlineData.toJson());
            } else if(fileData != null) {
                partJson.add("fileData", fileData.toJson());
            }
            return partJson;
        }

        public static Schema createSchema() {
            return Schema.builder()
                    .withField("text", Schema.FieldType.STRING)
                    .withField("inlineData", Schema.FieldType.element(Blob.createSchema()))
                    .withField("fileData", Schema.FieldType.element(FileData.createSchema()))
                    //.withField("functionCall", Schema.FieldType.element(FunctionCall.createSchema()))
                    //.withField("functionResponse", Schema.FieldType.element())
                    //.withField("videoMetadata", Schema.FieldType.element(VideoMetadata.createSchema()))
                    .build();
        }
    }

    public static class GenerationConfig implements Serializable {

        public Double temperature;
        public Double topP;
        public Double topK;
        public Integer candidateCount;
        public Integer maxOutputTokens;
        public List<String> stopSequences;
        public Double presencePenalty;
        public Double frequencyPenalty;
        public String responseMimeType;
        public OpenApiSchema responseSchema;

        public List<String> validate(String prefix) {
            final List<String> errorMessages = new ArrayList<>();

            return errorMessages;
        }

        public JsonObject toJson() {
            final JsonObject generationConfigJson = new JsonObject();
            if(responseMimeType != null) {
                generationConfigJson.addProperty("responseMimeType", responseMimeType);
            }
            if(responseSchema != null) {
                generationConfigJson.add("responseSchema", responseSchema.toJson());
            }
            return generationConfigJson;
        }

        public static Schema createSchema() {
            return Schema.builder()
                    .withField("temperature", Schema.FieldType.FLOAT64)
                    .withField("topP", Schema.FieldType.FLOAT64)
                    .withField("topK", Schema.FieldType.FLOAT64)
                    .withField("candidateCount", Schema.FieldType.INT32)
                    .withField("maxOutputTokens", Schema.FieldType.INT32)
                    .withField("stopSequences", Schema.FieldType.array(Schema.FieldType.STRING))
                    .withField("presencePenalty", Schema.FieldType.FLOAT64)
                    .withField("frequencyPenalty", Schema.FieldType.FLOAT64)
                    .withField("responseMimeType", Schema.FieldType.STRING)
                    .withField("responseSchema", Schema.FieldType.element(OpenApiSchema.createSchema()))
                    .build();
        }

    }

    public static class Blob {
        public String mimeType;
        public String data;

        public List<String> validate(String prefix) {
            final List<String> errorMessages = new ArrayList<>();
            if(mimeType == null) {
                errorMessages.add(prefix + ".mimeType must not be null");
            }
            if(data == null) {
                errorMessages.add(prefix + ".data must not be null");
            }

            return errorMessages;
        }

        public JsonObject toJson() {
            final JsonObject blobJson = new JsonObject();
            blobJson.addProperty("mimeType", mimeType);
            blobJson.addProperty("data", data);
            return blobJson;
        }

        public static Schema createSchema() {
            return Schema.builder()
                    .withField("mimeType", Schema.FieldType.STRING)
                    .withField("data", Schema.FieldType.STRING)
                    .build();
        }
    }

    public static class FileData {
        public String mimeType;
        public String fileUri;

        public List<String> validate(String prefix) {
            final List<String> errorMessages = new ArrayList<>();
            if(mimeType == null) {
                errorMessages.add(prefix + ".mimeType must not be null");
            }
            if(fileUri == null) {
                errorMessages.add(prefix + ".fileUri must not be null");
            }

            return errorMessages;
        }

        public JsonObject toJson() {
            final JsonObject fileDataJson = new JsonObject();
            fileDataJson.addProperty("mimeType", mimeType);
            fileDataJson.addProperty("fileUri", fileUri);
            return fileDataJson;
        }

        public static Schema createSchema() {
            return Schema.builder()
                    .withField("mimeType", Schema.FieldType.STRING)
                    .withField("fileUri", Schema.FieldType.STRING)
                    .build();
        }
    }

    public static class FunctionCall {
        public String name;
        public String args;
    }

    public static class FunctionResponse {
        public String name;
        public String response;
    }

    public static class VideoMetadata implements Serializable {

    }

    public static class SafetySetting implements Serializable {

        public HarmCategory category;
        public HarmBlockThreshold threshold;
        public Integer maxInfluenceTerms;
        public HarmBlockMethod method;

        public JsonObject toJson() {
            final JsonObject generationConfigJson = new JsonObject();

            return generationConfigJson;
        }

        public static Schema createSchema() {
            return Schema.builder()
                    .withField("category", Schema.FieldType.STRING)
                    .withField("threshold", Schema.FieldType.STRING)
                    .withField("maxInfluenceTerms", Schema.FieldType.INT32)
                    .withField("method", Schema.FieldType.STRING)
                    .build();
        }

    }

    public enum HarmCategory {
        HARM_CATEGORY_UNSPECIFIED,
        HARM_CATEGORY_HATE_SPEECH,
        HARM_CATEGORY_DANGEROUS_CONTENT,
        HARM_CATEGORY_HARASSMENT,
        HARM_CATEGORY_SEXUALLY_EXPLICIT
    }

    public enum HarmBlockThreshold {
        HARM_BLOCK_THRESHOLD_UNSPECIFIED,
        BLOCK_LOW_AND_ABOVE,
        BLOCK_MEDIUM_AND_ABOVE,
        BLOCK_ONLY_HIGH,
        BLOCK_NONE
    }

    public enum HarmBlockMethod {
        HARM_BLOCK_METHOD_UNSPECIFIED,
        SEVERITY,
        PROBABILITY
    }

    public static BatchPredictionJobsResponse batchPredictionJobs(
            final HttpClient client,
            final String token,
            final String project,
            final String region,
            final BatchPredictionJobsRequest request) {

        final JsonObject body = request.toJson();
        return batchPredictionJobs(client, token, project, region, body);
    }

    public static BatchPredictionJobsResponse batchPredictionJobs(
            final HttpClient client,
            final String token,
            final String project,
            final String region,
            final JsonObject request) {

        final String endpoint = String.format(ENDPOINT_BATCH_PREDICTION, region, project, region);
        try {
            final HttpRequest req = HttpRequest.newBuilder()
                    .uri(new URI(endpoint))
                    .header("Content-Type", "application/json")
                    .header("Authorization", "Bearer " + token)
                    .POST(HttpRequest.BodyPublishers.ofString(request.toString(), StandardCharsets.UTF_8))
                    .build();
            final HttpResponse<String> res = client.send(req, HttpResponse.BodyHandlers.ofString());
            if(res.statusCode() >= 400) {
                throw new RuntimeException("Failed to batchPredictionJobs code: " + res.statusCode() + ", body: " + res.body());
            }
            System.out.println("response: " + res.body());
            final JsonObject responseJson = new Gson().fromJson(res.body(), JsonObject.class);

            return null;
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

}
