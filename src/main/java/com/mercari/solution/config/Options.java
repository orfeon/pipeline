package com.mercari.solution.config;

import com.mercari.solution.MPipeline;
import com.mercari.solution.config.options.*;
import com.mercari.solution.util.cloud.SecretProviders;
import com.mercari.solution.util.pipeline.OptionUtil;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.options.ApplicationNameOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.StreamingOptions;

import java.io.Serializable;
import java.util.Optional;

public class Options implements Serializable {

    // common options
    private String jobName;
    private String userAgent;
    private Long optionsId;

    private Boolean streaming;
    private String tempLocation;

    // runner options
    private DirectOptions direct;
    private PrismOptions prism;
    private PortableOptions portable;
    private DataflowOptions dataflow;
    private FlinkOptions flink;
    private SparkOptions spark;

    // cloud options
    private GCPOptions gcp;
    private AWSOptions aws;

    private BeamSQLOptions beamsql;

    // expansion service options
    private ExpansionOptions expansion;


    public String getJobName() {
        return jobName;
    }

    public String getUserAgent() {
        return userAgent;
    }

    public Long getOptionsId() {
        return optionsId;
    }

    public Boolean getStreaming() {
        return streaming;
    }

    public String getTempLocation() {
        return tempLocation;
    }

    public DirectOptions getDirect() {
        return direct;
    }

    public PrismOptions getPrism() {
        return prism;
    }

    public PortableOptions getPortable() {
        return portable;
    }

    public DataflowOptions getDataflow() {
        return dataflow;
    }

    public FlinkOptions getFlink() {
        return flink;
    }

    public SparkOptions getSpark() {
        return spark;
    }

    public GCPOptions getGcp() {
        return gcp;
    }

    public AWSOptions getAws() {
        return aws;
    }

    public BeamSQLOptions getBeamsql() {
        return beamsql;
    }

    public ExpansionOptions getExpansion() {
        return expansion;
    }

    public static void setOptions(final PipelineOptions pipelineOptions, final Options options) {

        final String version = Optional.ofNullable(System.getenv("APP_VERSION")).orElse("-");
        pipelineOptions.as(ApplicationNameOptions.class).setAppName("Mercari Pipeline " + version);

        if(options == null) {
            return;
        }

        if(options.jobName != null) {
            pipelineOptions.setJobName(options.jobName);
        }
        if(options.userAgent != null) {
            pipelineOptions.setUserAgent(options.userAgent);
        }
        if(options.optionsId != null) {
            pipelineOptions.setOptionsId(options.optionsId);
        }

        if(options.streaming != null) {
            pipelineOptions.as(StreamingOptions.class).setStreaming(options.streaming);
        }
        if(options.tempLocation != null) {
            pipelineOptions.setTempLocation(options.tempLocation);
        }

        GCPOptions.setOptions(pipelineOptions, options.gcp);
        AWSOptions.setOptions(pipelineOptions, options.aws);

        final MPipeline.Runner runner = OptionUtil.getRunner(pipelineOptions);
        switch (runner) {
            case direct -> DirectOptions.setOptions(pipelineOptions, options.direct);
            case prism -> PrismOptions.setOptions(pipelineOptions, options.prism);
            case portable -> PortableOptions.setOptions(pipelineOptions, options.portable);
            case dataflow -> DataflowOptions.setOptions(pipelineOptions, options.dataflow);
            case flink -> FlinkOptions.setOptions(pipelineOptions, options.flink);
            case spark -> SparkOptions.setOptions(pipelineOptions, options.spark);
        }

        BeamSQLOptions.setOptions(pipelineOptions, options.beamsql);
        ExpansionOptions.setOptions(pipelineOptions, options.expansion);

        // Register filesystems (gs://, s3://) and secret resolution with the fully-wired
        // options so that assembly-time file/secret access (schema files, GCP credentials
        // source, jdbc passwords, ...) already sees the configured AWS settings.
        FileSystems.setDefaultPipelineOptions(pipelineOptions);
        SecretProviders.configure(pipelineOptions);
    }

}
