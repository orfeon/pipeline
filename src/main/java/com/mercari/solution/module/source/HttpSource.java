package com.mercari.solution.module.source;

import com.mercari.solution.module.*;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.coder.ElementCoder;
import com.mercari.solution.util.domain.web.HttpUtil;
import com.mercari.solution.util.pipeline.Http;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.*;
import org.joda.time.Instant;

import java.io.Serializable;
import java.util.*;


@Source.Module(name="http")
public class HttpSource extends Source {

    private static class Parameters implements Serializable {

        private List<HttpUtil.Request> requests;

        private HttpUtil.RetryParameters retry;
        private HttpUtil.BackoffParameters backoff;
        private HttpUtil.RateParameters rate;

        private DateTimeUtil.TimeUnit rateUnit;
        private Integer timeoutSecond;

        private Parameters validate() {
            final List<String> errorMessages = new ArrayList<>();
            if(requests == null || requests.isEmpty()) {
                errorMessages.add("parameters.requests must not be empty");
            } else {
                for(var request : requests) {
                    errorMessages.addAll(request.validate());
                }
            }
            if(!errorMessages.isEmpty()) {
                throw new IllegalArgumentException(String.join(", ", errorMessages));
            }
            return this;
        }

        private Parameters setDefaults() {
            if(timeoutSecond == null) {
                timeoutSecond = 60;
            }
            for(final HttpUtil.Request request : requests) {
                request.setDefaults();
            }
            if(rate == null) {
                //rate = 0L;
            }
            return this;
        }
    }

    @Override
    public MCollectionTuple expand(
            final PBegin begin,
            final MErrorHandler errorHandler) {

        final Parameters parameters = getParameters(Parameters.class)
                .validate()
                .setDefaults();

        final Http.Transform transform = Http.transform(
                parameters.requests, parameters.timeoutSecond, parameters.retry, parameters.backoff, getLoggings(), getFailFast());
        final PCollectionTuple outputs = begin
                .apply("Seed", Create
                        .of(MElement.createDummyElement(Instant.now()))
                        .withCoder(ElementCoder.of(MElement.dummySchema())))
                .apply("Http", transform);

        errorHandler.addError(outputs.get(transform.failureTag));
        return MCollectionTuple
                .of(outputs.get(transform.outputTag), HttpUtil.Request.schema());
    }

}
