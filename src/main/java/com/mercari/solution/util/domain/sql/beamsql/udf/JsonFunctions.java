package com.mercari.solution.util.domain.sql.beamsql.udf;

import org.apache.beam.vendor.calcite.v1_40_0.com.jayway.jsonpath.JsonPath;
import org.apache.beam.sdk.extensions.sql.BeamSqlUdf;

import java.util.Map;

public class JsonFunctions {

    public static class JsonExtractFn implements BeamSqlUdf {

        public static String eval(final String json, final String path) {

            if(json == null) {
                return null;
            }
            final Map<String, Object> map = JsonPath.read(json, path);
            return map.toString();
        }

    }

}
