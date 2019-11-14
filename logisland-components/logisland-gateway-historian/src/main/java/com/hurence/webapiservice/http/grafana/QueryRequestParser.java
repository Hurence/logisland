package com.hurence.webapiservice.http.grafana;

import com.hurence.webapiservice.http.grafana.modele.AdHocFilter;
import com.hurence.webapiservice.http.grafana.modele.QueryRequestParam;
import com.hurence.webapiservice.http.grafana.modele.Target;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.json.pointer.JsonPointer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.util.Collections;
import java.util.List;
import java.util.TimeZone;
import java.util.stream.Collectors;

public class QueryRequestParser {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryRequestParser.class);
    private SimpleDateFormat dateFormat = createDateFormat();

    private static SimpleDateFormat createDateFormat() {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
        TimeZone myTimeZone = TimeZone.getTimeZone(ZoneId.of("UTC"));
        dateFormat.setTimeZone(myTimeZone);
        dateFormat.setLenient(false);
        return dateFormat;
    }

    public QueryRequestParam parseRequest(JsonObject requestBody) throws IllegalArgumentException {
        LOGGER.debug("trying to parse requestBody : {}", requestBody);
        QueryRequestParam.Builder builder = new QueryRequestParam.Builder();
        long from = parseFrom(requestBody);
        builder.from(from);
        long to = parseTo(requestBody);
        builder.to(to);
        String format = parseFormat(requestBody);
        builder.withFormat(format);
        int maxDataPoints = parseMaxDataPoints(requestBody);;
        builder.withMaxDataPoints(maxDataPoints);
        List<Target> targets = parseTargets(requestBody);;
        builder.withTargets(targets);
        List<AdHocFilter> adHocFilters = parseAdHocFilters(requestBody);;
        builder.withAdHocFilters(adHocFilters);
        String requestId = parseRequestId(requestBody);
        builder.withId(requestId);
        return builder.build();

    }

    private List<Target> parseTargets(JsonObject requestBody) {
        return requestBody.getJsonArray("targets").stream()
                .map(JsonObject.class::cast)
                .map(JsonObject::encode)
                .map(json -> Json.decodeValue(json, Target.class))
                .collect(Collectors.toList());
    }

    private long parseFrom(JsonObject requestBody) {
        return parseDate(requestBody, "/range/from");
    }

    private long parseTo(JsonObject requestBody) {
        return parseDate(requestBody, "/range/to");
    }

    private long parseDate(JsonObject requestBody, String pointer) {
        LOGGER.debug("trying to parse pointer {}", pointer);
        JsonPointer jsonPointer = JsonPointer.from(pointer);
        Object fromObj = jsonPointer.queryJson(requestBody);
        if (fromObj instanceof String) {
            String fromStr = (String) fromObj;
            try {
                return dateFormat.parse(fromStr).getTime();
            } catch (ParseException e) {
                throw new IllegalArgumentException(
                        String.format("'%s' json pointer value '%s' could not be parsed as a valid date !",
                                pointer, fromObj), e);
            }
        }
        throw new IllegalArgumentException(
                String.format("'%s' json pointer value '%s' is not a string !",
                        pointer, fromObj));
    }

    private String parseFormat(JsonObject requestBody) {
        return requestBody.getString("format");
    }

    private String parseRequestId(JsonObject requestBody) {
        return requestBody.getString("requestId");
    }

    private int parseMaxDataPoints(JsonObject requestBody) {
        return requestBody.getInteger("maxDataPoints");
    }

    private List<AdHocFilter> parseAdHocFilters(JsonObject requestBody) {
        if (!requestBody.containsKey("adhocFilters"))
            return Collections.emptyList();
        return requestBody.getJsonArray("adhocFilters").stream()
                .map(JsonObject.class::cast)
                .map(JsonObject::encode)
                .map(json -> Json.decodeValue(json, AdHocFilter.class))
                .collect(Collectors.toList());
    }
}
