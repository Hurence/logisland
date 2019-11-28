package com.hurence.webapiservice.http;

import io.vertx.reactivex.core.MultiMap;

import java.util.List;

public class MultiMapRequestParser {

    protected List<String> parseListOrDefault(MultiMap map, String queryParam, List<String> defaut) {
        if (map.contains(queryParam)) {
            return map.getAll(queryParam);
        } else {
            return defaut;
        }
    }

    protected int parseIntOrDefault(MultiMap map, String queryParam, int defaut) {
        if (map.contains(queryParam)) {
            return parseInt(map, queryParam);
        } else {
            return defaut;
        }
    }

    protected int parseInt(MultiMap map, String queryParam) {
        try {
            return Integer.parseInt(map.get(queryParam));
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException(
                    String.format("Could not parse parameter '%s' as a integer. '%s' is not an integer",
                            queryParam, map.get(queryParam))
            );
        }
    }


    protected long parseLong(MultiMap map, String queryParam) throws IllegalArgumentException {
        try {
            return Long.parseLong(map.get(queryParam));
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException(
                    String.format("Could not parse parameter '%s' as a long. '%s' is not a long",
                            queryParam, map.get(queryParam))
            );
        }
    }

    protected long parseLongOrDefault(MultiMap map, String queryParam, long defaut) throws IllegalArgumentException {
        if (map.contains(queryParam)) {
            return parseLong(map, queryParam);
        } else {
            return defaut;
        }
    }
}
