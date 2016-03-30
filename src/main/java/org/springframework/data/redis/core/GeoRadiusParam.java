package org.springframework.data.redis.core;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by ndivadkar on 3/29/16.
 */
public class GeoRadiusParam {
    private static final String WITHCOORD = "withcoord";
    private static final String WITHDIST = "withdist";
    private static final String ASC = "asc";
    private static final String DESC = "desc";
    private static final String COUNT = "count";

    private Map<String, Object> params;
    private GeoRadiusParam() {
    }

    public static GeoRadiusParam geoRadiusParam() {
        return new GeoRadiusParam();
    }

    public GeoRadiusParam withCoord() {
        addParam(WITHCOORD);
        return this;
    }

    public GeoRadiusParam withDist() {
        addParam(WITHDIST);
        return this;
    }

    public GeoRadiusParam sortAscending() {
        addParam(ASC);
        return this;
    }

    public GeoRadiusParam sortDescending() {
        addParam(DESC);
        return this;
    }

    public GeoRadiusParam count(int count) {
        if (count > 0) {
            addParam(COUNT, count);
        }
        return this;
    }

    protected void addParam(String name, Object value) {
        if (params == null) {
            params = new HashMap<String, Object>();
        }
        params.put(name, value);
    }

    protected void addParam(String name) {
        if (params == null) {
            params = new HashMap<String, Object>();
        }
        params.put(name, null);
    }
}
