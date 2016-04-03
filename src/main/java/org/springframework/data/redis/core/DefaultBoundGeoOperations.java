/*
 * Copyright 2011-2013 the original author or authors.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.redis.core;

import org.springframework.data.redis.connection.DataType;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author Ninad Divadkar
 */
class DefaultBoundGeoOperations<K, M> extends DefaultBoundKeyOperations<K> implements BoundGeoOperations<K, M> {

	private final GeoOperations<K, M> ops;

	/**
	 * Constructs a new <code>DefaultBoundGeoOperations</code> instance.
	 *
	 * @param key
	 * @param operations
	 */
	public DefaultBoundGeoOperations(K key, RedisOperations<K, M> operations) {
		super(key, operations);
		this.ops = operations.opsForGeo();
	}

    @Override
    public Long geoAdd(K key, double longitude, double latitude, M member) {
        return ops.geoAdd(key, longitude, latitude, member);
    }

    @Override
    public Long geoAdd(K key, Map<M, GeoCoordinate> memberCoordinateMap) {
        return ops.geoAdd(key, memberCoordinateMap);
    }

    @Override
    public Double geoDist(K key, M member1, M member2) {
        return ops.geoDist(key, member1, member2);
    }

    @Override
    public Double geoDist(K key, M member1, M member2, GeoUnit unit) {
        return ops.geoDist(key, member1, member2, unit);
    }

    @Override
    public List<byte[]> geoHash(K key, M... members) {
        return ops.geoHash(key, members);
    }

    @Override
    public List<GeoCoordinate> geoPos(K key, M... members) {
        return ops.geoPos(key, members);
    }

    @Override
    public List<GeoRadiusResponse> georadius(K key, double longitude, double latitude, double radius, GeoUnit unit) {
        return ops.georadius(key, longitude, latitude, radius, unit);
    }

    @Override
    public List<GeoRadiusResponse> georadius(K key, double longitude, double latitude, double radius, GeoUnit unit, GeoRadiusParam param) {
        return ops.georadius(key, longitude, latitude, radius, unit, param);
    }

    @Override
    public List<GeoRadiusResponse> georadiusByMember(K key, M member, double radius, GeoUnit unit) {
        return ops.georadiusByMember(key, member, radius, unit);
    }

    @Override
    public List<GeoRadiusResponse> georadiusByMember(K key, M member, double radius, GeoUnit unit, GeoRadiusParam param) {
        return ops.georadiusByMember(key, member, radius, unit, param);
    }

    @Override
    public Long geoRemove(K key, M... members) {
        return ops.geoRemove(key, members);
    }

    @Override
    public DataType getType() {
        return DataType.STRING;
    }
}
