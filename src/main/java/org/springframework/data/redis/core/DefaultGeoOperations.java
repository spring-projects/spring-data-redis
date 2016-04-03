/*
 * Copyright 2011-2014 the original author or authors.
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

import org.springframework.data.redis.connection.RedisConnection;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Default implementation of {@link GeoOperations}.
 *
 * @author Ninad Divadkar
 */
public class DefaultGeoOperations<K, M> extends AbstractOperations<K, M> implements GeoOperations<K, M> {
    DefaultGeoOperations(RedisTemplate<K, M> template) {
        super(template);
    }

    @Override
    public Long geoAdd(K key, final double longitude, final double latitude, M member) {
        final byte[] rawKey = rawKey(key);
        final byte[] rawMember = rawValue(member);

        return execute(new RedisCallback<Long>() {

            public Long doInRedis(RedisConnection connection) {
                return connection.geoAdd(rawKey, longitude, latitude, rawMember);
            }
        }, true);
    }

    @Override
    public Long geoAdd(K key, Map<M, GeoCoordinate> memberCoordinateMap) {
        final byte[] rawKey = rawKey(key);
        final Map<byte[], GeoCoordinate> rawMemberCoordinateMap = new HashMap<byte[], GeoCoordinate>();
        for(M member : memberCoordinateMap.keySet()){
            final byte[] rawMember = rawValue(member);
            rawMemberCoordinateMap.put(rawMember, memberCoordinateMap.get(member));
        }

        return execute(new RedisCallback<Long>() {

            public Long doInRedis(RedisConnection connection) {
                return connection.geoAdd(rawKey, rawMemberCoordinateMap);
            }
        }, true);
    }

    @Override
    public Double geoDist(K key, final M member1, final M member2) {
        final byte[] rawKey = rawKey(key);
        final byte[] rawMember1 = rawValue(member1);
        final byte[] rawMember2 = rawValue(member2);

        return execute(new RedisCallback<Double>() {

            public Double doInRedis(RedisConnection connection) {
                return connection.geoDist(rawKey, rawMember1, rawMember2);
            }
        }, true);
    }

    @Override
    public Double geoDist(K key, M member1, M member2, final GeoUnit unit) {
        final byte[] rawKey = rawKey(key);
        final byte[] rawMember1 = rawValue(member1);
        final byte[] rawMember2 = rawValue(member2);

        return execute(new RedisCallback<Double>() {

            public Double doInRedis(RedisConnection connection) {
                return connection.geoDist(rawKey, rawMember1, rawMember2, unit);
            }
        }, true);
    }

    @Override
    public List<byte[]> geoHash(K key, final M... members) {
        final byte[] rawKey = rawKey(key);
        final byte[][] rawMembers = rawValues(members);

        return execute(new RedisCallback<List<byte[]>>() {

            public List<byte[]> doInRedis(RedisConnection connection) {
                return connection.geoHash(rawKey, rawMembers);
            }
        }, true);
    }

    @Override
    public List<GeoCoordinate> geoPos(K key, M... members) {
        final byte[] rawKey = rawKey(key);
        final byte[][] rawMembers = rawValues(members);

        return execute(new RedisCallback<List<GeoCoordinate>>() {

            public List<GeoCoordinate> doInRedis(RedisConnection connection) {
                return connection.geoPos(rawKey, rawMembers);
            }
        }, true);
    }

    @Override
    public List<GeoRadiusResponse> georadius(K key, final double longitude, final double latitude, final double radius, final GeoUnit unit) {
        final byte[] rawKey = rawKey(key);

        return execute(new RedisCallback<List<GeoRadiusResponse>>() {

            public List<GeoRadiusResponse> doInRedis(RedisConnection connection) {
                return connection.georadius(rawKey, longitude, latitude, radius, unit);
            }
        }, true);
    }

    @Override
    public List<GeoRadiusResponse> georadius(K key, final double longitude, final double latitude, final double radius, final GeoUnit unit, final GeoRadiusParam param) {
        final byte[] rawKey = rawKey(key);

        return execute(new RedisCallback<List<GeoRadiusResponse>>() {

            public List<GeoRadiusResponse> doInRedis(RedisConnection connection) {
                return connection.georadius(rawKey, longitude, latitude, radius, unit, param);
            }
        }, true);
    }

    @Override
    public List<GeoRadiusResponse> georadiusByMember(K key, M member, final double radius, final GeoUnit unit) {
        final byte[] rawKey = rawKey(key);
        final byte[] rawMember = rawValue(member);

        return execute(new RedisCallback<List<GeoRadiusResponse>>() {

            public List<GeoRadiusResponse> doInRedis(RedisConnection connection) {
                return connection.georadiusByMember(rawKey, rawMember, radius, unit);
            }
        }, true);
    }

    @Override
    public List<GeoRadiusResponse> georadiusByMember(K key, M member, final double radius, final GeoUnit unit, final GeoRadiusParam param) {
        final byte[] rawKey = rawKey(key);
        final byte[] rawMember = rawValue(member);

        return execute(new RedisCallback<List<GeoRadiusResponse>>() {

            public List<GeoRadiusResponse> doInRedis(RedisConnection connection) {
                return connection.georadiusByMember(rawKey, rawMember, radius, unit, param);
            }
        }, true);
    }
}
