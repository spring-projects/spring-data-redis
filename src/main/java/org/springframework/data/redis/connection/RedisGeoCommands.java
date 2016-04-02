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
package org.springframework.data.redis.connection;


import org.springframework.data.redis.core.GeoCoordinate;
import org.springframework.data.redis.core.GeoRadiusParam;
import org.springframework.data.redis.core.GeoRadiusResponse;
import org.springframework.data.redis.core.GeoUnit;

import java.util.List;
import java.util.Map;

/**
 * Geo-specific Redis commands.
 *
 * @author Ninad Divadkar
 */
public interface RedisGeoCommands {
    /**
     * Add latitude and longitude for a given key with a name.
     * Returns the number of elements added to the sorted set, not including elements already existing for which the
     * score was updated.
     * <p>
     * @see http://redis.io/commands/geoadd
     *
     * @param key
     * @param member
     * @param longitude
     * @param latitude
     * @return
     */
    Long geoAdd(byte[] key, double longitude, double latitude, byte[] member);

    Long geoAdd(byte[] key, Map<byte[], GeoCoordinate> memberCoordinateMap);

    Double geoDist(byte[] key, byte[] member1, byte[] member2);

    Double geoDist(byte[] key, byte[] member1, byte[] member2, GeoUnit unit);

    List<byte[]> geoHash(byte[] key, byte[]... members);

    List<GeoCoordinate> geoPos(byte[] key, byte[]... members);

    List<GeoRadiusResponse> georadius(byte[] key, double longitude, double latitude,
                                      double radius, GeoUnit unit);

    List<GeoRadiusResponse> georadius(byte[] key, double longitude, double latitude,
                                      double radius, GeoUnit unit, GeoRadiusParam param);

    List<GeoRadiusResponse> georadiusByMember(byte[] key, byte[] member, double radius,
                                              GeoUnit unit);

    List<GeoRadiusResponse> georadiusByMember(byte[] key, byte[] member, double radius,
                                              GeoUnit unit, GeoRadiusParam param);
}
