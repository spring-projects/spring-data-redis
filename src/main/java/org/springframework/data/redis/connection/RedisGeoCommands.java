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

    /**
     * Add latitude and longitude for a given key with a name.
     * Returns the number of elements added to the sorted set, not including elements already existing for which the
     * score was updated.
     * <p>
     * @see http://redis.io/commands/geoadd
     *
     * @param key
     * @param memberCoordinateMap
     * @return
     */
    Long geoAdd(byte[] key, Map<byte[], GeoCoordinate> memberCoordinateMap);

    /**
     * Return the distance between two members in the geospatial index represented by the sorted set.
     * <p>
     * @see http://redis.io/commands/geodist
     *
     * @param key
     * @param member1
     * @param member2
     * @return
     */
    Double geoDist(byte[] key, byte[] member1, byte[] member2);

    /**
     * Return the distance between two members in the geospatial index represented by the sorted set.
     * <p>
     * @see http://redis.io/commands/geodist
     *
     * @param key
     * @param member1
     * @param member2
     * @param unit
     * @return
     */
    Double geoDist(byte[] key, byte[] member1, byte[] member2, GeoUnit unit);

    /**
     * Return valid Geohash strings representing the position of one or more elements in a sorted set value
     * representing a geospatial index (where elements were added using GEOADD).
     * <p>
     * @see http://redis.io/commands/geohash
     *
     * @param key
     * @param members
     * @return
     */
    List<byte[]> geoHash(byte[] key, byte[]... members);

    /**
     * Return the positions (longitude,latitude) of all the specified members of the geospatial index represented by
     * the sorted set at key.
     * <p>
     * @see http://redis.io/commands/geopos
     *
     * @param key
     * @param members
     * @return
     */
    List<GeoCoordinate> geoPos(byte[] key, byte[]... members);

    /**
     * Return the members of a sorted set populated with geospatial information using GEOADD, which are within
     * the borders of the area specified with the center location and the maximum distance from the radius.
     * <p>
     * @see http://redis.io/commands/georadius
     *
     * @param key
     * @param longitude
     * @param latitude
     * @param radius
     * @param unit
     * @return
     */
    List<GeoRadiusResponse> georadius(byte[] key, double longitude, double latitude,
                                      double radius, GeoUnit unit);

    /**
     * Return the members of a sorted set populated with geospatial information using GEOADD, which are within
     * the borders of the area specified with the center location and the maximum distance from the radius.
     * <p>
     * @see http://redis.io/commands/georadius
     *
     * @param key
     * @param longitude
     * @param latitude
     * @param radius
     * @param unit
     * @param param
     * @return
     */
    List<GeoRadiusResponse> georadius(byte[] key, double longitude, double latitude,
                                      double radius, GeoUnit unit, GeoRadiusParam param);

    /**
     * Add latitude and longitude for a given key with a name.
     * Returns the number of elements added to the sorted set, not including elements already existing for which the
     * score was updated.
     * <p>
     * @see http://redis.io/commands/georadiusbymember
     *
     * @param key
     * @param member
     * @param radius
     * @param unit
     * @return
     */
    List<GeoRadiusResponse> georadiusByMember(byte[] key, byte[] member, double radius,
                                              GeoUnit unit);

    /**
     * This command is exactly like GEORADIUS with the sole difference that instead of taking, as the center of
     * the area to query, a longitude and latitude value, it takes the name of a member already existing inside
     * the geospatial index represented by the sorted set.
     * <p>
     * @see http://redis.io/commands/georadiusbymember
     *
     * @param key
     * @param member
     * @param radius
     * @param unit
     * @param param
     *
     * @return
     */
    List<GeoRadiusResponse> georadiusByMember(byte[] key, byte[] member, double radius,
                                              GeoUnit unit, GeoRadiusParam param);
}
