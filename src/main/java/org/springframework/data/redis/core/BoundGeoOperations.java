/*
 * Copyright 2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.redis.core;

import java.util.List;
import java.util.Map;

import org.springframework.data.geo.Circle;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.geo.Metric;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands.GeoLocation;
import org.springframework.data.redis.connection.RedisGeoCommands.GeoRadiusCommandArgs;

/**
 * {@link GeoOperations} bound to a certain key.
 *
 * @author Ninad Divadkar
 * @author Christoph Strobl
 * @since 1.8
 */
public interface BoundGeoOperations<K, M> extends BoundKeyOperations<K> {

	/**
	 * Add {@link Point} with given member {@literal name} to {@literal key}.
	 * 
	 * @param point must not be {@literal null}.
	 * @param member must not be {@literal null}.
	 * @return Number of elements added.
	 * @see <a href="http://redis.io/commands/geoadd">http://redis.io/commands/geoadd</a>
	 */
	Long geoAdd(Point point, M member);

	/**
	 * Add {@link GeoLocation} to {@literal key}.
	 * 
	 * @param location must not be {@literal null}.
	 * @return Number of elements added.
	 * @see <a href="http://redis.io/commands/geoadd">http://redis.io/commands/geoadd</a>
	 */
	Long geoAdd(GeoLocation<M> location);

	/**
	 * Add {@link Map} of member / {@link Point} pairs to {@literal key}.
	 * 
	 * @param memberCoordinateMap must not be {@literal null}.
	 * @return Number of elements added.
	 * @see <a href="http://redis.io/commands/geoadd">http://redis.io/commands/geoadd</a>
	 */
	Long geoAdd(Map<M, Point> memberCoordinateMap);

	/**
	 * Add {@link GeoLocation}s to {@literal key}
	 * 
	 * @param locations must not be {@literal null}.
	 * @return Number of elements added.
	 * @see <a href="http://redis.io/commands/geoadd">http://redis.io/commands/geoadd</a>
	 */
	Long geoAdd(Iterable<GeoLocation<M>> locations);

	/**
	 * Get the {@link Distance} between {@literal member1} and {@literal member2}.
	 * 
	 * @param member1 must not be {@literal null}.
	 * @param member2 must not be {@literal null}.
	 * @return can be {@literal null}.
	 * @see <a href="http://redis.io/commands/geodist">http://redis.io/commands/geodist</a>
	 */
	Distance geoDist(M member1, M member2);

	/**
	 * Get the {@link Distance} between {@literal member1} and {@literal member2} in the given {@link Metric}.
	 * 
	 * @param member1 must not be {@literal null}.
	 * @param member2 must not be {@literal null}.
	 * @param metric must not be {@literal null}.
	 * @return can be {@literal null}.
	 * @see <a href="http://redis.io/commands/geodist">http://redis.io/commands/geodist</a>
	 */
	Distance geoDist(M member1, M member2, Metric metric);

	/**
	 * Get Geohash representation of the position for one or more {@literal member}s.
	 * 
	 * @param members must not be {@literal null}.
	 * @return never {@literal null}.
	 * @see <a href="http://redis.io/commands/geohash">http://redis.io/commands/geohash</a>
	 */
	List<String> geoHash(M... members);

	/**
	 * Get the {@link Point} representation of positions for one or more {@literal member}s.
	 * 
	 * @param members must not be {@literal null}.
	 * @return never {@literal null}.
	 * @see <a href="http://redis.io/commands/geopos">http://redis.io/commands/geopos</a>
	 */
	List<Point> geoPos(M... members);

	/**
	 * Get the {@literal member}s within the boundaries of a given {@link Circle}.
	 * 
	 * @param within must not be {@literal null}.
	 * @return never {@literal null}.
	 * @see <a href="http://redis.io/commands/georadius">http://redis.io/commands/georadius</a>
	 */
	GeoResults<GeoLocation<M>> geoRadius(Circle within);

	/**
	 * Get the {@literal member}s within the boundaries of a given {@link Circle} applying {@link GeoRadiusCommandArgs}.
	 * 
	 * @param within must not be {@literal null}.
	 * @param args must not be {@literal null}.
	 * @return never {@literal null}.
	 * @see <a href="http://redis.io/commands/georadius">http://redis.io/commands/georadius</a>
	 */
	GeoResults<GeoLocation<M>> geoRadius(Circle within, GeoRadiusCommandArgs args);

	/**
	 * Get the {@literal member}s within the circle defined by the {@literal members} coordinates and given
	 * {@literal radius}.
	 * 
	 * @param member must not be {@literal null}.
	 * @param radius
	 * @return never {@literal null}.
	 * @see <a href="http://redis.io/commands/georadiusbymember">http://redis.io/commands/georadiusbymember</a>
	 */
	GeoResults<GeoLocation<M>> geoRadiusByMember(K key, M member, double radius);

	/**
	 * Get the {@literal member}s within the circle defined by the {@literal members} coordinates and given
	 * {@literal radius} applying {@link Metric}.
	 * 
	 * @param member must not be {@literal null}.
	 * @param distance must not be {@literal null}.
	 * @return never {@literal null}.
	 * @see <a href="http://redis.io/commands/georadiusbymember">http://redis.io/commands/georadiusbymember</a>
	 */
	GeoResults<GeoLocation<M>> geoRadiusByMember(M member, Distance distance);

	/**
	 * Get the {@literal member}s within the circle defined by the {@literal members} coordinates and given
	 * {@literal radius} applying {@link Metric} and {@link GeoRadiusCommandArgs}.
	 * 
	 * @param member must not be {@literal null}.
	 * @param distance must not be {@literal null}.
	 * @param args must not be {@literal null}.
	 * @return never {@literal null}.
	 * @see <a href="http://redis.io/commands/georadiusbymember">http://redis.io/commands/georadiusbymember</a>
	 */
	GeoResults<GeoLocation<M>> geoRadiusByMember(M member, Distance distance, GeoRadiusCommandArgs args);

	/**
	 * Remove the {@literal member}s.
	 * 
	 * @param members must not be {@literal null}.
	 * @return Number of elements removed.
	 */
	Long geoRemove(M... members);
}
