/*
 * Copyright 2016-2018 the original author or authors.
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

import java.util.List;
import java.util.Map;

import org.springframework.data.geo.Circle;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.geo.Metric;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands.GeoLocation;
import org.springframework.data.redis.connection.RedisGeoCommands.GeoRadiusCommandArgs;
import org.springframework.lang.Nullable;

/**
 * Redis operations for geo commands.
 *
 * @author Ninad Divadkar
 * @author Christoph Strobl
 * @author Mark Paluch
 * @see <a href="http://redis.io/commands#geo">Redis Documentation: Geo Commands</a>
 * @since 1.8
 */
public interface GeoOperations<K, M> {

	/**
	 * Add {@link Point} with given member {@literal name} to {@literal key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param point must not be {@literal null}.
	 * @param member must not be {@literal null}.
	 * @return Number of elements added. {@literal null} when used in pipeline / transaction.
	 * @since 2.0
	 * @see <a href="http://redis.io/commands/geoadd">Redis Documentation: GEOADD</a>
	 */
	@Nullable
	Long add(K key, Point point, M member);

	/**
	 * Add {@link Point} with given member {@literal name} to {@literal key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param point must not be {@literal null}.
	 * @param member must not be {@literal null}.
	 * @return Number of elements added. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/geoadd">Redis Documentation: GEOADD</a>
	 * @deprecated since 2.0, use {@link #add(Object, Point, Object)}.
	 */
	@Deprecated
	@Nullable
	default Long geoAdd(K key, Point point, M member) {
		return add(key, point, member);
	}

	/**
	 * Add {@link GeoLocation} to {@literal key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param location must not be {@literal null}.
	 * @return Number of elements added. {@literal null} when used in pipeline / transaction.
	 * @since 2.0
	 * @see <a href="http://redis.io/commands/geoadd">Redis Documentation: GEOADD</a>
	 */
	@Nullable
	Long add(K key, GeoLocation<M> location);

	/**
	 * Add {@link GeoLocation} to {@literal key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param location must not be {@literal null}.
	 * @return Number of elements added. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/geoadd">Redis Documentation: GEOADD</a>
	 * @deprecated since 2.0, use {@link #add(Object, GeoLocation)}.
	 */
	@Deprecated
	@Nullable
	default Long geoAdd(K key, GeoLocation<M> location) {
		return add(key, location);
	}

	/**
	 * Add {@link Map} of member / {@link Point} pairs to {@literal key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param memberCoordinateMap must not be {@literal null}.
	 * @return Number of elements added. {@literal null} when used in pipeline / transaction.
	 * @since 2.0
	 * @see <a href="http://redis.io/commands/geoadd">Redis Documentation: GEOADD</a>
	 */
	@Nullable
	Long add(K key, Map<M, Point> memberCoordinateMap);

	/**
	 * Add {@link Map} of member / {@link Point} pairs to {@literal key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param memberCoordinateMap must not be {@literal null}.
	 * @return Number of elements added. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/geoadd">Redis Documentation: GEOADD</a>
	 * @deprecated since 2.0, use {@link #add(Object, Map)}.
	 */
	@Deprecated
	@Nullable
	default Long geoAdd(K key, Map<M, Point> memberCoordinateMap) {
		return add(key, memberCoordinateMap);
	}

	/**
	 * Add {@link GeoLocation}s to {@literal key}
	 *
	 * @param key must not be {@literal null}.
	 * @param locations must not be {@literal null}.
	 * @return Number of elements added. {@literal null} when used in pipeline / transaction.
	 * @since 2.0
	 * @see <a href="http://redis.io/commands/geoadd">Redis Documentation: GEOADD</a>
	 */
	@Nullable
	Long add(K key, Iterable<GeoLocation<M>> locations);

	/**
	 * Add {@link GeoLocation}s to {@literal key}
	 *
	 * @param key must not be {@literal null}.
	 * @param locations must not be {@literal null}.
	 * @return Number of elements added. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/geoadd">Redis Documentation: GEOADD</a>
	 * @deprecated since 2.0, use {@link #add(Object, Iterable)}.
	 */
	@Deprecated
	@Nullable
	default Long geoAdd(K key, Iterable<GeoLocation<M>> locations) {
		return add(key, locations);
	}

	/**
	 * Get the {@link Distance} between {@literal member1} and {@literal member2}.
	 *
	 * @param key must not be {@literal null}.
	 * @param member1 must not be {@literal null}.
	 * @param member2 must not be {@literal null}.
	 * @return can be {@literal null}.
	 * @since 2.0
	 * @see <a href="http://redis.io/commands/geodist">Redis Documentation: GEODIST</a>
	 */
	@Nullable
	Distance distance(K key, M member1, M member2);

	/**
	 * Get the {@link Distance} between {@literal member1} and {@literal member2}.
	 *
	 * @param key must not be {@literal null}.
	 * @param member1 must not be {@literal null}.
	 * @param member2 must not be {@literal null}.
	 * @return can be {@literal null}.
	 * @see <a href="http://redis.io/commands/geodist">Redis Documentation: GEODIST</a>
	 * @deprecated since 2.0, use {@link #distance(Object, Object, Object)}.
	 */
	@Deprecated
	@Nullable
	default Distance geoDist(K key, M member1, M member2) {
		return distance(key, member1, member2);
	}

	/**
	 * Get the {@link Distance} between {@literal member1} and {@literal member2} in the given {@link Metric}.
	 *
	 * @param key must not be {@literal null}.
	 * @param member1 must not be {@literal null}.
	 * @param member2 must not be {@literal null}.
	 * @param metric must not be {@literal null}.
	 * @return can be {@literal null}.
	 * @since 2.0
	 * @see <a href="http://redis.io/commands/geodist">Redis Documentation: GEODIST</a>
	 */
	@Nullable
	Distance distance(K key, M member1, M member2, Metric metric);

	/**
	 * Get the {@link Distance} between {@literal member1} and {@literal member2} in the given {@link Metric}.
	 *
	 * @param key must not be {@literal null}.
	 * @param member1 must not be {@literal null}.
	 * @param member2 must not be {@literal null}.
	 * @param metric must not be {@literal null}.
	 * @return can be {@literal null}.
	 * @see <a href="http://redis.io/commands/geodist">Redis Documentation: GEODIST</a>
	 * @deprecated since 2.0, use {@link #distance(Object, Object, Object, Metric)}.
	 */
	@Deprecated
	@Nullable
	default Distance geoDist(K key, M member1, M member2, Metric metric) {
		return distance(key, member1, member2, metric);
	}

	/**
	 * Get Geohash representation of the position for one or more {@literal member}s.
	 *
	 * @param key must not be {@literal null}.
	 * @param members must not be {@literal null}.
	 * @return never {@literal null} unless used in pipeline / transaction.
	 * @since 2.0
	 * @see <a href="http://redis.io/commands/geohash">Redis Documentation: GEOHASH</a>
	 */
	@Nullable
	List<String> hash(K key, M... members);

	/**
	 * Get Geohash representation of the position for one or more {@literal member}s.
	 *
	 * @param key must not be {@literal null}.
	 * @param members must not be {@literal null}.
	 * @return never {@literal null} unless used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/geohash">Redis Documentation: GEOHASH</a>
	 * @deprecated since 2.0, use {@link #hash(Object, Object[])}.
	 */
	@Deprecated
	@Nullable
	default List<String> geoHash(K key, M... members) {
		return hash(key, members);
	}

	/**
	 * Get the {@link Point} representation of positions for one or more {@literal member}s.
	 *
	 * @param key must not be {@literal null}.
	 * @param members must not be {@literal null}.
	 * @return never {@literal null} unless used in pipeline / transaction.
	 * @since 2.0
	 * @see <a href="http://redis.io/commands/geopos">Redis Documentation: GEOPOS</a>
	 */
	@Nullable
	List<Point> position(K key, M... members);

	/**
	 * Get the {@link Point} representation of positions for one or more {@literal member}s.
	 *
	 * @param key must not be {@literal null}.
	 * @param members must not be {@literal null}.
	 * @return never {@literal null} unless used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/geopos">Redis Documentation: GEOPOS</a>
	 * @deprecated since 2.0, use {@link #position(Object, Object[])}.
	 */
	@Deprecated
	@Nullable
	default List<Point> geoPos(K key, M... members) {
		return position(key, members);
	}

	/**
	 * Get the {@literal member}s within the boundaries of a given {@link Circle}.
	 *
	 * @param key must not be {@literal null}.
	 * @param within must not be {@literal null}.
	 * @return never {@literal null} unless used in pipeline / transaction.
	 * @since 2.0
	 * @see <a href="http://redis.io/commands/georadius">Redis Documentation: GEORADIUS</a>
	 */
	@Nullable
	GeoResults<GeoLocation<M>> radius(K key, Circle within);

	/**
	 * Get the {@literal member}s within the boundaries of a given {@link Circle}.
	 *
	 * @param key must not be {@literal null}.
	 * @param within must not be {@literal null}.
	 * @return never {@literal null} unless used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/georadius">Redis Documentation: GEORADIUS</a>
	 * @deprecated since 2.0, use {@link #radius(Object, Circle)}.
	 */
	@Deprecated
	@Nullable
	default GeoResults<GeoLocation<M>> geoRadius(K key, Circle within) {
		return radius(key, within);
	}

	/**
	 * Get the {@literal member}s within the boundaries of a given {@link Circle} applying {@link GeoRadiusCommandArgs}.
	 *
	 * @param key must not be {@literal null}.
	 * @param within must not be {@literal null}.
	 * @param args must not be {@literal null}.
	 * @return never {@literal null} unless used in pipeline / transaction.
	 * @since 2.0
	 * @see <a href="http://redis.io/commands/georadius">Redis Documentation: GEORADIUS</a>
	 */
	@Nullable
	GeoResults<GeoLocation<M>> radius(K key, Circle within, GeoRadiusCommandArgs args);

	/**
	 * Get the {@literal member}s within the boundaries of a given {@link Circle} applying {@link GeoRadiusCommandArgs}.
	 *
	 * @param key must not be {@literal null}.
	 * @param within must not be {@literal null}.
	 * @param args must not be {@literal null}.
	 * @return never {@literal null} unless used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/georadius">Redis Documentation: GEORADIUS</a>
	 * @deprecated since 2.0, use {@link #radius(Object, Circle, GeoRadiusCommandArgs)}.
	 */
	@Deprecated
	@Nullable
	default GeoResults<GeoLocation<M>> geoRadius(K key, Circle within, GeoRadiusCommandArgs args) {
		return radius(key, within, args);
	}

	/**
	 * Get the {@literal member}s within the circle defined by the {@literal members} coordinates and given
	 * {@literal radius}.
	 *
	 * @param key must not be {@literal null}.
	 * @param member must not be {@literal null}.
	 * @param radius
	 * @return never {@literal null} unless used in pipeline / transaction.
	 * @since 2.0
	 * @see <a href="http://redis.io/commands/georadiusbymember">Redis Documentation: GEORADIUSBYMEMBER</a>
	 */
	@Nullable
	GeoResults<GeoLocation<M>> radius(K key, M member, double radius);

	/**
	 * Get the {@literal member}s within the circle defined by the {@literal members} coordinates and given
	 * {@literal radius}.
	 *
	 * @param key must not be {@literal null}.
	 * @param member must not be {@literal null}.
	 * @param radius
	 * @return never {@literal null} unless used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/georadiusbymember">Redis Documentation: GEORADIUSBYMEMBER</a>
	 * @deprecated since 2.0, use {@link #radius(Object, Object, double)}.
	 */
	@Deprecated
	@Nullable
	default GeoResults<GeoLocation<M>> geoRadiusByMember(K key, M member, double radius) {
		return radius(key, member, radius);
	}

	/**
	 * Get the {@literal member}s within the circle defined by the {@literal members} coordinates and given
	 * {@literal radius} applying {@link Metric}.
	 *
	 * @param key must not be {@literal null}.
	 * @param member must not be {@literal null}.
	 * @param distance must not be {@literal null}.
	 * @return never {@literal null} unless used in pipeline / transaction.
	 * @since 2.0
	 * @see <a href="http://redis.io/commands/georadiusbymember">Redis Documentation: GEORADIUSBYMEMBER</a>
	 */
	@Nullable
	GeoResults<GeoLocation<M>> radius(K key, M member, Distance distance);

	/**
	 * Get the {@literal member}s within the circle defined by the {@literal members} coordinates and given
	 * {@literal radius} applying {@link Metric}.
	 *
	 * @param key must not be {@literal null}.
	 * @param member must not be {@literal null}.
	 * @param distance must not be {@literal null}.
	 * @return never {@literal null} unless used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/georadiusbymember">Redis Documentation: GEORADIUSBYMEMBER</a>
	 * @deprecated since 2.0, use {@link #radius(Object, Object, Distance)}.
	 */
	@Deprecated
	@Nullable
	default GeoResults<GeoLocation<M>> geoRadiusByMember(K key, M member, Distance distance) {
		return radius(key, member, distance);
	}

	/**
	 * Get the {@literal member}s within the circle defined by the {@literal members} coordinates and given
	 * {@literal radius} applying {@link Metric} and {@link GeoRadiusCommandArgs}.
	 *
	 * @param key must not be {@literal null}.
	 * @param member must not be {@literal null}.
	 * @param distance must not be {@literal null}.
	 * @param args must not be {@literal null}.
	 * @return never {@literal null} unless used in pipeline / transaction.
	 * @since 2.0
	 * @see <a href="http://redis.io/commands/georadiusbymember">Redis Documentation: GEORADIUSBYMEMBER</a>
	 */
	@Nullable
	GeoResults<GeoLocation<M>> radius(K key, M member, Distance distance, GeoRadiusCommandArgs args);

	/**
	 * Get the {@literal member}s within the circle defined by the {@literal members} coordinates and given
	 * {@literal radius} applying {@link Metric} and {@link GeoRadiusCommandArgs}.
	 *
	 * @param key must not be {@literal null}.
	 * @param member must not be {@literal null}.
	 * @param distance must not be {@literal null}.
	 * @param args must not be {@literal null}.
	 * @return never {@literal null} unless used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/georadiusbymember">Redis Documentation: GEORADIUSBYMEMBER</a>
	 * @deprecated since 2.0, use {@link #radius(Object, Object, Distance, GeoRadiusCommandArgs)}.
	 */
	@Deprecated
	@Nullable
	default GeoResults<GeoLocation<M>> geoRadiusByMember(K key, M member, Distance distance, GeoRadiusCommandArgs args) {
		return radius(key, member, distance, args);
	}

	/**
	 * Remove the {@literal member}s.
	 *
	 * @param key must not be {@literal null}.
	 * @param members must not be {@literal null}.
	 * @return Number of elements removed. {@literal null} when used in pipeline / transaction.
	 * @since 2.0
	 */
	@Nullable
	Long remove(K key, M... members);

	/**
	 * Remove the {@literal member}s.
	 *
	 * @param key must not be {@literal null}.
	 * @param members must not be {@literal null}.
	 * @return Number of elements removed. {@literal null} when used in pipeline / transaction.
	 * @deprecated since 2.0, use {@link #remove(Object, Object[])}.
	 */
	@Deprecated
	@Nullable
	default Long geoRemove(K key, M... members) {
		return remove(key, members);
	}
}
