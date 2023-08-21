/*
 * Copyright 2016-2023 the original author or authors.
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
import org.springframework.data.redis.connection.RedisGeoCommands.GeoSearchCommandArgs;
import org.springframework.data.redis.connection.RedisGeoCommands.GeoSearchStoreCommandArgs;
import org.springframework.data.redis.domain.geo.BoundingBox;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.data.redis.domain.geo.GeoShape;
import org.springframework.lang.Nullable;

/**
 * Redis operations for geo commands.
 *
 * @author Ninad Divadkar
 * @author Christoph Strobl
 * @author Mark Paluch
 * @see <a href="https://redis.io/commands#geo">Redis Documentation: Geo Commands</a>
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
	 * @see <a href="https://redis.io/commands/geoadd">Redis Documentation: GEOADD</a>
	 */
	Long add(K key, Point point, M member);

	/**
	 * Add {@link GeoLocation} to {@literal key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param location must not be {@literal null}.
	 * @return Number of elements added. {@literal null} when used in pipeline / transaction.
	 * @since 2.0
	 * @see <a href="https://redis.io/commands/geoadd">Redis Documentation: GEOADD</a>
	 */
	Long add(K key, GeoLocation<M> location);

	/**
	 * Add {@link Map} of member / {@link Point} pairs to {@literal key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param memberCoordinateMap must not be {@literal null}.
	 * @return Number of elements added. {@literal null} when used in pipeline / transaction.
	 * @since 2.0
	 * @see <a href="https://redis.io/commands/geoadd">Redis Documentation: GEOADD</a>
	 */
	Long add(K key, Map<M, Point> memberCoordinateMap);

	/**
	 * Add {@link GeoLocation}s to {@literal key}
	 *
	 * @param key must not be {@literal null}.
	 * @param locations must not be {@literal null}.
	 * @return Number of elements added. {@literal null} when used in pipeline / transaction.
	 * @since 2.0
	 * @see <a href="https://redis.io/commands/geoadd">Redis Documentation: GEOADD</a>
	 */
	Long add(K key, Iterable<GeoLocation<M>> locations);

	/**
	 * Get the {@link Distance} between {@literal member1} and {@literal member2}.
	 *
	 * @param key must not be {@literal null}.
	 * @param member1 must not be {@literal null}.
	 * @param member2 must not be {@literal null}.
	 * @return can be {@literal null}.
	 * @since 2.0
	 * @see <a href="https://redis.io/commands/geodist">Redis Documentation: GEODIST</a>
	 */
	@Nullable
	Distance distance(K key, M member1, M member2);

	/**
	 * Get the {@link Distance} between {@literal member1} and {@literal member2} in the given {@link Metric}.
	 *
	 * @param key must not be {@literal null}.
	 * @param member1 must not be {@literal null}.
	 * @param member2 must not be {@literal null}.
	 * @param metric must not be {@literal null}.
	 * @return can be {@literal null}.
	 * @since 2.0
	 * @see <a href="https://redis.io/commands/geodist">Redis Documentation: GEODIST</a>
	 */
	@Nullable
	Distance distance(K key, M member1, M member2, Metric metric);

	/**
	 * Get Geohash representation of the position for one or more {@literal member}s.
	 *
	 * @param key must not be {@literal null}.
	 * @param members must not be {@literal null}.
	 * @return never {@literal null} unless used in pipeline / transaction.
	 * @since 2.0
	 * @see <a href="https://redis.io/commands/geohash">Redis Documentation: GEOHASH</a>
	 */
	List<String> hash(K key, M... members);

	/**
	 * Get the {@link Point} representation of positions for one or more {@literal member}s.
	 *
	 * @param key must not be {@literal null}.
	 * @param members must not be {@literal null}.
	 * @return never {@literal null} unless used in pipeline / transaction.
	 * @since 2.0
	 * @see <a href="https://redis.io/commands/geopos">Redis Documentation: GEOPOS</a>
	 */
	List<Point> position(K key, M... members);

	/**
	 * Get the {@literal member}s within the boundaries of a given {@link Circle}.
	 *
	 * @param key must not be {@literal null}.
	 * @param within must not be {@literal null}.
	 * @return never {@literal null} unless used in pipeline / transaction.
	 * @since 2.0
	 * @see <a href="https://redis.io/commands/georadius">Redis Documentation: GEORADIUS</a>
	 */
	GeoResults<GeoLocation<M>> radius(K key, Circle within);

	/**
	 * Get the {@literal member}s within the boundaries of a given {@link Circle} applying {@link GeoRadiusCommandArgs}.
	 *
	 * @param key must not be {@literal null}.
	 * @param within must not be {@literal null}.
	 * @param args must not be {@literal null}.
	 * @return never {@literal null} unless used in pipeline / transaction.
	 * @since 2.0
	 * @see <a href="https://redis.io/commands/georadius">Redis Documentation: GEORADIUS</a>
	 */
	GeoResults<GeoLocation<M>> radius(K key, Circle within, GeoRadiusCommandArgs args);

	/**
	 * Get the {@literal member}s within the circle defined by the {@literal members} coordinates and given
	 * {@literal radius}.
	 *
	 * @param key must not be {@literal null}.
	 * @param member must not be {@literal null}.
	 * @param radius
	 * @return never {@literal null} unless used in pipeline / transaction.
	 * @since 2.0
	 * @see <a href="https://redis.io/commands/georadiusbymember">Redis Documentation: GEORADIUSBYMEMBER</a>
	 */
	GeoResults<GeoLocation<M>> radius(K key, M member, double radius);

	/**
	 * Get the {@literal member}s within the circle defined by the {@literal members} coordinates and given
	 * {@literal radius} applying {@link Metric}.
	 *
	 * @param key must not be {@literal null}.
	 * @param member must not be {@literal null}.
	 * @param distance must not be {@literal null}.
	 * @return never {@literal null} unless used in pipeline / transaction.
	 * @since 2.0
	 * @see <a href="https://redis.io/commands/georadiusbymember">Redis Documentation: GEORADIUSBYMEMBER</a>
	 */
	GeoResults<GeoLocation<M>> radius(K key, M member, Distance distance);

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
	 * @see <a href="https://redis.io/commands/georadiusbymember">Redis Documentation: GEORADIUSBYMEMBER</a>
	 */
	GeoResults<GeoLocation<M>> radius(K key, M member, Distance distance, GeoRadiusCommandArgs args);

	/**
	 * Remove the {@literal member}s.
	 *
	 * @param key must not be {@literal null}.
	 * @param members must not be {@literal null}.
	 * @return Number of elements removed. {@literal null} when used in pipeline / transaction.
	 * @since 2.0
	 */
	Long remove(K key, M... members);

	/**
	 * Get the {@literal member}s within the boundaries of a given {@link Circle}.
	 *
	 * @param key must not be {@literal null}.
	 * @param within must not be {@literal null}.
	 * @return never {@literal null} unless used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/geosearch">Redis Documentation: GEOSEARCH</a>
	 */
	default GeoResults<GeoLocation<M>> search(K key, Circle within) {
		return search(key, GeoReference.fromCircle(within), GeoShape.byRadius(within.getRadius()),
				GeoSearchCommandArgs.newGeoSearchArgs());
	}

	/**
	 * Get the {@literal member}s using {@link GeoReference} as center of the query within the boundaries of a given
	 * {@link Distance radius}.
	 *
	 * @param key must not be {@literal null}.
	 * @param reference must not be {@literal null}.
	 * @param radius must not be {@literal null}.
	 * @return never {@literal null} unless used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/geosearch">Redis Documentation: GEOSEARCH</a>
	 */
	default GeoResults<GeoLocation<M>> search(K key, GeoReference<M> reference, Distance radius) {
		return search(key, reference, radius, GeoSearchCommandArgs.newGeoSearchArgs());
	}

	/**
	 * Get the {@literal member}s using {@link GeoReference} as center of the query within the boundaries of a given
	 * {@link Distance radius} applying {@link GeoRadiusCommandArgs}.
	 *
	 * @param key must not be {@literal null}.
	 * @param reference must not be {@literal null}.
	 * @param radius must not be {@literal null}.
	 * @param args must not be {@literal null}.
	 * @return never {@literal null} unless used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/geosearch">Redis Documentation: GEOSEARCH</a>
	 */
	default GeoResults<GeoLocation<M>> search(K key, GeoReference<M> reference, Distance radius,
			GeoSearchCommandArgs args) {
		return search(key, reference, GeoShape.byRadius(radius), args);
	}

	/**
	 * Get the {@literal member}s using {@link GeoReference} as center of the query within the boundaries of a given
	 * bounding box.
	 *
	 * @param key must not be {@literal null}.
	 * @param reference must not be {@literal null}.
	 * @param boundingBox must not be {@literal null}.
	 * @return never {@literal null} unless used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/geosearch">Redis Documentation: GEOSEARCH</a>
	 */
	default GeoResults<GeoLocation<M>> search(K key, GeoReference<M> reference,
			BoundingBox boundingBox) {
		return search(key, reference, boundingBox, GeoSearchCommandArgs.newGeoSearchArgs());
	}

	/**
	 * Get the {@literal member}s using {@link GeoReference} as center of the query within the boundaries of a given
	 * bounding box applying {@link GeoRadiusCommandArgs}.
	 *
	 * @param key must not be {@literal null}.
	 * @param reference must not be {@literal null}.
	 * @param boundingBox must not be {@literal null}.
	 * @param args must not be {@literal null}.
	 * @return never {@literal null} unless used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/geosearch">Redis Documentation: GEOSEARCH</a>
	 */
	default GeoResults<GeoLocation<M>> search(K key, GeoReference<M> reference, BoundingBox boundingBox,
			GeoSearchCommandArgs args) {
		return search(key, reference, GeoShape.byBox(boundingBox), args);
	}

	/**
	 * Get the {@literal member}s using {@link GeoReference} as center of the query within the boundaries of a given
	 * {@link GeoShape predicate} applying {@link GeoRadiusCommandArgs}.
	 *
	 * @param key must not be {@literal null}.
	 * @param reference must not be {@literal null}.
	 * @param geoPredicate must not be {@literal null}.
	 * @param args must not be {@literal null}.
	 * @return never {@literal null} unless used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/geosearch">Redis Documentation: GEOSEARCH</a>
	 */
	GeoResults<GeoLocation<M>> search(K key, GeoReference<M> reference,
			GeoShape geoPredicate, GeoSearchCommandArgs args);

	/**
	 * Get the {@literal member}s within the boundaries of a given {@link Circle} and store results at {@code destKey}.
	 *
	 * @param key must not be {@literal null}.
	 * @param within must not be {@literal null}.
	 * @return never {@literal null} unless used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/geosearchstore">Redis Documentation: GEOSEARCHSTORE</a>
	 */
	default Long searchAndStore(K key, K destKey, Circle within) {
		return searchAndStore(key, destKey, GeoReference.fromCircle(within), GeoShape.byRadius(within.getRadius()),
				GeoSearchStoreCommandArgs.newGeoSearchStoreArgs());
	}

	/**
	 * Get the {@literal member}s using {@link GeoReference} as center of the query within the boundaries of a given
	 * {@link Distance radius} and store results at {@code destKey}.
	 *
	 * @param key must not be {@literal null}.
	 * @param reference must not be {@literal null}.
	 * @param radius must not be {@literal null}.
	 * @return never {@literal null} unless used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/geosearchstore">Redis Documentation: GEOSEARCHSTORE</a>
	 */
	default Long searchAndStore(K key, K destKey, GeoReference<M> reference, Distance radius) {
		return searchAndStore(key, destKey, reference, radius, GeoSearchStoreCommandArgs.newGeoSearchStoreArgs());
	}

	/**
	 * Get the {@literal member}s using {@link GeoReference} as center of the query within the boundaries of a given
	 * {@link Distance radius} applying {@link GeoRadiusCommandArgs} and store results at {@code destKey}.
	 *
	 * @param key must not be {@literal null}.
	 * @param reference must not be {@literal null}.
	 * @param radius must not be {@literal null}.
	 * @param args must not be {@literal null}.
	 * @return never {@literal null} unless used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/geosearchstore">Redis Documentation: GEOSEARCHSTORE</a>
	 */
	default Long searchAndStore(K key, K destKey, GeoReference<M> reference, Distance radius,
			GeoSearchStoreCommandArgs args) {
		return searchAndStore(key, destKey, reference, GeoShape.byRadius(radius), args);
	}

	/**
	 * Get the {@literal member}s using {@link GeoReference} as center of the query within the boundaries of a given
	 * bounding box and store results at {@code destKey}.
	 *
	 * @param key must not be {@literal null}.
	 * @param reference must not be {@literal null}.
	 * @param boundingBox must not be {@literal null}.
	 * @return never {@literal null} unless used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/geosearchstore">Redis Documentation: GEOSEARCHSTORE</a>
	 */
	default Long searchAndStore(K key, K destKey, GeoReference<M> reference, BoundingBox boundingBox) {
		return searchAndStore(key, destKey, reference, boundingBox, GeoSearchStoreCommandArgs.newGeoSearchStoreArgs());
	}

	/**
	 * Get the {@literal member}s using {@link GeoReference} as center of the query within the boundaries of a given
	 * bounding box applying {@link GeoRadiusCommandArgs} and store results at {@code destKey}.
	 *
	 * @param key must not be {@literal null}.
	 * @param reference must not be {@literal null}.
	 * @param boundingBox must not be {@literal null}.
	 * @param args must not be {@literal null}.
	 * @return never {@literal null} unless used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/geosearchstore">Redis Documentation: GEOSEARCHSTORE</a>
	 */
	default Long searchAndStore(K key, K destKey, GeoReference<M> reference, BoundingBox boundingBox,
			GeoSearchStoreCommandArgs args) {
		return searchAndStore(key, destKey, reference, GeoShape.byBox(boundingBox), args);
	}

	/**
	 * Get the {@literal member}s using {@link GeoReference} as center of the query within the boundaries of a given
	 * {@link GeoShape predicate} applying {@link GeoRadiusCommandArgs} and store results at {@code destKey}.
	 *
	 * @param key must not be {@literal null}.
	 * @param reference must not be {@literal null}.
	 * @param geoPredicate must not be {@literal null}.
	 * @param args must not be {@literal null}.
	 * @return never {@literal null} unless used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/geosearchstore">Redis Documentation: GEOSEARCHSTORE</a>
	 */
	Long searchAndStore(K key, K destKey, GeoReference<M> reference, GeoShape geoPredicate,
			GeoSearchStoreCommandArgs args);

}
