/*
 * Copyright 2016-2025 the original author or authors.
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

import static org.springframework.data.redis.connection.RedisGeoCommands.*;

import java.util.List;
import java.util.Map;

import org.springframework.data.geo.Circle;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.geo.Metric;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.domain.geo.BoundingBox;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.data.redis.domain.geo.GeoShape;
import org.springframework.lang.Nullable;

/**
 * {@link GeoOperations} bound to a certain key.
 *
 * @author Ninad Divadkar
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 1.8
 */
public interface BoundGeoOperations<K, M> extends BoundKeyOperations<K> {

	/**
	 * Add {@link Point} with given member {@literal name} to {@literal key}.
	 *
	 * @param point must not be {@literal null}.
	 * @param member must not be {@literal null}.
	 * @return Number of elements added. {@literal null} when used in pipeline / transaction.
	 * @since 2.0
	 * @see <a href="https://redis.io/commands/geoadd">Redis Documentation: GEOADD</a>
	 */
	@Nullable
	Long add(Point point, M member);

	/**
	 * Add {@link GeoLocation} to {@literal key}.
	 *
	 * @param location must not be {@literal null}.
	 * @return Number of elements added. {@literal null} when used in pipeline / transaction.
	 * @since 2.0
	 * @see <a href="https://redis.io/commands/geoadd">Redis Documentation: GEOADD</a>
	 */
	@Nullable
	Long add(GeoLocation<M> location);

	/**
	 * Add {@link Map} of member / {@link Point} pairs to {@literal key}.
	 *
	 * @param memberCoordinateMap must not be {@literal null}.
	 * @return Number of elements added. {@literal null} when used in pipeline / transaction.
	 * @since 2.0
	 * @see <a href="https://redis.io/commands/geoadd">Redis Documentation: GEOADD</a>
	 */
	@Nullable
	Long add(Map<M, Point> memberCoordinateMap);

	/**
	 * Add {@link GeoLocation}s to {@literal key}
	 *
	 * @param locations must not be {@literal null}.
	 * @return Number of elements added. {@literal null} when used in pipeline / transaction.
	 * @since 2.0
	 * @see <a href="https://redis.io/commands/geoadd">Redis Documentation: GEOADD</a>
	 */
	@Nullable
	Long add(Iterable<GeoLocation<M>> locations);

	/**
	 * Get the {@link Distance} between {@literal member1} and {@literal member2}.
	 *
	 * @param member1 must not be {@literal null}.
	 * @param member2 must not be {@literal null}.
	 * @return can be {@literal null}.
	 * @since 2.0
	 * @see <a href="https://redis.io/commands/geodist">Redis Documentation: GEODIST</a>
	 */
	@Nullable
	Distance distance(M member1, M member2);

	/**
	 * Get the {@link Distance} between {@literal member1} and {@literal member2} in the given {@link Metric}.
	 *
	 * @param member1 must not be {@literal null}.
	 * @param member2 must not be {@literal null}.
	 * @param metric must not be {@literal null}.
	 * @return can be {@literal null}.
	 * @since 2.0
	 * @see <a href="https://redis.io/commands/geodist">Redis Documentation: GEODIST</a>
	 */
	@Nullable
	Distance distance(M member1, M member2, Metric metric);

	/**
	 * Get Geohash representation of the position for one or more {@literal member}s.
	 *
	 * @param members must not be {@literal null}.
	 * @return never {@literal null} unless used in pipeline / transaction.
	 * @since 2.0
	 * @see <a href="https://redis.io/commands/geohash">Redis Documentation: GEOHASH</a>
	 */
	@Nullable
	List<String> hash(M... members);

	/**
	 * Get the {@link Point} representation of positions for one or more {@literal member}s.
	 *
	 * @param members must not be {@literal null}.
	 * @return never {@literal null} unless used in pipeline / transaction.
	 * @since 2.0
	 * @see <a href="https://redis.io/commands/geopos">Redis Documentation: GEOPOS</a>
	 */
	@Nullable
	List<Point> position(M... members);

	/**
	 * Get the {@literal member}s within the boundaries of a given {@link Circle}.
	 *
	 * @param within must not be {@literal null}.
	 * @return never {@literal null} unless used in pipeline / transaction.
	 * @since 2.0
	 * @see <a href="https://redis.io/commands/georadius">Redis Documentation: GEORADIUS</a>
	 */
	@Nullable
	GeoResults<GeoLocation<M>> radius(Circle within);

	/**
	 * Get the {@literal member}s within the boundaries of a given {@link Circle} applying {@link GeoRadiusCommandArgs}.
	 *
	 * @param within must not be {@literal null}.
	 * @param args must not be {@literal null}.
	 * @return never {@literal null} unless used in pipeline / transaction.
	 * @since 2.0
	 * @see <a href="https://redis.io/commands/georadius">Redis Documentation: GEORADIUS</a>
	 */
	@Nullable
	GeoResults<GeoLocation<M>> radius(Circle within, GeoRadiusCommandArgs args);

	/**
	 * Get the {@literal member}s within the circle defined by the {@literal members} coordinates and given
	 * {@literal radius}.
	 *
	 * @param member must not be {@literal null}.
	 * @param radius
	 * @return never {@literal null} unless used in pipeline / transaction.
	 * @since 2.0
	 * @see <a href="https://redis.io/commands/georadiusbymember">Redis Documentation: GEORADIUSBYMEMBER</a>
	 */
	@Nullable
	GeoResults<GeoLocation<M>> radius(M member, double radius);

	/**
	 * Get the {@literal member}s within the circle defined by the {@literal members} coordinates and given
	 * {@literal radius} applying {@link Metric}.
	 *
	 * @param member must not be {@literal null}.
	 * @param distance must not be {@literal null}.
	 * @return never {@literal null} unless used in pipeline / transaction.
	 * @since 2.0
	 * @see <a href="https://redis.io/commands/georadiusbymember">Redis Documentation: GEORADIUSBYMEMBER</a>
	 */
	@Nullable
	GeoResults<GeoLocation<M>> radius(M member, Distance distance);

	/**
	 * Get the {@literal member}s within the circle defined by the {@literal members} coordinates and given
	 * {@literal radius} applying {@link Metric} and {@link GeoRadiusCommandArgs}.
	 *
	 * @param member must not be {@literal null}.
	 * @param distance must not be {@literal null}.
	 * @param args must not be {@literal null}.
	 * @return never {@literal null} unless used in pipeline / transaction.
	 * @since 2.0
	 * @see <a href="https://redis.io/commands/georadiusbymember">Redis Documentation: GEORADIUSBYMEMBER</a>
	 */
	@Nullable
	GeoResults<GeoLocation<M>> radius(M member, Distance distance, GeoRadiusCommandArgs args);

	/**
	 * Remove the {@literal member}s.
	 *
	 * @param members must not be {@literal null}.
	 * @return Number of elements removed. {@literal null} when used in pipeline / transaction.
	 * @since 2.0
	 */
	@Nullable
	Long remove(M... members);

	/**
	 * Get the {@literal member}s within the boundaries of a given {@link Circle}.
	 *
	 * @param within must not be {@literal null}.
	 * @return never {@literal null} unless used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/geosearch">Redis Documentation: GEOSEARCH</a>
	 */
	@Nullable
	default GeoResults<GeoLocation<M>> search(Circle within) {
		return search(GeoReference.fromCircle(within), GeoShape.byRadius(within.getRadius()),
				GeoSearchCommandArgs.newGeoSearchArgs());
	}

	/**
	 * Get the {@literal member}s using {@link GeoReference} as center of the query within the boundaries of a given
	 * {@link Distance radius}.
	 *
	 * @param reference must not be {@literal null}.
	 * @param radius must not be {@literal null}.
	 * @return never {@literal null} unless used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/geosearch">Redis Documentation: GEOSEARCH</a>
	 */
	@Nullable
	default GeoResults<GeoLocation<M>> search(GeoReference<M> reference, Distance radius) {
		return search(reference, radius, GeoSearchCommandArgs.newGeoSearchArgs());
	}

	/**
	 * Get the {@literal member}s using {@link GeoReference} as center of the query within the boundaries of a given
	 * {@link Distance radius} applying {@link GeoRadiusCommandArgs}.
	 *
	 * @param reference must not be {@literal null}.
	 * @param radius must not be {@literal null}.
	 * @param args must not be {@literal null}.
	 * @return never {@literal null} unless used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/geosearch">Redis Documentation: GEOSEARCH</a>
	 */
	@Nullable
	default GeoResults<GeoLocation<M>> search(GeoReference<M> reference, Distance radius, GeoSearchCommandArgs args) {
		return search(reference, GeoShape.byRadius(radius), args);
	}

	/**
	 * Get the {@literal member}s using {@link GeoReference} as center of the query within the boundaries of a given
	 * bounding box.
	 *
	 * @param reference must not be {@literal null}.
	 * @param boundingBox must not be {@literal null}.
	 * @return never {@literal null} unless used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/geosearch">Redis Documentation: GEOSEARCH</a>
	 */
	@Nullable
	default GeoResults<GeoLocation<M>> search(GeoReference<M> reference, BoundingBox boundingBox) {
		return search(reference, boundingBox, GeoSearchCommandArgs.newGeoSearchArgs());
	}

	/**
	 * Get the {@literal member}s using {@link GeoReference} as center of the query within the boundaries of a given
	 * bounding box applying {@link GeoRadiusCommandArgs}.
	 *
	 * @param reference must not be {@literal null}.
	 * @param boundingBox must not be {@literal null}.
	 * @param args must not be {@literal null}.
	 * @return never {@literal null} unless used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/geosearch">Redis Documentation: GEOSEARCH</a>
	 */
	@Nullable
	default GeoResults<GeoLocation<M>> search(GeoReference<M> reference, BoundingBox boundingBox,
			GeoSearchCommandArgs args) {
		return search(reference, GeoShape.byBox(boundingBox), args);
	}

	/**
	 * Get the {@literal member}s using {@link GeoReference} as center of the query within the boundaries of a given
	 * {@link GeoShape predicate} applying {@link GeoRadiusCommandArgs}.
	 *
	 * @param reference must not be {@literal null}.
	 * @param geoPredicate must not be {@literal null}.
	 * @param args must not be {@literal null}.
	 * @return never {@literal null} unless used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/geosearch">Redis Documentation: GEOSEARCH</a>
	 */
	@Nullable
	GeoResults<GeoLocation<M>> search(GeoReference<M> reference, GeoShape geoPredicate, GeoSearchCommandArgs args);

	/**
	 * Get the {@literal member}s within the boundaries of a given {@link Circle} and store results at {@code destKey}.
	 *
	 * @param within must not be {@literal null}.
	 * @return never {@literal null} unless used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/geosearchstore">Redis Documentation: GEOSEARCHSTORE</a>
	 */
	@Nullable
	default Long searchAndStore(K destKey, Circle within) {
		return searchAndStore(destKey, GeoReference.fromCircle(within), GeoShape.byRadius(within.getRadius()),
				GeoSearchStoreCommandArgs.newGeoSearchStoreArgs());
	}

	/**
	 * Get the {@literal member}s using {@link GeoReference} as center of the query within the boundaries of a given
	 * {@link Distance radius} and store results at {@code destKey}.
	 *
	 * @param reference must not be {@literal null}.
	 * @param radius must not be {@literal null}.
	 * @return never {@literal null} unless used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/geosearchstore">Redis Documentation: GEOSEARCHSTORE</a>
	 */
	@Nullable
	default Long searchAndStore(K destKey, GeoReference<M> reference, Distance radius) {
		return searchAndStore(destKey, reference, radius, GeoSearchStoreCommandArgs.newGeoSearchStoreArgs());
	}

	/**
	 * Get the {@literal member}s using {@link GeoReference} as center of the query within the boundaries of a given
	 * {@link Distance radius} applying {@link GeoRadiusCommandArgs} and store results at {@code destKey}.
	 *
	 * @param reference must not be {@literal null}.
	 * @param radius must not be {@literal null}.
	 * @param args must not be {@literal null}.
	 * @return never {@literal null} unless used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/geosearchstore">Redis Documentation: GEOSEARCHSTORE</a>
	 */
	@Nullable
	default Long searchAndStore(K destKey, GeoReference<M> reference, Distance radius, GeoSearchStoreCommandArgs args) {
		return searchAndStore(destKey, reference, GeoShape.byRadius(radius), args);
	}

	/**
	 * Get the {@literal member}s using {@link GeoReference} as center of the query within the boundaries of a given
	 * bounding box and store results at {@code destKey}.
	 *
	 * @param reference must not be {@literal null}.
	 * @param boundingBox must not be {@literal null}.
	 * @return never {@literal null} unless used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/geosearchstore">Redis Documentation: GEOSEARCHSTORE</a>
	 */
	@Nullable
	default Long searchAndStore(K destKey, GeoReference<M> reference, BoundingBox boundingBox) {
		return searchAndStore(destKey, reference, boundingBox, GeoSearchStoreCommandArgs.newGeoSearchStoreArgs());
	}

	/**
	 * Get the {@literal member}s using {@link GeoReference} as center of the query within the boundaries of a given
	 * bounding box applying {@link GeoRadiusCommandArgs} and store results at {@code destKey}.
	 *
	 * @param reference must not be {@literal null}.
	 * @param boundingBox must not be {@literal null}.
	 * @param args must not be {@literal null}.
	 * @return never {@literal null} unless used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/geosearchstore">Redis Documentation: GEOSEARCHSTORE</a>
	 */
	@Nullable
	default Long searchAndStore(K destKey, GeoReference<M> reference, BoundingBox boundingBox,
			GeoSearchStoreCommandArgs args) {
		return searchAndStore(destKey, reference, GeoShape.byBox(boundingBox), args);
	}

	/**
	 * Get the {@literal member}s using {@link GeoReference} as center of the query within the boundaries of a given
	 * {@link GeoShape predicate} applying {@link GeoRadiusCommandArgs} and store results at {@code destKey}.
	 *
	 * @param reference must not be {@literal null}.
	 * @param geoPredicate must not be {@literal null}.
	 * @param args must not be {@literal null}.
	 * @return never {@literal null} unless used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/geosearchstore">Redis Documentation: GEOSEARCHSTORE</a>
	 */
	@Nullable
	Long searchAndStore(K destKey, GeoReference<M> reference, GeoShape geoPredicate, GeoSearchStoreCommandArgs args);

}
