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
package org.springframework.data.redis.connection;

import static org.springframework.data.redis.connection.RedisGeoCommands.GeoRadiusCommandArgs.*;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.geo.Circle;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.geo.Metric;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.data.redis.domain.geo.GeoShape;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Geo-specific Redis commands.
 *
 * @author Ninad Divadkar
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 1.8
 */
public interface RedisGeoCommands {

	/**
	 * Add {@link Point} with given member {@literal name} to {@literal key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param point must not be {@literal null}.
	 * @param member must not be {@literal null}.
	 * @return Number of elements added. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/geoadd">Redis Documentation: GEOADD</a>
	 */
	@Nullable
	Long geoAdd(byte[] key, Point point, byte[] member);

	/**
	 * Add {@link GeoLocation} to {@literal key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param location must not be {@literal null}.
	 * @return Number of elements added. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/geoadd">Redis Documentation: GEOADD</a>
	 */
	@Nullable
	default Long geoAdd(byte[] key, GeoLocation<byte[]> location) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(location, "Location must not be null!");

		return geoAdd(key, location.getPoint(), location.getName());
	}

	/**
	 * Add {@link Map} of member / {@link Point} pairs to {@literal key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param memberCoordinateMap must not be {@literal null}.
	 * @return Number of elements added. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/geoadd">Redis Documentation: GEOADD</a>
	 */
	@Nullable
	Long geoAdd(byte[] key, Map<byte[], Point> memberCoordinateMap);

	/**
	 * Add {@link GeoLocation}s to {@literal key}
	 *
	 * @param key must not be {@literal null}.
	 * @param locations must not be {@literal null}.
	 * @return Number of elements added. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/geoadd">Redis Documentation: GEOADD</a>
	 */
	@Nullable
	Long geoAdd(byte[] key, Iterable<GeoLocation<byte[]>> locations);

	/**
	 * Get the {@link Distance} between {@literal member1} and {@literal member2}.
	 *
	 * @param key must not be {@literal null}.
	 * @param member1 must not be {@literal null}.
	 * @param member2 must not be {@literal null}.
	 * @return can be {@literal null}. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/geodist">Redis Documentation: GEODIST</a>
	 */
	@Nullable
	Distance geoDist(byte[] key, byte[] member1, byte[] member2);

	/**
	 * Get the {@link Distance} between {@literal member1} and {@literal member2} in the given {@link Metric}.
	 *
	 * @param key must not be {@literal null}.
	 * @param member1 must not be {@literal null}.
	 * @param member2 must not be {@literal null}.
	 * @param metric must not be {@literal null}.
	 * @return can be {@literal null}. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/geodist">Redis Documentation: GEODIST</a>
	 */
	@Nullable
	Distance geoDist(byte[] key, byte[] member1, byte[] member2, Metric metric);

	/**
	 * Get Geohash representation of the position for one or more {@literal member}s.
	 *
	 * @param key must not be {@literal null}.
	 * @param members must not be {@literal null}.
	 * @return empty list when key or members do not exists. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/geohash">Redis Documentation: GEOHASH</a>
	 */
	@Nullable
	List<String> geoHash(byte[] key, byte[]... members);

	/**
	 * Get the {@link Point} representation of positions for one or more {@literal member}s.
	 *
	 * @param key must not be {@literal null}.
	 * @param members must not be {@literal null}.
	 * @return empty {@link List} when key of members do not exist. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/geopos">Redis Documentation: GEOPOS</a>
	 */
	@Nullable
	List<Point> geoPos(byte[] key, byte[]... members);

	/**
	 * Get the {@literal member}s within the boundaries of a given {@link Circle}.
	 *
	 * @param key must not be {@literal null}.
	 * @param within must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/georadius">Redis Documentation: GEORADIUS</a>
	 */
	@Nullable
	GeoResults<GeoLocation<byte[]>> geoRadius(byte[] key, Circle within);

	/**
	 * Get the {@literal member}s within the boundaries of a given {@link Circle} applying {@link GeoRadiusCommandArgs}.
	 *
	 * @param key must not be {@literal null}.
	 * @param within must not be {@literal null}.
	 * @param args must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/georadius">Redis Documentation: GEORADIUS</a>
	 */
	@Nullable
	GeoResults<GeoLocation<byte[]>> geoRadius(byte[] key, Circle within, GeoRadiusCommandArgs args);

	/**
	 * Get the {@literal member}s within the circle defined by the {@literal members} coordinates and given
	 * {@literal radius}.
	 *
	 * @param key must not be {@literal null}.
	 * @param member must not be {@literal null}.
	 * @param radius
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/georadiusbymember">Redis Documentation: GEORADIUSBYMEMBER</a>
	 */
	@Nullable
	default GeoResults<GeoLocation<byte[]>> geoRadiusByMember(byte[] key, byte[] member, double radius) {
		return geoRadiusByMember(key, member, new Distance(radius, DistanceUnit.METERS));
	}

	/**
	 * Get the {@literal member}s within the circle defined by the {@literal members} coordinates and given
	 * {@link Distance}.
	 *
	 * @param key must not be {@literal null}.
	 * @param member must not be {@literal null}.
	 * @param radius must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction..
	 * @see <a href="https://redis.io/commands/georadiusbymember">Redis Documentation: GEORADIUSBYMEMBER</a>
	 */
	@Nullable
	GeoResults<GeoLocation<byte[]>> geoRadiusByMember(byte[] key, byte[] member, Distance radius);

	/**
	 * Get the {@literal member}s within the circle defined by the {@literal members} coordinates, given {@link Distance}
	 * and {@link GeoRadiusCommandArgs}.
	 *
	 * @param key must not be {@literal null}.
	 * @param member must not be {@literal null}.
	 * @param radius must not be {@literal null}.
	 * @param args must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/georadiusbymember">Redis Documentation: GEORADIUSBYMEMBER</a>
	 */
	@Nullable
	GeoResults<GeoLocation<byte[]>> geoRadiusByMember(byte[] key, byte[] member, Distance radius,
			GeoRadiusCommandArgs args);

	/**
	 * Remove the {@literal member}s.
	 *
	 * @param key must not be {@literal null}.
	 * @param members must not be {@literal null}.
	 * @return Number of elements removed. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zrem">Redis Documentation: ZREM</a>
	 */
	@Nullable
	Long geoRemove(byte[] key, byte[]... members);

	/**
	 * Return the members of a geo set which are within the borders of the area specified by a given {@link GeoShape
	 * shape}. The query's center point is provided by {@link GeoReference}.
	 *
	 * @param key must not be {@literal null}.
	 * @param reference must not be {@literal null}.
	 * @param predicate must not be {@literal null}.
	 * @param args must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/geosearch">Redis Documentation: GEOSEARCH</a>
	 */
	@Nullable
	GeoResults<GeoLocation<byte[]>> geoSearch(byte[] key, GeoReference<byte[]> reference, GeoShape predicate,
			GeoSearchCommandArgs args);

	/**
	 * Query the members of a geo set which are within the borders of the area specified by a given {@link GeoShape shape}
	 * and store the result at {@code destKey}. The query's center point is provided by {@link GeoReference}.
	 *
	 * @param key must not be {@literal null}.
	 * @param reference must not be {@literal null}.
	 * @param predicate must not be {@literal null}.
	 * @param args must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/geosearch">Redis Documentation: GEOSEARCH</a>
	 */
	@Nullable
	Long geoSearchStore(byte[] destKey, byte[] key, GeoReference<byte[]> reference, GeoShape predicate,
			GeoSearchStoreCommandArgs args);

	/**
	 * Arguments to be used with {@link RedisGeoCommands}.
	 *
	 * @author Christoph Strobl
	 * @since 2.6
	 */
	interface GeoCommandArgs {

		/**
		 * @return can be {@literal null}.
		 */
		@Nullable
		public Direction getSortDirection();

		/**
		 * @return can be {@literal null}.
		 */
		@Nullable
		Long getLimit();

		/**
		 * @return never {@literal null}.
		 */
		Set<? extends GeoCommandFlag> getFlags();

		/**
		 * @return {@literal true} if {@literal limit} has been set.
		 */
		default boolean hasLimit() {
			return getLimit() != null;
		}

		/**
		 * @return {@literal true} if {@literal sort} has been set.
		 */
		default boolean hasSortDirection() {
			return getSortDirection() != null;
		}

		/**
		 * @return {@literal true} if {@literal flags} is not empty.
		 */
		default boolean hasFlags() {
			return !getFlags().isEmpty();
		}

		/**
		 * A flag to be used.
		 */
		interface GeoCommandFlag {

			static GeoCommandFlag any() {
				return Flag.ANY;
			}

			static GeoCommandFlag withCord() {
				return Flag.WITHCOORD;
			}

			static GeoCommandFlag withDist() {
				return Flag.WITHDIST;
			}

			static GeoCommandFlag storeDist() {
				return Flag.STOREDIST;
			}
		}
	}

	/**
	 * Additional arguments (like count/sort/...) to be used with {@link RedisGeoCommands}.
	 *
	 * @author Mark Paluch
	 * @author Christoph Strobl
	 * @since 2.6
	 */
	class GeoSearchCommandArgs implements GeoCommandArgs, Cloneable {

		protected final Set<GeoCommandFlag> flags = new LinkedHashSet<>(2, 1);

		@Nullable protected Long limit;

		@Nullable protected Direction sortDirection;

		private GeoSearchCommandArgs() {}

		/**
		 * Create new {@link GeoSearchCommandArgs}.
		 *
		 * @return never {@literal null}.
		 */
		public static GeoSearchCommandArgs newGeoSearchArgs() {
			return new GeoSearchCommandArgs();
		}

		/**
		 * Sets the {@link Flag#WITHCOORD} flag to also return the longitude, latitude coordinates of the matching items.
		 *
		 * @return
		 */
		public GeoSearchCommandArgs includeCoordinates() {

			flags.add(GeoCommandFlag.withCord());
			return this;
		}

		/**
		 * Sets the {@link Flag#WITHDIST} flag to also return the distance of the returned items from the specified center.
		 *
		 * @return never {@literal null}.
		 */
		public GeoSearchCommandArgs includeDistance() {

			flags.add(GeoCommandFlag.withDist());
			return this;
		}

		/**
		 * Apply a sort direction.
		 *
		 * @return never {@literal null}.
		 */
		public GeoSearchCommandArgs sort(Direction direction) {

			Assert.notNull(direction, "Sort direction must not be null");

			this.sortDirection = direction;
			return this;
		}

		/**
		 * Sort returned items from the nearest to the furthest, relative to the center.
		 *
		 * @return never {@literal null}.
		 */
		public GeoSearchCommandArgs sortAscending() {
			return sort(Direction.ASC);
		}

		/**
		 * Sort returned items from the furthest to the nearest, relative to the center.
		 *
		 * @return never {@literal null}.
		 */
		public GeoSearchCommandArgs sortDescending() {
			return sort(Direction.DESC);
		}

		/**
		 * Limit the results to the first N matching items.
		 *
		 * @param count
		 * @return never {@literal null}.
		 */
		public GeoSearchCommandArgs limit(long count) {
			return limit(count, false);
		}

		/**
		 * Limit the results to the first N matching items.
		 *
		 * @param count
		 * @param any
		 * @return never {@literal null}.
		 */
		public GeoSearchCommandArgs limit(long count, boolean any) {

			Assert.isTrue(count > 0, "Count has to positive value.");
			limit = count;
			if (any) {
				flags.add(GeoCommandFlag.any());
			}
			return this;
		}

		/**
		 * @return never {@literal null}.
		 */
		public Set<? extends GeoCommandFlag> getFlags() {
			return flags;
		}

		/**
		 * @return can be {@literal null}.
		 */
		@Nullable
		public Long getLimit() {
			return limit;
		}

		/**
		 * @return can be {@literal null}.
		 */
		@Nullable
		public Direction getSortDirection() {
			return sortDirection;
		}

		public boolean hasAnyLimit() {
			return hasLimit() && flags.contains(GeoCommandFlag.any());
		}

		@Override
		protected GeoSearchCommandArgs clone() {

			GeoSearchCommandArgs that = new GeoSearchCommandArgs();
			that.flags.addAll(this.flags);
			that.limit = this.limit;
			that.sortDirection = this.sortDirection;
			return that;
		}
	}

	/**
	 * Additional arguments (like count/sort/...) to be used with {@link RedisGeoCommands}.
	 *
	 * @author Mark Paluch
	 * @since 2.6
	 */
	class GeoSearchStoreCommandArgs implements GeoCommandArgs, Cloneable {

		private final Set<GeoCommandFlag> flags = new LinkedHashSet<>(2, 1);

		@Nullable private Long limit;

		@Nullable private Direction sortDirection;

		private GeoSearchStoreCommandArgs() {}

		/**
		 * Create new {@link GeoSearchStoreCommandArgs}.
		 *
		 * @return never {@literal null}.
		 */
		public static GeoSearchStoreCommandArgs newGeoSearchStoreArgs() {
			return new GeoSearchStoreCommandArgs();
		}

		/**
		 * Sets the {@link Flag#STOREDIST} flag to also store the distance of the returned items from the specified center.
		 *
		 * @return never {@literal null}.
		 */
		public GeoSearchStoreCommandArgs storeDistance() {

			flags.add(Flag.STOREDIST);
			return this;
		}

		/**
		 * Apply a sort direction.
		 *
		 * @return never {@literal null}.
		 */
		public GeoSearchStoreCommandArgs sort(Direction direction) {

			Assert.notNull(direction, "Sort direction must not be null");

			sortDirection = Direction.ASC;
			return this;
		}

		/**
		 * Sort returned items from the nearest to the furthest, relative to the center.
		 *
		 * @return never {@literal null}.
		 */
		public GeoSearchStoreCommandArgs sortAscending() {
			return sort(Direction.ASC);
		}

		/**
		 * Sort returned items from the furthest to the nearest, relative to the center.
		 *
		 * @return never {@literal null}.
		 */
		public GeoSearchStoreCommandArgs sortDescending() {
			return sort(Direction.DESC);
		}

		/**
		 * Limit the results to the first N matching items.
		 *
		 * @param count
		 * @return never {@literal null}.
		 */
		public GeoSearchStoreCommandArgs limit(long count) {
			return limit(count, false);
		}

		/**
		 * Limit the results to the first N matching items.
		 *
		 * @param count
		 * @param any
		 * @return never {@literal null}.
		 */
		public GeoSearchStoreCommandArgs limit(long count, boolean any) {

			Assert.isTrue(count > 0, "Count has to positive value.");
			this.limit = count;
			if (any) {
				flags.add(GeoCommandFlag.any());
			}
			return this;
		}

		/**
		 * @return never {@literal null}.
		 */
		public Set<GeoCommandFlag> getFlags() {
			return flags;
		}

		/**
		 * @return can be {@literal null}.
		 */
		@Nullable
		public Long getLimit() {
			return limit;
		}

		/**
		 * @return can be {@literal null}.
		 */
		@Nullable
		public Direction getSortDirection() {
			return sortDirection;
		}

		public boolean isStoreDistance() {
			return flags.contains(GeoCommandFlag.storeDist());
		}

		public boolean hasAnyLimit() {
			return hasLimit() && flags.contains(GeoCommandFlag.any());
		}

		@Override
		protected GeoSearchStoreCommandArgs clone() {

			GeoSearchStoreCommandArgs that = new GeoSearchStoreCommandArgs();
			that.flags.addAll(this.flags);
			that.limit = this.limit;
			that.sortDirection = this.sortDirection;
			return that;
		}
	}

	/**
	 * Additional arguments (like count/sort/...) to be used with {@link RedisGeoCommands}.
	 *
	 * @author Ninad Divadkar
	 * @author Christoph Strobl
	 * @since 1.8
	 */
	class GeoRadiusCommandArgs extends GeoSearchCommandArgs implements Cloneable {

		private GeoRadiusCommandArgs() {}

		/**
		 * Create new {@link GeoRadiusCommandArgs}.
		 *
		 * @return never {@literal null}.
		 */
		public static GeoRadiusCommandArgs newGeoRadiusArgs() {
			return new GeoRadiusCommandArgs();
		}

		/**
		 * Sets the {@link Flag#WITHCOORD} flag to also return the longitude, latitude coordinates of the matching items.
		 *
		 * @return
		 */
		public GeoRadiusCommandArgs includeCoordinates() {
			super.includeCoordinates();
			return this;
		}

		/**
		 * Sets the {@link Flag#WITHDIST} flag to also return the distance of the returned items from the specified center.
		 *
		 * @return never {@literal null}.
		 */
		public GeoRadiusCommandArgs includeDistance() {
			super.includeDistance();
			return this;
		}

		/**
		 * Apply a sort direction.
		 *
		 * @return never {@literal null}.
		 * @since 2.6
		 */
		public GeoRadiusCommandArgs sort(Direction direction) {
			super.sort(direction);
			return this;
		}

		/**
		 * Sort returned items from the nearest to the furthest, relative to the center.
		 *
		 * @return never {@literal null}.
		 */
		public GeoRadiusCommandArgs sortAscending() {
			super.sortAscending();
			return this;
		}

		/**
		 * Sort returned items from the furthest to the nearest, relative to the center.
		 *
		 * @return never {@literal null}.
		 */
		public GeoRadiusCommandArgs sortDescending() {
			super.sortDescending();
			return this;
		}

		/**
		 * Limit the results to the first N matching items.
		 *
		 * @param count
		 * @return never {@literal null}.
		 */
		public GeoRadiusCommandArgs limit(long count) {
			super.limit(count);
			return this;
		}

		public Set<Flag> getFlags() {
			return flags.stream().map(it -> (Flag) it).collect(Collectors.toSet());
		}

		public enum Flag implements GeoCommandFlag {
			WITHCOORD, WITHDIST, ANY, STOREDIST
		}

		@Override
		protected GeoRadiusCommandArgs clone() {

			GeoRadiusCommandArgs that = new GeoRadiusCommandArgs();
			that.flags.addAll(this.flags);
			that.limit = this.limit;
			that.sortDirection = this.sortDirection;
			return that;
		}
	}

	/**
	 * {@link GeoLocation} representing a {@link Point} associated with a {@literal name}.
	 *
	 * @author Christoph Strobl
	 * @param <T>
	 * @since 1.8
	 */
	class GeoLocation<T> extends org.springframework.data.redis.domain.geo.GeoLocation<T> {

		public GeoLocation(T name, Point point) {
			super(name, point);
		}

		public String toString() {
			return "RedisGeoCommands.GeoLocation(name=" + this.getName() + ", point=" + this.getPoint() + ")";
		}
	}

	/**
	 * {@link Metric}s supported by Redis.
	 *
	 * @author Christoph Strobl
	 * @since 1.8
	 */
	enum DistanceUnit implements Metric {

		METERS(6378137, "m"), KILOMETERS(6378.137, "km"), MILES(3963.191, "mi"), FEET(20925646.325, "ft");

		private final double multiplier;
		private final String abbreviation;

		/**
		 * Creates a new {@link DistanceUnit} using the given muliplier.
		 *
		 * @param multiplier the earth radius at equator.
		 */
		private DistanceUnit(double multiplier, String abbreviation) {

			this.multiplier = multiplier;
			this.abbreviation = abbreviation;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.geo.Metric#getMultiplier()
		 */
		public double getMultiplier() {
			return multiplier;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.geo.Metric#getAbbreviation()
		 */
		@Override
		public String getAbbreviation() {
			return abbreviation;
		}
	}
}
