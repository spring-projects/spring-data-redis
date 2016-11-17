/*
 * Copyright 2016 the original author or authors.
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

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.geo.Circle;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.geo.Metric;
import org.springframework.data.geo.Point;
import org.springframework.util.Assert;

import lombok.Data;
import lombok.RequiredArgsConstructor;

/**
 * Geo-specific Redis commands.
 *
 * @author Ninad Divadkar
 * @author Christoph Strobl
 * @since 1.8
 */
public interface RedisGeoCommands {

	/**
	 * Add {@link Point} with given member {@literal name} to {@literal key}.
	 * 
	 * @param key must not be {@literal null}.
	 * @param point must not be {@literal null}.
	 * @param member must not be {@literal null}.
	 * @return Number of elements added.
	 * @see <a href="http://redis.io/commands/geoadd">http://redis.io/commands/geoadd</a>
	 */
	Long geoAdd(byte[] key, Point point, byte[] member);

	/**
	 * Add {@link GeoLocation} to {@literal key}.
	 * 
	 * @param key must not be {@literal null}.
	 * @param location must not be {@literal null}.
	 * @return Number of elements added.
	 * @see <a href="http://redis.io/commands/geoadd">http://redis.io/commands/geoadd</a>
	 */
	Long geoAdd(byte[] key, GeoLocation<byte[]> location);

	/**
	 * Add {@link Map} of member / {@link Point} pairs to {@literal key}.
	 * 
	 * @param key must not be {@literal null}.
	 * @param memberCoordinateMap must not be {@literal null}.
	 * @return Number of elements added.
	 * @see <a href="http://redis.io/commands/geoadd">http://redis.io/commands/geoadd</a>
	 */
	Long geoAdd(byte[] key, Map<byte[], Point> memberCoordinateMap);

	/**
	 * Add {@link GeoLocation}s to {@literal key}
	 * 
	 * @param key must not be {@literal null}.
	 * @param locations must not be {@literal null}.
	 * @return Number of elements added.
	 * @see <a href="http://redis.io/commands/geoadd">http://redis.io/commands/geoadd</a>
	 */
	Long geoAdd(byte[] key, Iterable<GeoLocation<byte[]>> locations);

	/**
	 * Get the {@link Distance} between {@literal member1} and {@literal member2}.
	 * 
	 * @param key must not be {@literal null}.
	 * @param member1 must not be {@literal null}.
	 * @param member2 must not be {@literal null}.
	 * @return can be {@literal null}.
	 * @see <a href="http://redis.io/commands/geodist">http://redis.io/commands/geodist</a>
	 */
	Distance geoDist(byte[] key, byte[] member1, byte[] member2);

	/**
	 * Get the {@link Distance} between {@literal member1} and {@literal member2} in the given {@link Metric}.
	 * 
	 * @param key must not be {@literal null}.
	 * @param member1 must not be {@literal null}.
	 * @param member2 must not be {@literal null}.
	 * @param metric must not be {@literal null}.
	 * @return can be {@literal null}.
	 * @see <a href="http://redis.io/commands/geodist">http://redis.io/commands/geodist</a>
	 */
	Distance geoDist(byte[] key, byte[] member1, byte[] member2, Metric metric);

	/**
	 * Get Geohash representation of the position for one or more {@literal member}s.
	 * 
	 * @param key must not be {@literal null}.
	 * @param members must not be {@literal null}.
	 * @return never {@literal null}.
	 * @see <a href="http://redis.io/commands/geohash">http://redis.io/commands/geohash</a>
	 */
	List<String> geoHash(byte[] key, byte[]... members);

	/**
	 * Get the {@link Point} representation of positions for one or more {@literal member}s.
	 * 
	 * @param key must not be {@literal null}.
	 * @param members must not be {@literal null}.
	 * @return never {@literal null}.
	 * @see <a href="http://redis.io/commands/geopos">http://redis.io/commands/geopos</a>
	 */
	List<Point> geoPos(byte[] key, byte[]... members);

	/**
	 * Get the {@literal member}s within the boundaries of a given {@link Circle}.
	 * 
	 * @param key must not be {@literal null}.
	 * @param within must not be {@literal null}.
	 * @return never {@literal null}.
	 * @see <a href="http://redis.io/commands/georadius">http://redis.io/commands/georadius</a>
	 */
	GeoResults<GeoLocation<byte[]>> geoRadius(byte[] key, Circle within);

	/**
	 * Get the {@literal member}s within the boundaries of a given {@link Circle} applying {@link GeoRadiusCommandArgs}.
	 * 
	 * @param key must not be {@literal null}.
	 * @param within must not be {@literal null}.
	 * @param args must not be {@literal null}.
	 * @return never {@literal null}.
	 * @see <a href="http://redis.io/commands/georadius">http://redis.io/commands/georadius</a>
	 */
	GeoResults<GeoLocation<byte[]>> geoRadius(byte[] key, Circle within, GeoRadiusCommandArgs args);

	/**
	 * Get the {@literal member}s within the circle defined by the {@literal members} coordinates and given
	 * {@literal radius}.
	 * 
	 * @param key must not be {@literal null}.
	 * @param member must not be {@literal null}.
	 * @param radius
	 * @return never {@literal null}.
	 * @see <a href="http://redis.io/commands/georadiusbymember">http://redis.io/commands/georadiusbymember</a>
	 */
	GeoResults<GeoLocation<byte[]>> geoRadiusByMember(byte[] key, byte[] member, double radius);

	/**
	 * Get the {@literal member}s within the circle defined by the {@literal members} coordinates and given
	 * {@link Distance}.
	 * 
	 * @param key must not be {@literal null}.
	 * @param member must not be {@literal null}.
	 * @param radius must not be {@literal null}.
	 * @return never {@literal null}.
	 * @see <a href="http://redis.io/commands/georadiusbymember">http://redis.io/commands/georadiusbymember</a>
	 */
	GeoResults<GeoLocation<byte[]>> geoRadiusByMember(byte[] key, byte[] member, Distance radius);

	/**
	 * Get the {@literal member}s within the circle defined by the {@literal members} coordinates, given {@link Distance}
	 * and {@link GeoRadiusCommandArgs}.
	 * 
	 * @param key must not be {@literal null}.
	 * @param member must not be {@literal null}.
	 * @param radius must not be {@literal null}.
	 * @param args must not be {@literal null}.
	 * @return never {@literal null}.
	 * @see <a href="http://redis.io/commands/georadiusbymember">http://redis.io/commands/georadiusbymember</a>
	 */
	GeoResults<GeoLocation<byte[]>> geoRadiusByMember(byte[] key, byte[] member, Distance radius,
			GeoRadiusCommandArgs args);

	/**
	 * Remove the {@literal member}s.
	 *
	 * @param key must not be {@literal null}.
	 * @param members must not be {@literal null}.
	 * @return Number of elements removed.
	 */
	Long geoRemove(byte[] key, byte[]... members);

	/**
	 * Additional arguments (like count/sort/...) to be used with {@link RedisGeoCommands}.
	 * 
	 * @author Ninad Divadkar
	 * @author Christoph Strobl
	 * @since 1.8
	 */
	public class GeoRadiusCommandArgs implements Cloneable {

		Set<Flag> flags = new LinkedHashSet<Flag>(2, 1);
		Long limit;
		Direction sortDirection;

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

			flags.add(Flag.WITHCOORD);
			return this;
		}

		/**
		 * Sets the {@link Flag#WITHDIST} flag to also return the distance of the returned items from the specified center.
		 *
		 * @return never {@literal null}.
		 */
		public GeoRadiusCommandArgs includeDistance() {

			flags.add(Flag.WITHDIST);
			return this;
		}

		/**
		 * Sort returned items from the nearest to the furthest, relative to the center.
		 * 
		 * @return never {@literal null}.
		 */
		public GeoRadiusCommandArgs sortAscending() {

			sortDirection = Direction.ASC;
			return this;
		}

		/**
		 * Sort returned items from the furthest to the nearest, relative to the center.
		 * 
		 * @return never {@literal null}.
		 */
		public GeoRadiusCommandArgs sortDescending() {

			sortDirection = Direction.DESC;
			return this;
		}

		/**
		 * Limit the results to the first N matching items.
		 * 
		 * @param count
		 * @return never {@literal null}.
		 */
		public GeoRadiusCommandArgs limit(long count) {

			Assert.isTrue(count > 0, "Count has to positive value.");
			limit = count;
			return this;
		}

		/**
		 * @return never {@literal null}.
		 */
		public Set<Flag> getFlags() {
			return flags;
		}

		/**
		 * @return can be {@literal null}.
		 */
		public Long getLimit() {
			return limit;
		}

		/**
		 * @return can be {@literal null}.
		 */
		public Direction getSortDirection() {
			return sortDirection;
		}

		public boolean hasFlags() {
			return !flags.isEmpty();
		}

		public boolean hasSortDirection() {
			return sortDirection != null;
		}

		public boolean hasLimit() {
			return limit != null;
		}

		public static enum Flag {
			WITHCOORD, WITHDIST
		}

		@Override
		protected GeoRadiusCommandArgs clone() {

			GeoRadiusCommandArgs tmp = new GeoRadiusCommandArgs();
			tmp.flags = this.flags != null ? new LinkedHashSet<>(this.flags) : new LinkedHashSet<>(2);
			tmp.limit = this.limit;
			tmp.sortDirection = this.sortDirection;
			return tmp;
		}
	}

	/**
	 * {@link GeoLocation} representing a {@link Point} associated with a {@literal name}.
	 * 
	 * @author Christoph Strobl
	 * @param <T>
	 * @since 1.8
	 */
	@Data
	@RequiredArgsConstructor
	public static class GeoLocation<T> {

		private final T name;
		private final Point point;
	}

	/**
	 * {@link Metric}s supported by Redis.
	 *
	 * @author Christoph Strobl
	 * @since 1.8
	 */
	public static enum DistanceUnit implements Metric {

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
		 * @see org.springframework.data.mongodb.core.geo.Metric#getMultiplier()
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
