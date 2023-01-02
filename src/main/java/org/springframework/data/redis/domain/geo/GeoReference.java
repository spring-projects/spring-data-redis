/*
 * Copyright 2021-2023 the original author or authors.
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
package org.springframework.data.redis.domain.geo;

import org.springframework.data.geo.Circle;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

/**
 * Reference point for {@code GEOSEARCH} and {@code GEOSEARCHSTORE} commands. Provides factory methods to create
 * {@link GeoReference} from geo-set members or reference points.
 *
 * @param <T>
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.6
 */
public interface GeoReference<T> {

	/**
	 * Creates a {@link GeoReference} from a geoset member.
	 *
	 * @param member must not be {@literal null}.
	 * @param <T>
	 * @return
	 */
	static <T> GeoReference<T> fromMember(T member) {

		Assert.notNull(member, "Geoset member must not be null");

		return new GeoMemberReference<>(member);
	}

	/**
	 * Creates a {@link GeoReference} from a {@link RedisGeoCommands.GeoLocation geoset member}.
	 *
	 * @param member must not be {@literal null}.
	 * @param <T>
	 * @return
	 */
	static <T> GeoReference<T> fromMember(RedisGeoCommands.GeoLocation<T> member) {

		Assert.notNull(member, "GeoLocation must not be null");

		return new GeoMemberReference<>(member.getName());
	}

	/**
	 * Creates a {@link GeoReference} from a {@link Circle#getCenter() circle center point} .
	 *
	 * @param within must not be {@literal null}.
	 * @param <T>
	 * @return
	 */
	static <T> GeoReference<T> fromCircle(Circle within) {

		Assert.notNull(within, "Circle must not be null");

		return fromCoordinate(within.getCenter());
	}

	/**
	 * Creates a {@link GeoReference} from a WGS84 longitude/latitude coordinate.
	 *
	 * @param longitude
	 * @param latitude
	 * @param <T>
	 * @return
	 */
	static <T> GeoReference<T> fromCoordinate(double longitude, double latitude) {
		return new GeoCoordinateReference<>(longitude, latitude);
	}

	/**
	 * Creates a {@link GeoReference} from a WGS84 longitude/latitude coordinate.
	 *
	 * @param location must not be {@literal null}.
	 * @param <T>
	 * @return
	 */
	static <T> GeoReference<T> fromCoordinate(RedisGeoCommands.GeoLocation<?> location) {

		Assert.notNull(location, "GeoLocation must not be null");
		Assert.notNull(location.getPoint(), "GeoLocation point must not be null");

		return fromCoordinate(location.getPoint());
	}

	/**
	 * Creates a {@link GeoReference} from a WGS84 longitude/latitude coordinate.
	 *
	 * @param point must not be {@literal null}.
	 * @param <T>
	 * @return
	 */
	static <T> GeoReference<T> fromCoordinate(Point point) {

		Assert.notNull(point, "Reference point must not be null");

		return fromCoordinate(point.getX(), point.getY());
	}

	class GeoMemberReference<T> implements GeoReference<T> {

		private final T member;

		public GeoMemberReference(T member) {
			this.member = member;
		}

		public T getMember() {
			return member;
		}

		@Override
		public boolean equals(@Nullable Object o) {
			if (this == o) {
				return true;
			}
			if (!(o instanceof GeoReference.GeoMemberReference)) {
				return false;
			}
			GeoMemberReference<?> that = (GeoMemberReference<?>) o;
			return ObjectUtils.nullSafeEquals(member, that.member);
		}

		@Override
		public int hashCode() {
			return ObjectUtils.nullSafeHashCode(member);
		}

		@Override
		public String toString() {
			final StringBuffer sb = new StringBuffer();
			sb.append(getClass().getSimpleName());
			sb.append(" [member=").append(member);
			sb.append(']');
			return sb.toString();
		}
	}

	class GeoCoordinateReference<T> implements GeoReference<T> {

		private final double longitude;
		private final double latitude;

		public GeoCoordinateReference(double longitude, double latitude) {
			this.longitude = longitude;
			this.latitude = latitude;
		}

		public double getLongitude() {
			return longitude;
		}

		public double getLatitude() {
			return latitude;
		}

		@Override
		public boolean equals(@Nullable Object o) {
			if (this == o) {
				return true;
			}
			if (!(o instanceof GeoReference.GeoCoordinateReference)) {
				return false;
			}
			GeoCoordinateReference<?> that = (GeoCoordinateReference<?>) o;
			if (longitude != that.longitude) {
				return false;
			}
			return latitude == that.latitude;
		}

		@Override
		public int hashCode() {
			int result;
			long temp;
			temp = Double.doubleToLongBits(longitude);
			result = (int) (temp ^ (temp >>> 32));
			temp = Double.doubleToLongBits(latitude);
			result = 31 * result + (int) (temp ^ (temp >>> 32));
			return result;
		}

		@Override
		public String toString() {
			final StringBuffer sb = new StringBuffer();
			sb.append(getClass().getSimpleName());
			sb.append(" [").append(longitude);
			sb.append(",").append(latitude);
			sb.append(']');
			return sb.toString();
		}
	}
}
