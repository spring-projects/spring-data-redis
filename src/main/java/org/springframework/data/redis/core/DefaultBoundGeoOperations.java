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
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.connection.RedisGeoCommands.GeoLocation;
import org.springframework.data.redis.connection.RedisGeoCommands.GeoRadiusCommandArgs;

/**
 * Default implementation of {@link BoundGeoOperations}.
 *
 * @author Ninad Divadkar
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 1.8
 */
class DefaultBoundGeoOperations<K, M> extends DefaultBoundKeyOperations<K> implements BoundGeoOperations<K, M> {

	private final GeoOperations<K, M> ops;

	/**
	 * Constructs a new {@code DefaultBoundGeoOperations}.
	 *
	 * @param key must not be {@literal null}.
	 * @param operations must not be {@literal null}.
	 */
	DefaultBoundGeoOperations(K key, RedisOperations<K, M> operations) {

		super(key, operations);
		this.ops = operations.opsForGeo();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundGeoOperations#add(org.springframework.data.geo.Point, java.lang.Object)
	 */
	@Override
	public Long add(Point point, M member) {
		return ops.add(getKey(), point, member);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundGeoOperations#add(org.springframework.data.redis.connection.RedisGeoCommands.GeoLocation)
	 */
	@Override
	public Long add(GeoLocation<M> location) {
		return ops.add(getKey(), location);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundGeoOperations#add(java.util.Map)
	 */
	@Override
	public Long add(Map<M, Point> memberCoordinateMap) {
		return ops.add(getKey(), memberCoordinateMap);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundGeoOperations#add(java.lang.Iterable)
	 */
	@Override
	public Long add(Iterable<GeoLocation<M>> locations) {
		return ops.add(getKey(), locations);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundGeoOperations#distance(java.lang.Object, java.lang.Object)
	 */
	@Override
	public Distance distance(M member1, M member2) {
		return ops.distance(getKey(), member1, member2);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundGeoOperations#distance(java.lang.Object, java.lang.Object, org.springframework.data.geo.Metric)
	 */
	@Override
	public Distance distance(M member1, M member2, Metric unit) {
		return ops.distance(getKey(), member1, member2, unit);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundGeoOperations#hash(java.lang.Object[])
	 */
	@Override
	public List<String> hash(M... members) {
		return ops.hash(getKey(), members);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundGeoOperations#position(java.lang.Object[])
	 */
	@Override
	public List<Point> position(M... members) {
		return ops.position(getKey(), members);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundGeoOperations#radius(org.springframework.data.geo.Circle)
	 */
	@Override
	public GeoResults<GeoLocation<M>> radius(Circle within) {
		return ops.radius(getKey(), within);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundGeoOperations#radius(org.springframework.data.geo.Circle, org.springframework.data.redis.core.GeoRadiusCommandArgs)
	 */
	@Override
	public GeoResults<GeoLocation<M>> radius(Circle within, GeoRadiusCommandArgs param) {
		return ops.radius(getKey(), within, param);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundGeoOperations#radius(java.lang.Object, java.lang.Object, double)
	 */
	@Override
	public GeoResults<GeoLocation<M>> radius(K key, M member, double radius) {
		return ops.radius(getKey(), member, radius);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundGeoOperations#radius(java.lang.Object, org.springframework.data.geo.Distance)
	 */
	@Override
	public GeoResults<GeoLocation<M>> radius(M member, Distance distance) {
		return ops.radius(getKey(), member, distance);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundGeoOperations#radius(java.lang.Object, org.springframework.data.geo.Distance, org.springframework.data.redis.core.radiusCommandArgs)
	 */
	@Override
	public GeoResults<GeoLocation<M>> radius(M member, Distance distance, GeoRadiusCommandArgs param) {
		return ops.radius(getKey(), member, distance, param);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundGeoOperations#remove(java.lang.Object[])
	 */
	@Override
	public Long remove(M... members) {
		return ops.remove(getKey(), members);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundKeyOperations#getType()
	 */
	@Override
	public DataType getType() {
		return DataType.ZSET;
	}

}
