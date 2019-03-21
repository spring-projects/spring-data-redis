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
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.connection.RedisGeoCommands.GeoLocation;
import org.springframework.data.redis.connection.RedisGeoCommands.GeoRadiusCommandArgs;

/**
 * Default implementation of {@link BoundGeoOperations}.
 * 
 * @author Ninad Divadkar
 * @author Christoph Strobl
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
	public DefaultBoundGeoOperations(K key, RedisOperations<K, M> operations) {

		super(key, operations);
		this.ops = operations.opsForGeo();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundGeoOperations#geoAdd(org.springframework.data.geo.Point, java.lang.Object)
	 */
	@Override
	public Long geoAdd(Point point, M member) {
		return ops.geoAdd(getKey(), point, member);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundGeoOperations#geoAdd(org.springframework.data.redis.connection.RedisGeoCommands.GeoLocation)
	 */
	@Override
	public Long geoAdd(GeoLocation<M> location) {
		return ops.geoAdd(getKey(), location);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundGeoOperations#geoAdd(java.util.Map)
	 */
	@Override
	public Long geoAdd(Map<M, Point> memberCoordinateMap) {
		return ops.geoAdd(getKey(), memberCoordinateMap);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundGeoOperations#geoAdd(java.lang.Iterable)
	 */
	@Override
	public Long geoAdd(Iterable<GeoLocation<M>> locations) {
		return ops.geoAdd(getKey(), locations);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundGeoOperations#geoDist(java.lang.Object, java.lang.Object)
	 */
	@Override
	public Distance geoDist(M member1, M member2) {
		return ops.geoDist(getKey(), member1, member2);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundGeoOperations#geoDist(java.lang.Object, java.lang.Object, org.springframework.data.geo.Metric)
	 */
	@Override
	public Distance geoDist(M member1, M member2, Metric unit) {
		return ops.geoDist(getKey(), member1, member2, unit);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundGeoOperations#geoHash(java.lang.Object[])
	 */
	@Override
	public List<String> geoHash(M... members) {
		return ops.geoHash(getKey(), members);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundGeoOperations#geoPos(java.lang.Object[])
	 */
	@Override
	public List<Point> geoPos(M... members) {
		return ops.geoPos(getKey(), members);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundGeoOperations#georadius(org.springframework.data.geo.Circle)
	 */
	@Override
	public GeoResults<GeoLocation<M>> georadius(Circle within) {
		return ops.georadius(getKey(), within);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundGeoOperations#georadius(org.springframework.data.geo.Circle, org.springframework.data.redis.core.GeoRadiusCommandArgs)
	 */
	@Override
	public GeoResults<GeoLocation<M>> georadius(Circle within, GeoRadiusCommandArgs param) {
		return ops.georadius(getKey(), within, param);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundGeoOperations#georadiusByMember(java.lang.Object, java.lang.Object, double)
	 */
	@Override
	public GeoResults<GeoLocation<M>> georadiusByMember(K key, M member, double radius) {
		return ops.georadiusByMember(getKey(), member, radius);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundGeoOperations#georadiusByMember(java.lang.Object, org.springframework.data.geo.Distance)
	 */
	@Override
	public GeoResults<GeoLocation<M>> georadiusByMember(M member, Distance distance) {
		return ops.georadiusByMember(getKey(), member, distance);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundGeoOperations#georadiusByMember(java.lang.Object, org.springframework.data.geo.Distance, org.springframework.data.redis.core.GeoRadiusCommandArgs)
	 */
	@Override
	public GeoResults<GeoLocation<M>> georadiusByMember(M member, Distance distance, GeoRadiusCommandArgs param) {
		return ops.georadiusByMember(getKey(), member, distance, param);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundGeoOperations#geoRemove(java.lang.Object[])
	 */
	@Override
	public Long geoRemove(M... members) {
		return ops.geoRemove(getKey(), members);
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
