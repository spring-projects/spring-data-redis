/*
 * Copyright 2016-2022 the original author or authors.
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
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.connection.RedisGeoCommands.GeoLocation;
import org.springframework.data.redis.connection.RedisGeoCommands.GeoRadiusCommandArgs;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.data.redis.domain.geo.GeoShape;

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

	@Override
	public Long add(Point point, M member) {
		return ops.add(getKey(), point, member);
	}

	@Override
	public Long add(GeoLocation<M> location) {
		return ops.add(getKey(), location);
	}

	@Override
	public Long add(Map<M, Point> memberCoordinateMap) {
		return ops.add(getKey(), memberCoordinateMap);
	}

	@Override
	public Long add(Iterable<GeoLocation<M>> locations) {
		return ops.add(getKey(), locations);
	}

	@Override
	public Distance distance(M member1, M member2) {
		return ops.distance(getKey(), member1, member2);
	}

	@Override
	public Distance distance(M member1, M member2, Metric unit) {
		return ops.distance(getKey(), member1, member2, unit);
	}

	@Override
	public List<String> hash(M... members) {
		return ops.hash(getKey(), members);
	}

	@Override
	public List<Point> position(M... members) {
		return ops.position(getKey(), members);
	}

	@Override
	public GeoResults<GeoLocation<M>> radius(Circle within) {
		return ops.radius(getKey(), within);
	}

	@Override
	public GeoResults<GeoLocation<M>> radius(Circle within, GeoRadiusCommandArgs param) {
		return ops.radius(getKey(), within, param);
	}

	@Override
	public GeoResults<GeoLocation<M>> radius(K key, M member, double radius) {
		return ops.radius(getKey(), member, radius);
	}

	@Override
	public GeoResults<GeoLocation<M>> radius(M member, Distance distance) {
		return ops.radius(getKey(), member, distance);
	}

	@Override
	public GeoResults<GeoLocation<M>> radius(M member, Distance distance, GeoRadiusCommandArgs param) {
		return ops.radius(getKey(), member, distance, param);
	}

	@Override
	public Long remove(M... members) {
		return ops.remove(getKey(), members);
	}

	@Override
	public GeoResults<GeoLocation<M>> search(GeoReference<M> reference,
			GeoShape geoPredicate, RedisGeoCommands.GeoSearchCommandArgs args) {
		return ops.search(getKey(), reference, geoPredicate, args);
	}

	@Override
	public Long searchAndStore(K destKey, GeoReference<M> reference,
			GeoShape geoPredicate, RedisGeoCommands.GeoSearchStoreCommandArgs args) {
		return ops.searchAndStore(getKey(), destKey, reference, geoPredicate, args);
	}

	@Override
	public DataType getType() {
		return DataType.ZSET;
	}

}
