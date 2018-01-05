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

import java.util.HashMap;
import java.util.LinkedHashMap;
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
 * Default implementation of {@link GeoOperations}.
 *
 * @author Ninad Divadkar
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 1.8
 */
class DefaultGeoOperations<K, M> extends AbstractOperations<K, M> implements GeoOperations<K, M> {

	/**
	 * Creates new {@link DefaultGeoOperations}.
	 *
	 * @param template must not be {@literal null}.
	 */
	DefaultGeoOperations(RedisTemplate<K, M> template) {
		super(template);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.GeoOperations#add(java.lang.Object, org.springframework.data.geo.Point, java.lang.Object)
	 */
	@Override
	public Long add(K key, Point point, M member) {

		byte[] rawKey = rawKey(key);
		byte[] rawMember = rawValue(member);

		return execute(connection -> connection.geoAdd(rawKey, point, rawMember), true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.GeoOperations#add(java.lang.Object, org.springframework.data.redis.connection.RedisGeoCommands.GeoLocation)
	 */
	@Override
	public Long add(K key, GeoLocation<M> location) {
		return add(key, location.getPoint(), location.getName());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.GeoOperations#add(java.lang.Object, java.util.Map)
	 */
	@Override
	public Long add(K key, Map<M, Point> memberCoordinateMap) {

		byte[] rawKey = rawKey(key);
		Map<byte[], Point> rawMemberCoordinateMap = new HashMap<>();

		for (M member : memberCoordinateMap.keySet()) {
			byte[] rawMember = rawValue(member);
			rawMemberCoordinateMap.put(rawMember, memberCoordinateMap.get(member));
		}

		return execute(connection -> connection.geoAdd(rawKey, rawMemberCoordinateMap), true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.GeoOperations#add(java.lang.Object, java.lang.Iterable)
	 */
	@Override
	public Long add(K key, Iterable<GeoLocation<M>> locations) {

		Map<M, Point> memberCoordinateMap = new LinkedHashMap<>();
		for (GeoLocation<M> location : locations) {
			memberCoordinateMap.put(location.getName(), location.getPoint());
		}

		return add(key, memberCoordinateMap);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.GeoOperations#distance(java.lang.Object, java.lang.Object, java.lang.Object)
	 */
	@Override
	public Distance distance(K key, M member1, M member2) {

		byte[] rawKey = rawKey(key);
		byte[] rawMember1 = rawValue(member1);
		byte[] rawMember2 = rawValue(member2);

		return execute(connection -> connection.geoDist(rawKey, rawMember1, rawMember2), true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.GeoOperations#distance(java.lang.Object, java.lang.Object, java.lang.Object, org.springframework.data.geo.Metric)
	 */
	@Override
	public Distance distance(K key, M member1, M member2, Metric metric) {

		byte[] rawKey = rawKey(key);
		byte[] rawMember1 = rawValue(member1);
		byte[] rawMember2 = rawValue(member2);

		return execute(connection -> connection.geoDist(rawKey, rawMember1, rawMember2, metric), true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.GeoOperations#hash(java.lang.Object, java.lang.Object[])
	 */
	@Override
	public List<String> hash(K key, M... members) {

		byte[] rawKey = rawKey(key);
		byte[][] rawMembers = rawValues(members);

		return execute(connection -> connection.geoHash(rawKey, rawMembers), true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.GeoOperations#position(java.lang.Object, java.lang.Object[])
	 */
	@Override
	public List<Point> position(K key, M... members) {
		byte[] rawKey = rawKey(key);
		byte[][] rawMembers = rawValues(members);

		return execute(connection -> connection.geoPos(rawKey, rawMembers), true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.GeoOperations#radius(java.lang.Object, org.springframework.data.geo.Circle)
	 */
	@Override
	public GeoResults<GeoLocation<M>> radius(K key, Circle within) {

		byte[] rawKey = rawKey(key);

		GeoResults<GeoLocation<byte[]>> raw = execute(connection -> connection.geoRadius(rawKey, within), true);

		return deserializeGeoResults(raw);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.GeoOperations#radius(java.lang.Object, org.springframework.data.geo.Circle, org.springframework.data.redis.core.GeoRadiusCommandArgs)
	 */
	@Override
	public GeoResults<GeoLocation<M>> radius(K key, Circle within, GeoRadiusCommandArgs args) {

		byte[] rawKey = rawKey(key);

		GeoResults<GeoLocation<byte[]>> raw = execute(connection -> connection.geoRadius(rawKey, within, args), true);

		return deserializeGeoResults(raw);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.GeoOperations#radius(java.lang.Object, java.lang.Object, double)
	 */
	@Override
	public GeoResults<GeoLocation<M>> radius(K key, M member, double radius) {

		byte[] rawKey = rawKey(key);
		byte[] rawMember = rawValue(member);
		GeoResults<GeoLocation<byte[]>> raw = execute(connection -> connection.geoRadiusByMember(rawKey, rawMember, radius),
				true);

		return deserializeGeoResults(raw);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.GeoOperations#radius(java.lang.Object, java.lang.Object, org.springframework.data.geo.Distance)
	 */
	@Override
	public GeoResults<GeoLocation<M>> radius(K key, M member, Distance distance) {

		byte[] rawKey = rawKey(key);
		byte[] rawMember = rawValue(member);

		GeoResults<GeoLocation<byte[]>> raw = execute(
				connection -> connection.geoRadiusByMember(rawKey, rawMember, distance), true);

		return deserializeGeoResults(raw);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.GeoOperations#radius(java.lang.Object, java.lang.Object, double, org.springframework.data.geo.Metric, org.springframework.data.redis.core.GeoRadiusCommandArgs)
	 */
	@Override
	public GeoResults<GeoLocation<M>> radius(K key, M member, Distance distance, GeoRadiusCommandArgs param) {

		byte[] rawKey = rawKey(key);
		byte[] rawMember = rawValue(member);

		GeoResults<GeoLocation<byte[]>> raw = execute(
				connection -> connection.geoRadiusByMember(rawKey, rawMember, distance, param), true);

		return deserializeGeoResults(raw);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.GeoOperations#remove(java.lang.Object, java.lang.Object[])
	 */
	@Override
	public Long remove(K key, M... members) {

		byte[] rawKey = rawKey(key);
		byte[][] rawMembers = rawValues(members);
		return execute(connection -> connection.zRem(rawKey, rawMembers), true);
	}
}
