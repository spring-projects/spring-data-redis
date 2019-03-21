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

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.data.geo.Circle;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.geo.Metric;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisGeoCommands.GeoLocation;
import org.springframework.data.redis.connection.RedisGeoCommands.GeoRadiusCommandArgs;

/**
 * Default implementation of {@link GeoOperations}.
 *
 * @author Ninad Divadkar
 * @author Christoph Strobl
 * @since 1.8
 */
public class DefaultGeoOperations<K, M> extends AbstractOperations<K, M> implements GeoOperations<K, M> {

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
	 * @see org.springframework.data.redis.core.GeoOperations#geoAdd(java.lang.Object, org.springframework.data.geo.Point, java.lang.Object)
	 */
	@Override
	public Long geoAdd(K key, final Point point, M member) {

		final byte[] rawKey = rawKey(key);
		final byte[] rawMember = rawValue(member);

		return execute(new RedisCallback<Long>() {

			public Long doInRedis(RedisConnection connection) {
				return connection.geoAdd(rawKey, point, rawMember);
			}
		}, true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.GeoOperations#geoAdd(java.lang.Object, org.springframework.data.redis.connection.RedisGeoCommands.GeoLocation)
	 */
	@Override
	public Long geoAdd(K key, GeoLocation<M> location) {
		return geoAdd(key, location.getPoint(), location.getName());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.GeoOperations#geoAdd(java.lang.Object, java.util.Map)
	 */
	@Override
	public Long geoAdd(K key, Map<M, Point> memberCoordinateMap) {

		final byte[] rawKey = rawKey(key);
		final Map<byte[], Point> rawMemberCoordinateMap = new HashMap<byte[], Point>();

		for (M member : memberCoordinateMap.keySet()) {
			final byte[] rawMember = rawValue(member);
			rawMemberCoordinateMap.put(rawMember, memberCoordinateMap.get(member));
		}

		return execute(new RedisCallback<Long>() {

			public Long doInRedis(RedisConnection connection) {
				return connection.geoAdd(rawKey, rawMemberCoordinateMap);
			}
		}, true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.GeoOperations#geoAdd(java.lang.Object, java.lang.Iterable)
	 */
	@Override
	public Long geoAdd(K key, Iterable<GeoLocation<M>> locations) {

		Map<M, Point> memberCoordinateMap = new LinkedHashMap<M, Point>();
		for (GeoLocation<M> location : locations) {
			memberCoordinateMap.put(location.getName(), location.getPoint());
		}

		return geoAdd(key, memberCoordinateMap);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.GeoOperations#geoDist(java.lang.Object, java.lang.Object, java.lang.Object)
	 */
	@Override
	public Distance geoDist(K key, final M member1, final M member2) {

		final byte[] rawKey = rawKey(key);
		final byte[] rawMember1 = rawValue(member1);
		final byte[] rawMember2 = rawValue(member2);

		return execute(new RedisCallback<Distance>() {

			public Distance doInRedis(RedisConnection connection) {
				return connection.geoDist(rawKey, rawMember1, rawMember2);
			}
		}, true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.GeoOperations#geoDist(java.lang.Object, java.lang.Object, java.lang.Object, org.springframework.data.geo.Metric)
	 */
	@Override
	public Distance geoDist(K key, M member1, M member2, final Metric metric) {

		final byte[] rawKey = rawKey(key);
		final byte[] rawMember1 = rawValue(member1);
		final byte[] rawMember2 = rawValue(member2);

		return execute(new RedisCallback<Distance>() {

			public Distance doInRedis(RedisConnection connection) {
				return connection.geoDist(rawKey, rawMember1, rawMember2, metric);
			}
		}, true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.GeoOperations#geoHash(java.lang.Object, java.lang.Object[])
	 */
	@Override
	public List<String> geoHash(K key, final M... members) {

		final byte[] rawKey = rawKey(key);
		final byte[][] rawMembers = rawValues(members);

		return execute(new RedisCallback<List<String>>() {

			public List<String> doInRedis(RedisConnection connection) {
				return connection.geoHash(rawKey, rawMembers);
			}
		}, true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.GeoOperations#geoPos(java.lang.Object, java.lang.Object[])
	 */
	@Override
	public List<Point> geoPos(K key, M... members) {
		final byte[] rawKey = rawKey(key);
		final byte[][] rawMembers = rawValues(members);

		return execute(new RedisCallback<List<Point>>() {

			public List<Point> doInRedis(RedisConnection connection) {
				return connection.geoPos(rawKey, rawMembers);
			}
		}, true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.GeoOperations#georadius(java.lang.Object, org.springframework.data.geo.Circle)
	 */
	@Override
	public GeoResults<GeoLocation<M>> georadius(K key, final Circle within) {

		final byte[] rawKey = rawKey(key);

		GeoResults<GeoLocation<byte[]>> raw = execute(new RedisCallback<GeoResults<GeoLocation<byte[]>>>() {

			public GeoResults<GeoLocation<byte[]>> doInRedis(RedisConnection connection) {
				return connection.geoRadius(rawKey, within);
			}
		}, true);

		return deserializeGeoResults(raw);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.GeoOperations#georadius(java.lang.Object, org.springframework.data.geo.Circle, org.springframework.data.redis.core.GeoRadiusCommandArgs)
	 */
	@Override
	public GeoResults<GeoLocation<M>> georadius(K key, final Circle within, final GeoRadiusCommandArgs args) {

		final byte[] rawKey = rawKey(key);

		GeoResults<GeoLocation<byte[]>> raw = execute(new RedisCallback<GeoResults<GeoLocation<byte[]>>>() {

			public GeoResults<GeoLocation<byte[]>> doInRedis(RedisConnection connection) {
				return connection.geoRadius(rawKey, within, args);
			}
		}, true);

		return deserializeGeoResults(raw);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.GeoOperations#georadiusByMember(java.lang.Object, java.lang.Object, double)
	 */
	@Override
	public GeoResults<GeoLocation<M>> georadiusByMember(K key, M member, final double radius) {

		final byte[] rawKey = rawKey(key);
		final byte[] rawMember = rawValue(member);
		GeoResults<GeoLocation<byte[]>> raw = execute(new RedisCallback<GeoResults<GeoLocation<byte[]>>>() {

			public GeoResults<GeoLocation<byte[]>> doInRedis(RedisConnection connection) {
				return connection.geoRadiusByMember(rawKey, rawMember, radius);
			}
		}, true);

		return deserializeGeoResults(raw);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.GeoOperations#georadiusByMember(java.lang.Object, java.lang.Object, org.springframework.data.geo.Distance)
	 */
	@Override
	public GeoResults<GeoLocation<M>> georadiusByMember(K key, M member, final Distance distance) {

		final byte[] rawKey = rawKey(key);
		final byte[] rawMember = rawValue(member);

		GeoResults<GeoLocation<byte[]>> raw = execute(new RedisCallback<GeoResults<GeoLocation<byte[]>>>() {

			public GeoResults<GeoLocation<byte[]>> doInRedis(RedisConnection connection) {
				return connection.geoRadiusByMember(rawKey, rawMember, distance);
			}
		}, true);

		return deserializeGeoResults(raw);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.GeoOperations#georadiusByMember(java.lang.Object, java.lang.Object, double, org.springframework.data.geo.Metric, org.springframework.data.redis.core.GeoRadiusCommandArgs)
	 */
	@Override
	public GeoResults<GeoLocation<M>> georadiusByMember(K key, M member, final Distance distance,
			final GeoRadiusCommandArgs param) {

		final byte[] rawKey = rawKey(key);
		final byte[] rawMember = rawValue(member);

		GeoResults<GeoLocation<byte[]>> raw = execute(new RedisCallback<GeoResults<GeoLocation<byte[]>>>() {

			public GeoResults<GeoLocation<byte[]>> doInRedis(RedisConnection connection) {
				return connection.geoRadiusByMember(rawKey, rawMember, distance, param);
			}
		}, true);

		return deserializeGeoResults(raw);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.GeoOperations#geoRemove(java.lang.Object, java.lang.Object[])
	 */
	@Override
	public Long geoRemove(K key, M... members) {

		final byte[] rawKey = rawKey(key);
		final byte[][] rawMembers = rawValues(members);

		return execute(new RedisCallback<Long>() {

			public Long doInRedis(RedisConnection connection) {
				return connection.zRem(rawKey, rawMembers);
			}
		}, true);
	}
}
