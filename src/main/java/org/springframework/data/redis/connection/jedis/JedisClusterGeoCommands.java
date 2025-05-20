/*
 * Copyright 2017-2025 the original author or authors.
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
package org.springframework.data.redis.connection.jedis;

import redis.clients.jedis.GeoCoordinate;
import redis.clients.jedis.args.GeoUnit;
import redis.clients.jedis.params.GeoRadiusParam;
import redis.clients.jedis.params.GeoSearchParam;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullUnmarked;
import org.springframework.dao.DataAccessException;
import org.springframework.data.geo.Circle;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.geo.Metric;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.data.redis.domain.geo.GeoShape;
import org.springframework.util.Assert;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.0
 */
@NullUnmarked
class JedisClusterGeoCommands implements RedisGeoCommands {

	private final JedisClusterConnection connection;

	JedisClusterGeoCommands(JedisClusterConnection connection) {

		Assert.notNull(connection, "Connection must not be null");
		this.connection = connection;
	}

	@Override
	public Long geoAdd(byte @NonNull [] key, @NonNull Point point, byte @NonNull [] member) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(point, "Point must not be null");
		Assert.notNull(member, "Member must not be null");

		try {
			return connection.getCluster().geoadd(key, point.getX(), point.getY(), member);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long geoAdd(byte @NonNull [] key, @NonNull Map<byte @NonNull [], @NonNull Point> memberCoordinateMap) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(memberCoordinateMap, "MemberCoordinateMap must not be null");

		Map<byte[], GeoCoordinate> redisGeoCoordinateMap = new HashMap<>();
		for (byte[] mapKey : memberCoordinateMap.keySet()) {
			redisGeoCoordinateMap.put(mapKey, JedisConverters.toGeoCoordinate(memberCoordinateMap.get(mapKey)));
		}

		try {
			return connection.getCluster().geoadd(key, redisGeoCoordinateMap);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long geoAdd(byte @NonNull [] key, @NonNull Iterable<@NonNull GeoLocation<byte[]>> locations) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(locations, "Locations must not be null");

		Map<byte[], redis.clients.jedis.GeoCoordinate> redisGeoCoordinateMap = new HashMap<>();
		for (GeoLocation<byte[]> location : locations) {
			redisGeoCoordinateMap.put(location.getName(), JedisConverters.toGeoCoordinate(location.getPoint()));
		}

		try {
			return connection.getCluster().geoadd(key, redisGeoCoordinateMap);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Distance geoDist(byte @NonNull [] key, byte @NonNull [] member1, byte @NonNull [] member2) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(member1, "Member1 must not be null");
		Assert.notNull(member2, "Member2 must not be null");

		try {
			return JedisConverters.distanceConverterForMetric(DistanceUnit.METERS)
					.convert(connection.getCluster().geodist(key, member1, member2));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Distance geoDist(byte @NonNull [] key, byte @NonNull [] member1, byte @NonNull [] member2,
			@NonNull Metric metric) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(member1, "Member1 must not be null");
		Assert.notNull(member2, "Member2 must not be null");
		Assert.notNull(metric, "Metric must not be null");

		GeoUnit geoUnit = JedisConverters.toGeoUnit(metric);
		try {
			return JedisConverters.distanceConverterForMetric(metric)
					.convert(connection.getCluster().geodist(key, member1, member2, geoUnit));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public List<@NonNull String> geoHash(byte @NonNull [] key, byte @NonNull [] @NonNull... members) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(members, "Members must not be null");
		Assert.noNullElements(members, "Members must not contain null");

		try {
			return JedisConverters.toStrings(connection.getCluster().geohash(key, members));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public List<@NonNull Point> geoPos(byte @NonNull [] key, byte @NonNull [] @NonNull... members) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(members, "Members must not be null");
		Assert.noNullElements(members, "Members must not contain null");

		try {
			return JedisConverters.geoCoordinateToPointConverter().convert(connection.getCluster().geopos(key, members));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public GeoResults<GeoLocation<byte[]>> geoRadius(byte @NonNull [] key, @NonNull Circle within) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(within, "Within must not be null");

		try {
			return JedisConverters.geoRadiusResponseToGeoResultsConverter(within.getRadius().getMetric())
					.convert(connection.getCluster().georadius(key, within.getCenter().getX(), within.getCenter().getY(),
							within.getRadius().getValue(), JedisConverters.toGeoUnit(within.getRadius().getMetric())));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public GeoResults<GeoLocation<byte[]>> geoRadius(byte @NonNull [] key, @NonNull Circle within,
			@NonNull GeoRadiusCommandArgs args) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(within, "Within must not be null");
		Assert.notNull(args, "Args must not be null");

		GeoRadiusParam geoRadiusParam = JedisConverters.toGeoRadiusParam(args);

		try {
			return JedisConverters.geoRadiusResponseToGeoResultsConverter(within.getRadius().getMetric())
					.convert(connection.getCluster().georadius(key, within.getCenter().getX(), within.getCenter().getY(),
							within.getRadius().getValue(), JedisConverters.toGeoUnit(within.getRadius().getMetric()),
							geoRadiusParam));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public GeoResults<GeoLocation<byte[]>> geoRadiusByMember(byte @NonNull [] key, byte @NonNull [] member,
			@NonNull Distance radius) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(member, "Member must not be null");
		Assert.notNull(radius, "Radius must not be null");

		GeoUnit geoUnit = JedisConverters.toGeoUnit(radius.getMetric());
		try {
			return JedisConverters.geoRadiusResponseToGeoResultsConverter(radius.getMetric())
					.convert(connection.getCluster().georadiusByMember(key, member, radius.getValue(), geoUnit));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public GeoResults<GeoLocation<byte[]>> geoRadiusByMember(byte @NonNull [] key, byte @NonNull [] member,
			@NonNull Distance radius, @NonNull GeoRadiusCommandArgs args) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(member, "Member must not be null");
		Assert.notNull(radius, "Radius must not be null");
		Assert.notNull(args, "Args must not be null");

		GeoUnit geoUnit = JedisConverters.toGeoUnit(radius.getMetric());
		redis.clients.jedis.params.GeoRadiusParam geoRadiusParam = JedisConverters.toGeoRadiusParam(args);

		try {
			return JedisConverters.geoRadiusResponseToGeoResultsConverter(radius.getMetric())
					.convert(connection.getCluster().georadiusByMember(key, member, radius.getValue(), geoUnit, geoRadiusParam));

		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long geoRemove(byte @NonNull [] key, byte @NonNull [] @NonNull... members) {
		return connection.zRem(key, members);
	}

	@Override
	public GeoResults<GeoLocation<byte[]>> geoSearch(byte @NonNull [] key, @NonNull GeoReference<byte[]> reference,
			@NonNull GeoShape predicate, @NonNull GeoSearchCommandArgs args) {

		Assert.notNull(key, "Key must not be null");
		GeoSearchParam params = JedisConverters.toGeoSearchParams(reference, predicate, args);

		try {

			return JedisConverters.geoRadiusResponseToGeoResultsConverter(predicate.getMetric())
					.convert(connection.getCluster().geosearch(key, params));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long geoSearchStore(byte @NonNull [] destKey, byte @NonNull [] key, @NonNull GeoReference<byte[]> reference,
			@NonNull GeoShape predicate, @NonNull GeoSearchStoreCommandArgs args) {

		Assert.notNull(destKey, "Destination Key must not be null");
		Assert.notNull(key, "Key must not be null");
		GeoSearchParam params = JedisConverters.toGeoSearchParams(reference, predicate, args);

		try {

			if (args.isStoreDistance()) {
				return connection.getCluster().geosearchStoreStoreDist(destKey, key, params);
			}

			return connection.getCluster().geosearchStore(destKey, key, params);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	private DataAccessException convertJedisAccessException(Exception ex) {
		return connection.convertJedisAccessException(ex);
	}
}
