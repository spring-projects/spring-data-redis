/*
 * Copyright 2017-2018 the original author or authors.
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
package org.springframework.data.redis.connection.jedis;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import redis.clients.jedis.GeoCoordinate;
import redis.clients.jedis.GeoUnit;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.geo.Circle;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.geo.Metric;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.connection.convert.ListConverter;
import org.springframework.util.Assert;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.0
 */
@RequiredArgsConstructor
class JedisGeoCommands implements RedisGeoCommands {

	private final @NonNull JedisConnection connection;

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisGeoCommands#geoAdd(byte[], org.springframework.data.geo.Point, byte[])
	 */
	@Override
	public Long geoAdd(byte[] key, Point point, byte[] member) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(point, "Point must not be null!");
		Assert.notNull(member, "Member must not be null!");

		try {
			if (isPipelined()) {
				pipeline(connection
						.newJedisResult(connection.getRequiredPipeline().geoadd(key, point.getX(), point.getY(), member)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection
						.newJedisResult(connection.getRequiredTransaction().geoadd(key, point.getX(), point.getY(), member)));
				return null;
			}

			return connection.getJedis().geoadd(key, point.getX(), point.getY(), member);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisGeoCommands#geoAdd(byte[], java.util.Map)
	 */
	@Override
	public Long geoAdd(byte[] key, Map<byte[], Point> memberCoordinateMap) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(memberCoordinateMap, "MemberCoordinateMap must not be null!");

		Map<byte[], GeoCoordinate> redisGeoCoordinateMap = new HashMap<>();

		for (byte[] mapKey : memberCoordinateMap.keySet()) {
			redisGeoCoordinateMap.put(mapKey, JedisConverters.toGeoCoordinate(memberCoordinateMap.get(mapKey)));
		}

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getRequiredPipeline().geoadd(key, redisGeoCoordinateMap)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getRequiredTransaction().geoadd(key, redisGeoCoordinateMap)));
				return null;
			}

			return connection.getJedis().geoadd(key, redisGeoCoordinateMap);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisGeoCommands#geoAdd(byte[], java.lang.Iterable)
	 */
	@Override
	public Long geoAdd(byte[] key, Iterable<GeoLocation<byte[]>> locations) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(locations, "Locations must not be null!");

		Map<byte[], redis.clients.jedis.GeoCoordinate> redisGeoCoordinateMap = new HashMap<>();

		for (GeoLocation<byte[]> location : locations) {
			redisGeoCoordinateMap.put(location.getName(), JedisConverters.toGeoCoordinate(location.getPoint()));
		}

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getRequiredPipeline().geoadd(key, redisGeoCoordinateMap)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getRequiredTransaction().geoadd(key, redisGeoCoordinateMap)));
				return null;
			}

			return connection.getJedis().geoadd(key, redisGeoCoordinateMap);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisGeoCommands#geoDist(byte[], byte[], byte[])
	 */
	@Override
	public Distance geoDist(byte[] key, byte[] member1, byte[] member2) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(member1, "Member1 must not be null!");
		Assert.notNull(member2, "Member2 must not be null!");

		Converter<Double, Distance> distanceConverter = JedisConverters.distanceConverterForMetric(DistanceUnit.METERS);

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getRequiredPipeline().geodist(key, member1, member2),
						distanceConverter));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getRequiredTransaction().geodist(key, member1, member2),
						distanceConverter));
				return null;
			}

			return distanceConverter.convert(connection.getJedis().geodist(key, member1, member2));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisGeoCommands#geoDist(byte[], byte[], byte[], org.springframework.data.geo.Metric)
	 */
	@Override
	public Distance geoDist(byte[] key, byte[] member1, byte[] member2, Metric metric) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(member1, "Member1 must not be null!");
		Assert.notNull(member2, "Member2 must not be null!");
		Assert.notNull(metric, "Metric must not be null!");

		GeoUnit geoUnit = JedisConverters.toGeoUnit(metric);
		Converter<Double, Distance> distanceConverter = JedisConverters.distanceConverterForMetric(metric);

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getRequiredPipeline().geodist(key, member1, member2, geoUnit),
						distanceConverter));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(
						connection.getRequiredTransaction().geodist(key, member1, member2, geoUnit), distanceConverter));
				return null;
			}

			return distanceConverter.convert(connection.getJedis().geodist(key, member1, member2, geoUnit));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisGeoCommands#geoHash(byte[], byte[][])
	 */
	@Override
	public List<String> geoHash(byte[] key, byte[]... members) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(members, "Members must not be null!");
		Assert.noNullElements(members, "Members must not contain null!");

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getRequiredPipeline().geohash(key, members),
						JedisConverters.bytesListToStringListConverter()));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getRequiredTransaction().geohash(key, members),
						JedisConverters.bytesListToStringListConverter()));
				return null;
			}

			return JedisConverters.bytesListToStringListConverter().convert(connection.getJedis().geohash(key, members));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisGeoCommands#geoPos(byte[], byte[][])
	 */
	@Override
	public List<Point> geoPos(byte[] key, byte[]... members) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(members, "Members must not be null!");
		Assert.noNullElements(members, "Members must not contain null!");

		ListConverter<GeoCoordinate, Point> converter = JedisConverters.geoCoordinateToPointConverter();
		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getRequiredPipeline().geopos(key, members), converter));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getRequiredTransaction().geopos(key, members), converter));
				return null;
			}
			return converter.convert(connection.getJedis().geopos(key, members));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisGeoCommands#geoRadius(byte[], org.springframework.data.geo.Circle)
	 */
	@Override
	public GeoResults<GeoLocation<byte[]>> geoRadius(byte[] key, Circle within) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(within, "Within must not be null!");

		Converter<List<redis.clients.jedis.GeoRadiusResponse>, GeoResults<GeoLocation<byte[]>>> converter = JedisConverters
				.geoRadiusResponseToGeoResultsConverter(within.getRadius().getMetric());
		try {
			if (isPipelined()) {
				pipeline(
						connection.newJedisResult(
								connection.getRequiredPipeline().georadius(key, within.getCenter().getX(), within.getCenter().getY(),
										within.getRadius().getValue(), JedisConverters.toGeoUnit(within.getRadius().getMetric())),
								converter));
				return null;
			}
			if (isQueueing()) {
				transaction(
						connection.newJedisResult(
								connection.getRequiredTransaction().georadius(key, within.getCenter().getX(), within.getCenter().getY(),
										within.getRadius().getValue(), JedisConverters.toGeoUnit(within.getRadius().getMetric())),
								converter));
				return null;
			}

			return converter
					.convert(connection.getJedis().georadius(key, within.getCenter().getX(), within.getCenter().getY(),
							within.getRadius().getValue(), JedisConverters.toGeoUnit(within.getRadius().getMetric())));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisGeoCommands#geoRadius(byte[], org.springframework.data.geo.Circle, org.springframework.data.redis.core.GeoRadiusCommandArgs)
	 */
	@Override
	public GeoResults<GeoLocation<byte[]>> geoRadius(byte[] key, Circle within, GeoRadiusCommandArgs args) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(within, "Within must not be null!");
		Assert.notNull(args, "Args must not be null!");

		redis.clients.jedis.params.geo.GeoRadiusParam geoRadiusParam = JedisConverters.toGeoRadiusParam(args);
		Converter<List<redis.clients.jedis.GeoRadiusResponse>, GeoResults<GeoLocation<byte[]>>> converter = JedisConverters
				.geoRadiusResponseToGeoResultsConverter(within.getRadius().getMetric());

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getRequiredPipeline().georadius(key, within.getCenter().getX(),
						within.getCenter().getY(), within.getRadius().getValue(),
						JedisConverters.toGeoUnit(within.getRadius().getMetric()), geoRadiusParam), converter));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getRequiredTransaction().georadius(key,
						within.getCenter().getX(), within.getCenter().getY(), within.getRadius().getValue(),
						JedisConverters.toGeoUnit(within.getRadius().getMetric()), geoRadiusParam), converter));
				return null;
			}

			return converter.convert(connection.getJedis().georadius(key, within.getCenter().getX(),
					within.getCenter().getY(), within.getRadius().getValue(),
					JedisConverters.toGeoUnit(within.getRadius().getMetric()), geoRadiusParam));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisGeoCommands#geoRadiusByMember(byte[], byte[], org.springframework.data.geo.Distance)
	 */
	@Override
	public GeoResults<GeoLocation<byte[]>> geoRadiusByMember(byte[] key, byte[] member, Distance radius) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(member, "Member must not be null!");
		Assert.notNull(radius, "Radius must not be null!");

		GeoUnit geoUnit = JedisConverters.toGeoUnit(radius.getMetric());
		Converter<List<redis.clients.jedis.GeoRadiusResponse>, GeoResults<GeoLocation<byte[]>>> converter = JedisConverters
				.geoRadiusResponseToGeoResultsConverter(radius.getMetric());

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(
						connection.getRequiredPipeline().georadiusByMember(key, member, radius.getValue(), geoUnit), converter));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(
						connection.getRequiredTransaction().georadiusByMember(key, member, radius.getValue(), geoUnit), converter));
				return null;
			}

			return converter.convert(connection.getJedis().georadiusByMember(key, member, radius.getValue(), geoUnit));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisGeoCommands#geoRadiusByMember(byte[], byte[], org.springframework.data.geo.Distance, org.springframework.data.redis.connection.RedisGeoCommands.GeoRadiusCommandArgs)
	 */
	@Override
	public GeoResults<GeoLocation<byte[]>> geoRadiusByMember(byte[] key, byte[] member, Distance radius,
			GeoRadiusCommandArgs args) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(member, "Member must not be null!");
		Assert.notNull(radius, "Radius must not be null!");
		Assert.notNull(args, "Args must not be null!");

		GeoUnit geoUnit = JedisConverters.toGeoUnit(radius.getMetric());
		Converter<List<redis.clients.jedis.GeoRadiusResponse>, GeoResults<GeoLocation<byte[]>>> converter = JedisConverters
				.geoRadiusResponseToGeoResultsConverter(radius.getMetric());
		redis.clients.jedis.params.geo.GeoRadiusParam geoRadiusParam = JedisConverters.toGeoRadiusParam(args);

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(
						connection.getRequiredPipeline().georadiusByMember(key, member, radius.getValue(), geoUnit, geoRadiusParam),
						converter));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getRequiredTransaction().georadiusByMember(key, member,
						radius.getValue(), geoUnit, geoRadiusParam), converter));
				return null;
			}
			return converter
					.convert(connection.getJedis().georadiusByMember(key, member, radius.getValue(), geoUnit, geoRadiusParam));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisGeoCommands#geoRemove(byte[], byte[][])
	 */
	@Override
	public Long geoRemove(byte[] key, byte[]... members) {
		return connection.zSetCommands().zRem(key, members);
	}

	private boolean isPipelined() {
		return connection.isPipelined();
	}

	private void pipeline(JedisResult result) {
		connection.pipeline(result);
	}

	private boolean isQueueing() {
		return connection.isQueueing();
	}

	private void transaction(JedisResult result) {
		connection.transaction(result);
	}

	private RuntimeException convertJedisAccessException(Exception ex) {
		return connection.convertJedisAccessException(ex);
	}
}
