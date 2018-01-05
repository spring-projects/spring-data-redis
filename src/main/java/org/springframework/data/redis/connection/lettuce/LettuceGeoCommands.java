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
package org.springframework.data.redis.connection.lettuce;

import io.lettuce.core.GeoArgs;
import io.lettuce.core.GeoCoordinates;
import io.lettuce.core.GeoWithin;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.core.convert.converter.Converter;
import org.springframework.dao.DataAccessException;
import org.springframework.data.geo.Circle;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.geo.Metric;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.connection.convert.ListConverter;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.0
 */
@RequiredArgsConstructor
class LettuceGeoCommands implements RedisGeoCommands {

	private final @NonNull LettuceConnection connection;

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
				pipeline(connection.newLettuceResult(getAsyncConnection().geoadd(key, point.getX(), point.getY(), member)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceResult(getAsyncConnection().geoadd(key, point.getX(), point.getY(), member)));
				return null;
			}
			return getConnection().geoadd(key, point.getX(), point.getY(), member);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
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

		List<Object> values = new ArrayList<>();
		for (Entry<byte[], Point> entry : memberCoordinateMap.entrySet()) {

			values.add(entry.getValue().getX());
			values.add(entry.getValue().getY());
			values.add(entry.getKey());
		}

		return geoAdd(key, values);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisGeoCommands#geoAdd(byte[], java.lang.Iterable)
	 */
	@Override
	public Long geoAdd(byte[] key, Iterable<GeoLocation<byte[]>> locations) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(locations, "Locations must not be null!");

		List<Object> values = new ArrayList<>();
		for (GeoLocation<byte[]> location : locations) {

			values.add(location.getPoint().getX());
			values.add(location.getPoint().getY());
			values.add(location.getName());
		}

		return geoAdd(key, values);
	}

	@Nullable
	private Long geoAdd(byte[] key, Collection<Object> values) {

		try {

			if (isPipelined()) {
				pipeline(connection.newLettuceResult(getAsyncConnection().geoadd(key, values.toArray())));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceResult(getAsyncConnection().geoadd(key, values.toArray())));
				return null;
			}
			return getConnection().geoadd(key, values.toArray());
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisGeoCommands#geoDist(byte[], byte[], byte[])
	 */
	@Override
	public Distance geoDist(byte[] key, byte[] member1, byte[] member2) {
		return geoDist(key, member1, member2, DistanceUnit.METERS);
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

		GeoArgs.Unit geoUnit = LettuceConverters.toGeoArgsUnit(metric);
		Converter<Double, Distance> distanceConverter = LettuceConverters.distanceConverterForMetric(metric);

		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceResult(getAsyncConnection().geodist(key, member1, member2, geoUnit),
						distanceConverter));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceResult(getAsyncConnection().geodist(key, member1, member2, geoUnit),
						distanceConverter));
				return null;
			}
			return distanceConverter.convert(getConnection().geodist(key, member1, member2, geoUnit));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
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
				pipeline(connection.newLettuceResult(getAsyncConnection().geohash(key, members)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceResult(getAsyncConnection().geohash(key, members)));
				return null;
			}
			return getConnection().geohash(key, members).stream().map(value -> value.getValueOrElse(null))
					.collect(Collectors.toList());
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
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

		ListConverter<GeoCoordinates, Point> converter = LettuceConverters.geoCoordinatesToPointConverter();

		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceResult(getAsyncConnection().geopos(key, members), converter));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceResult(getAsyncConnection().geopos(key, members), converter));
				return null;
			}
			return converter.convert(getConnection().geopos(key, members));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
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

		Converter<Set<byte[]>, GeoResults<GeoLocation<byte[]>>> geoResultsConverter = LettuceConverters
				.bytesSetToGeoResultsConverter();

		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceResult(
						getAsyncConnection().georadius(key, within.getCenter().getX(), within.getCenter().getY(),
								within.getRadius().getValue(), LettuceConverters.toGeoArgsUnit(within.getRadius().getMetric())),
						geoResultsConverter));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceResult(
						getAsyncConnection().georadius(key, within.getCenter().getX(), within.getCenter().getY(),
								within.getRadius().getValue(), LettuceConverters.toGeoArgsUnit(within.getRadius().getMetric())),
						geoResultsConverter));
				return null;
			}
			return geoResultsConverter
					.convert(getConnection().georadius(key, within.getCenter().getX(), within.getCenter().getY(),
							within.getRadius().getValue(), LettuceConverters.toGeoArgsUnit(within.getRadius().getMetric())));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
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

		GeoArgs geoArgs = LettuceConverters.toGeoArgs(args);
		Converter<List<GeoWithin<byte[]>>, GeoResults<GeoLocation<byte[]>>> geoResultsConverter = LettuceConverters
				.geoRadiusResponseToGeoResultsConverter(within.getRadius().getMetric());

		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceResult(getAsyncConnection().georadius(key, within.getCenter().getX(),
						within.getCenter().getY(), within.getRadius().getValue(),
						LettuceConverters.toGeoArgsUnit(within.getRadius().getMetric()), geoArgs), geoResultsConverter));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceResult(getAsyncConnection().georadius(key, within.getCenter().getX(),
						within.getCenter().getY(), within.getRadius().getValue(),
						LettuceConverters.toGeoArgsUnit(within.getRadius().getMetric()), geoArgs), geoResultsConverter));
				return null;
			}
			return geoResultsConverter
					.convert(getConnection().georadius(key, within.getCenter().getX(), within.getCenter().getY(),
							within.getRadius().getValue(), LettuceConverters.toGeoArgsUnit(within.getRadius().getMetric()), geoArgs));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisGeoCommands#geoRadiusByMember(byte[], byte[], double)
	 */
	@Override
	public GeoResults<GeoLocation<byte[]>> geoRadiusByMember(byte[] key, byte[] member, double radius) {
		return geoRadiusByMember(key, member, new Distance(radius, DistanceUnit.METERS));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisGeoCommands#geoRadiusByMember(byte[], byte[], double, org.springframework.data.geo.Metric)
	 */
	@Override
	public GeoResults<GeoLocation<byte[]>> geoRadiusByMember(byte[] key, byte[] member, Distance radius) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(member, "Member must not be null!");
		Assert.notNull(radius, "Radius must not be null!");

		GeoArgs.Unit geoUnit = LettuceConverters.toGeoArgsUnit(radius.getMetric());
		Converter<Set<byte[]>, GeoResults<GeoLocation<byte[]>>> converter = LettuceConverters
				.bytesSetToGeoResultsConverter();

		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceResult(
						getAsyncConnection().georadiusbymember(key, member, radius.getValue(), geoUnit), converter));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceResult(
						getAsyncConnection().georadiusbymember(key, member, radius.getValue(), geoUnit), converter));
				return null;
			}
			return converter.convert(getConnection().georadiusbymember(key, member, radius.getValue(), geoUnit));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisGeoCommands#geoRadiusByMember(byte[], byte[], org.springframework.data.geo.Distance, org.springframework.data.redis.core.GeoRadiusCommandArgs)
	 */
	@Override
	public GeoResults<GeoLocation<byte[]>> geoRadiusByMember(byte[] key, byte[] member, Distance radius,
			GeoRadiusCommandArgs args) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(member, "Member must not be null!");
		Assert.notNull(radius, "Radius must not be null!");
		Assert.notNull(args, "Args must not be null!");

		GeoArgs.Unit geoUnit = LettuceConverters.toGeoArgsUnit(radius.getMetric());
		GeoArgs geoArgs = LettuceConverters.toGeoArgs(args);
		Converter<List<GeoWithin<byte[]>>, GeoResults<GeoLocation<byte[]>>> geoResultsConverter = LettuceConverters
				.geoRadiusResponseToGeoResultsConverter(radius.getMetric());

		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceResult(
						getAsyncConnection().georadiusbymember(key, member, radius.getValue(), geoUnit, geoArgs),
						geoResultsConverter));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceResult(
						getAsyncConnection().georadiusbymember(key, member, radius.getValue(), geoUnit, geoArgs),
						geoResultsConverter));
				return null;
			}
			return geoResultsConverter
					.convert(getConnection().georadiusbymember(key, member, radius.getValue(), geoUnit, geoArgs));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisGeoCommands#geoRemove(byte[], byte[][])
	 */
	@Override
	public Long geoRemove(byte[] key, byte[]... values) {
		return connection.zSetCommands().zRem(key, values);
	}

	private boolean isPipelined() {
		return connection.isPipelined();
	}

	private boolean isQueueing() {
		return connection.isQueueing();
	}

	private void pipeline(LettuceResult result) {
		connection.pipeline(result);
	}

	private void transaction(LettuceResult result) {
		connection.transaction(result);
	}

	private RedisClusterAsyncCommands<byte[], byte[]> getAsyncConnection() {
		return connection.getAsyncConnection();
	}

	public RedisClusterCommands<byte[], byte[]> getConnection() {
		return connection.getConnection();
	}

	private DataAccessException convertLettuceAccessException(Exception ex) {
		return connection.convertLettuceAccessException(ex);
	}
}
