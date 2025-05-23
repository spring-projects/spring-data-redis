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
import redis.clients.jedis.Jedis;
import redis.clients.jedis.args.GeoUnit;
import redis.clients.jedis.commands.PipelineBinaryCommands;
import redis.clients.jedis.params.GeoSearchParam;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullUnmarked;
import org.springframework.core.convert.converter.Converter;
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
class JedisGeoCommands implements RedisGeoCommands {

	private final JedisConnection connection;

	JedisGeoCommands(JedisConnection connection) {
		this.connection = connection;
	}

	@Override
	public Long geoAdd(byte @NonNull [] key, @NonNull Point point, byte @NonNull [] member) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(point, "Point must not be null");
		Assert.notNull(member, "Member must not be null");

		return connection.invoke().just(Jedis::geoadd, PipelineBinaryCommands::geoadd, key, point.getX(), point.getY(),
				member);
	}

	@Override
	public Long geoAdd(byte @NonNull [] key, @NonNull Map<byte @NonNull [], @NonNull Point> memberCoordinateMap) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(memberCoordinateMap, "MemberCoordinateMap must not be null");

		Map<byte[], GeoCoordinate> redisGeoCoordinateMap = new HashMap<>();

		for (byte[] mapKey : memberCoordinateMap.keySet()) {
			redisGeoCoordinateMap.put(mapKey, JedisConverters.toGeoCoordinate(memberCoordinateMap.get(mapKey)));
		}

		return connection.invoke().just(Jedis::geoadd, PipelineBinaryCommands::geoadd, key, redisGeoCoordinateMap);
	}

	@Override
	public Long geoAdd(byte @NonNull [] key, @NonNull Iterable<@NonNull GeoLocation<byte[]>> locations) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(locations, "Locations must not be null");

		Map<byte[], redis.clients.jedis.GeoCoordinate> redisGeoCoordinateMap = new HashMap<>();

		for (GeoLocation<byte[]> location : locations) {
			redisGeoCoordinateMap.put(location.getName(), JedisConverters.toGeoCoordinate(location.getPoint()));
		}

		return connection.invoke().just(Jedis::geoadd, PipelineBinaryCommands::geoadd, key, redisGeoCoordinateMap);
	}

	@Override
	public Distance geoDist(byte @NonNull [] key, byte @NonNull [] member1, byte @NonNull [] member2) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(member1, "Member1 must not be null");
		Assert.notNull(member2, "Member2 must not be null");

		Converter<Double, Distance> distanceConverter = JedisConverters.distanceConverterForMetric(DistanceUnit.METERS);

		return connection.invoke().from(Jedis::geodist, PipelineBinaryCommands::geodist, key, member1, member2)
				.get(distanceConverter);
	}

	@Override
	public Distance geoDist(byte @NonNull [] key, byte @NonNull [] member1, byte @NonNull [] member2,
			@NonNull Metric metric) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(member1, "Member1 must not be null");
		Assert.notNull(member2, "Member2 must not be null");
		Assert.notNull(metric, "Metric must not be null");

		GeoUnit geoUnit = JedisConverters.toGeoUnit(metric);
		Converter<Double, Distance> distanceConverter = JedisConverters.distanceConverterForMetric(metric);

		return connection.invoke().from(Jedis::geodist, PipelineBinaryCommands::geodist, key, member1, member2, geoUnit)
				.get(distanceConverter);
	}

	@Override
	public List<String> geoHash(byte @NonNull [] key, byte @NonNull [] @NonNull... members) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(members, "Members must not be null");
		Assert.noNullElements(members, "Members must not contain null");

		return connection.invoke().fromMany(Jedis::geohash, PipelineBinaryCommands::geohash, key, members)
				.toList(JedisConverters::toString);
	}

	@Override
	public List<@NonNull Point> geoPos(byte @NonNull [] key, byte @NonNull [] @NonNull... members) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(members, "Members must not be null");
		Assert.noNullElements(members, "Members must not contain null");

		return connection.invoke().fromMany(Jedis::geopos, PipelineBinaryCommands::geopos, key, members)
				.toList(JedisConverters::toPoint);
	}

	@Override
	public GeoResults<GeoLocation<byte[]>> geoRadius(byte @NonNull [] key, @NonNull Circle within) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(within, "Within must not be null");

		Converter<List<redis.clients.jedis.resps.GeoRadiusResponse>, GeoResults<GeoLocation<byte[]>>> converter = JedisConverters
				.geoRadiusResponseToGeoResultsConverter(within.getRadius().getMetric());

		return connection.invoke()
				.from(Jedis::georadius, PipelineBinaryCommands::georadius, key, within.getCenter().getX(),
						within.getCenter().getY(), within.getRadius().getValue(),
						JedisConverters.toGeoUnit(within.getRadius().getMetric()))
				.get(converter);
	}

	@Override
	public GeoResults<GeoLocation<byte[]>> geoRadius(byte @NonNull [] key, @NonNull Circle within,
			@NonNull GeoRadiusCommandArgs args) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(within, "Within must not be null");
		Assert.notNull(args, "Args must not be null");

		redis.clients.jedis.params.GeoRadiusParam geoRadiusParam = JedisConverters.toGeoRadiusParam(args);
		Converter<List<redis.clients.jedis.resps.GeoRadiusResponse>, GeoResults<GeoLocation<byte[]>>> converter = JedisConverters
				.geoRadiusResponseToGeoResultsConverter(within.getRadius().getMetric());

		return connection.invoke()
				.from(Jedis::georadius, PipelineBinaryCommands::georadius, key, within.getCenter().getX(),
						within.getCenter().getY(), within.getRadius().getValue(),
						JedisConverters.toGeoUnit(within.getRadius().getMetric()), geoRadiusParam)
				.get(converter);
	}

	@Override
	public GeoResults<GeoLocation<byte[]>> geoRadiusByMember(byte @NonNull [] key, byte @NonNull [] member,
			@NonNull Distance radius) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(member, "Member must not be null");
		Assert.notNull(radius, "Radius must not be null");

		GeoUnit geoUnit = JedisConverters.toGeoUnit(radius.getMetric());
		Converter<List<redis.clients.jedis.resps.GeoRadiusResponse>, GeoResults<GeoLocation<byte[]>>> converter = JedisConverters
				.geoRadiusResponseToGeoResultsConverter(radius.getMetric());

		return connection.invoke().from(Jedis::georadiusByMember, PipelineBinaryCommands::georadiusByMember, key, member,
				radius.getValue(), geoUnit).get(converter);
	}

	@Override
	public GeoResults<GeoLocation<byte[]>> geoRadiusByMember(byte @NonNull [] key, byte @NonNull [] member,
			@NonNull Distance radius, @NonNull GeoRadiusCommandArgs args) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(member, "Member must not be null");
		Assert.notNull(radius, "Radius must not be null");
		Assert.notNull(args, "Args must not be null");

		GeoUnit geoUnit = JedisConverters.toGeoUnit(radius.getMetric());
		Converter<List<redis.clients.jedis.resps.GeoRadiusResponse>, GeoResults<GeoLocation<byte[]>>> converter = JedisConverters
				.geoRadiusResponseToGeoResultsConverter(radius.getMetric());
		redis.clients.jedis.params.GeoRadiusParam geoRadiusParam = JedisConverters.toGeoRadiusParam(args);

		return connection.invoke().from(Jedis::georadiusByMember, PipelineBinaryCommands::georadiusByMember, key, member,
				radius.getValue(), geoUnit, geoRadiusParam).get(converter);
	}

	@Override
	public Long geoRemove(byte @NonNull [] key, byte @NonNull [] @NonNull... members) {
		return connection.zSetCommands().zRem(key, members);
	}

	@Override
	public GeoResults<GeoLocation<byte[]>> geoSearch(byte @NonNull [] key, @NonNull GeoReference<byte[]> reference,
			@NonNull GeoShape predicate, @NonNull GeoSearchCommandArgs args) {

		Assert.notNull(key, "Key must not be null");

		GeoSearchParam param = JedisConverters.toGeoSearchParams(reference, predicate, args);
		Converter<List<redis.clients.jedis.resps.GeoRadiusResponse>, GeoResults<GeoLocation<byte[]>>> converter = JedisConverters
				.geoRadiusResponseToGeoResultsConverter(predicate.getMetric());

		return connection.invoke().from(Jedis::geosearch, PipelineBinaryCommands::geosearch, key, param).get(converter);
	}

	@Override
	public Long geoSearchStore(byte @NonNull [] destKey, byte @NonNull [] key, @NonNull GeoReference<byte[]> reference,
			@NonNull GeoShape predicate, @NonNull GeoSearchStoreCommandArgs args) {

		Assert.notNull(destKey, "Destination Key must not be null");
		Assert.notNull(key, "Key must not be null");

		GeoSearchParam param = JedisConverters.toGeoSearchParams(reference, predicate, args);

		if (args.isStoreDistance()) {
			return connection.invoke().just(Jedis::geosearchStoreStoreDist, PipelineBinaryCommands::geosearchStoreStoreDist,
					destKey, key, param);
		}

		return connection.invoke().just(Jedis::geosearchStore, PipelineBinaryCommands::geosearchStore, destKey, key, param);
	}
}
