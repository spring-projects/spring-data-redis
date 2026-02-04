/*
 * Copyright 2026-present the original author or authors.
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

import java.util.ArrayList;
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

import redis.clients.jedis.GeoCoordinate;
import redis.clients.jedis.args.GeoUnit;
import redis.clients.jedis.params.GeoRadiusParam;
import redis.clients.jedis.params.GeoSearchParam;
import redis.clients.jedis.resps.GeoRadiusResponse;

import static org.springframework.data.redis.connection.convert.Converters.distanceConverterForMetric;
import static org.springframework.data.redis.connection.jedis.JedisConverters.*;

/**
 * @author Tihomir Mateev
 * @since 4.1
 */
@NullUnmarked
class JedisClientGeoCommands implements RedisGeoCommands {

	private final JedisClientConnection connection;

	JedisClientGeoCommands(JedisClientConnection connection) {
		this.connection = connection;
	}

	@Override
	public Long geoAdd(byte @NonNull [] key, @NonNull Point point, byte @NonNull [] member) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(point, "Point must not be null");
		Assert.notNull(member, "Member must not be null");

		return connection.execute(client -> client.geoadd(key, point.getX(), point.getY(), member),
				pipeline -> pipeline.geoadd(key, point.getX(), point.getY(), member));
	}

	@Override
	public Long geoAdd(byte @NonNull [] key, @NonNull Map<byte @NonNull [], @NonNull Point> memberCoordinateMap) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(memberCoordinateMap, "MemberCoordinateMap must not be null");

		Map<byte[], GeoCoordinate> redisGeoCoordinateMap = new HashMap<>();

		for (byte[] mapKey : memberCoordinateMap.keySet()) {
			redisGeoCoordinateMap.put(mapKey, toGeoCoordinate(memberCoordinateMap.get(mapKey)));
		}

		return connection.execute(client -> client.geoadd(key, redisGeoCoordinateMap),
				pipeline -> pipeline.geoadd(key, redisGeoCoordinateMap));
	}

	@Override
	public Long geoAdd(byte @NonNull [] key, @NonNull Iterable<@NonNull GeoLocation<byte[]>> locations) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(locations, "Locations must not be null");

		Map<byte[], GeoCoordinate> redisGeoCoordinateMap = new HashMap<>();

		for (GeoLocation<byte[]> location : locations) {
			redisGeoCoordinateMap.put(location.getName(), toGeoCoordinate(location.getPoint()));
		}

		return connection.execute(client -> client.geoadd(key, redisGeoCoordinateMap),
				pipeline -> pipeline.geoadd(key, redisGeoCoordinateMap));
	}

	@Override
	public Distance geoDist(byte @NonNull [] key, byte @NonNull [] member1, byte @NonNull [] member2) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(member1, "Member1 must not be null");
		Assert.notNull(member2, "Member2 must not be null");

		Converter<@NonNull Double, Distance> distanceConverter = distanceConverterForMetric(DistanceUnit.METERS);

		return connection.execute(client -> client.geodist(key, member1, member2),
				pipeline -> pipeline.geodist(key, member1, member2), distanceConverter);
	}

	@Override
	public Distance geoDist(byte @NonNull [] key, byte @NonNull [] member1, byte @NonNull [] member2,
			@NonNull Metric metric) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(member1, "Member1 must not be null");
		Assert.notNull(member2, "Member2 must not be null");
		Assert.notNull(metric, "Metric must not be null");

		GeoUnit geoUnit = toGeoUnit(metric);
		Converter<@NonNull Double, Distance> distanceConverter = distanceConverterForMetric(metric);

		return connection.execute(client -> client.geodist(key, member1, member2, geoUnit),
				pipeline -> pipeline.geodist(key, member1, member2, geoUnit), distanceConverter);
	}

	@Override
	public List<String> geoHash(byte @NonNull [] key, byte @NonNull [] @NonNull... members) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(members, "Members must not be null");
		Assert.noNullElements(members, "Members must not contain null");

		return connection.execute(client -> client.geohash(key, members), pipeline -> pipeline.geohash(key, members),
				JedisConverters::toStrings);
	}

	@Override
	public List<@NonNull Point> geoPos(byte @NonNull [] key, byte @NonNull [] @NonNull... members) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(members, "Members must not be null");
		Assert.noNullElements(members, "Members must not contain null");

		return connection.execute(client -> client.geopos(key, members), pipeline -> pipeline.geopos(key, members),
				result -> {
					List<Point> points = new ArrayList<>(result.size());
					for (GeoCoordinate cord : result) {
						points.add(JedisConverters.toPoint(cord));
					}
					return points;
				});
	}

	@Override
	public GeoResults<@NonNull GeoLocation<byte[]>> geoRadius(byte @NonNull [] key, @NonNull Circle within) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(within, "Within must not be null");

		Converter<@NonNull List<GeoRadiusResponse>, GeoResults<@NonNull GeoLocation<byte[]>>> converter = geoRadiusResponseToGeoResultsConverter(
				within.getRadius().getMetric());

		return connection.execute(
				client -> client.georadius(key, within.getCenter().getX(), within.getCenter().getY(),
						within.getRadius().getValue(), toGeoUnit(within.getRadius().getMetric())),
				pipeline -> pipeline.georadius(key, within.getCenter().getX(), within.getCenter().getY(),
						within.getRadius().getValue(), toGeoUnit(within.getRadius().getMetric())),
				converter);
	}

	@Override
	public GeoResults<@NonNull GeoLocation<byte[]>> geoRadius(byte @NonNull [] key, @NonNull Circle within,
			@NonNull GeoRadiusCommandArgs args) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(within, "Within must not be null");
		Assert.notNull(args, "Args must not be null");

		GeoRadiusParam geoRadiusParam = toGeoRadiusParam(args);
		Converter<@NonNull List<GeoRadiusResponse>, GeoResults<@NonNull GeoLocation<byte[]>>> converter = geoRadiusResponseToGeoResultsConverter(
				within.getRadius().getMetric());

		return connection.execute(
				client -> client.georadius(key, within.getCenter().getX(), within.getCenter().getY(),
						within.getRadius().getValue(), toGeoUnit(within.getRadius().getMetric()), geoRadiusParam),
				pipeline -> pipeline.georadius(key, within.getCenter().getX(), within.getCenter().getY(),
						within.getRadius().getValue(), toGeoUnit(within.getRadius().getMetric()), geoRadiusParam),
				converter);
	}

	@Override
	public GeoResults<@NonNull GeoLocation<byte[]>> geoRadiusByMember(byte @NonNull [] key, byte @NonNull [] member,
			@NonNull Distance radius) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(member, "Member must not be null");
		Assert.notNull(radius, "Radius must not be null");

		GeoUnit geoUnit = toGeoUnit(radius.getMetric());
		Converter<@NonNull List<GeoRadiusResponse>, GeoResults<@NonNull GeoLocation<byte[]>>> converter = geoRadiusResponseToGeoResultsConverter(
				radius.getMetric());

		return connection.execute(client -> client.georadiusByMember(key, member, radius.getValue(), geoUnit),
				pipeline -> pipeline.georadiusByMember(key, member, radius.getValue(), geoUnit), converter);
	}

	@Override
	public GeoResults<@NonNull GeoLocation<byte[]>> geoRadiusByMember(byte @NonNull [] key, byte @NonNull [] member,
			@NonNull Distance radius, @NonNull GeoRadiusCommandArgs args) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(member, "Member must not be null");
		Assert.notNull(radius, "Radius must not be null");
		Assert.notNull(args, "Args must not be null");

		GeoUnit geoUnit = toGeoUnit(radius.getMetric());
		Converter<@NonNull List<GeoRadiusResponse>, GeoResults<@NonNull GeoLocation<byte[]>>> converter = geoRadiusResponseToGeoResultsConverter(
				radius.getMetric());
		GeoRadiusParam geoRadiusParam = toGeoRadiusParam(args);

		return connection.execute(
				client -> client.georadiusByMember(key, member, radius.getValue(), geoUnit, geoRadiusParam),
				pipeline -> pipeline.georadiusByMember(key, member, radius.getValue(), geoUnit, geoRadiusParam), converter);
	}

	@Override
	public Long geoRemove(byte @NonNull [] key, byte @NonNull [] @NonNull... members) {
		return connection.zSetCommands().zRem(key, members);
	}

	@Override
	public GeoResults<@NonNull GeoLocation<byte[]>> geoSearch(byte @NonNull [] key,
			@NonNull GeoReference<byte[]> reference, @NonNull GeoShape predicate, @NonNull GeoSearchCommandArgs args) {

		Assert.notNull(key, "Key must not be null");

		GeoSearchParam param = toGeoSearchParams(reference, predicate, args);
		Converter<@NonNull List<GeoRadiusResponse>, GeoResults<@NonNull GeoLocation<byte[]>>> converter = geoRadiusResponseToGeoResultsConverter(
				predicate.getMetric());

		return connection.execute(client -> client.geosearch(key, param), pipeline -> pipeline.geosearch(key, param),
				converter);
	}

	@Override
	public Long geoSearchStore(byte @NonNull [] destKey, byte @NonNull [] key, @NonNull GeoReference<byte[]> reference,
			@NonNull GeoShape predicate, @NonNull GeoSearchStoreCommandArgs args) {

		Assert.notNull(destKey, "Destination Key must not be null");
		Assert.notNull(key, "Key must not be null");

		GeoSearchParam param = toGeoSearchParams(reference, predicate, args);

		if (args.isStoreDistance()) {
			return connection.execute(client -> client.geosearchStoreStoreDist(destKey, key, param),
					pipeline -> pipeline.geosearchStoreStoreDist(destKey, key, param));
		}

		return connection.execute(client -> client.geosearchStore(destKey, key, param),
				pipeline -> pipeline.geosearchStore(destKey, key, param));
	}
}
