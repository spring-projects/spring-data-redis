/*
 * Copyright 2017-2023 the original author or authors.
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
package org.springframework.data.redis.connection.lettuce;

import io.lettuce.core.GeoArgs;
import io.lettuce.core.GeoSearch;
import io.lettuce.core.GeoWithin;
import io.lettuce.core.api.async.RedisGeoAsyncCommands;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.geo.Circle;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.geo.Metric;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.data.redis.domain.geo.GeoShape;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.0
 */
class LettuceGeoCommands implements RedisGeoCommands {

	private final LettuceConnection connection;

	LettuceGeoCommands(LettuceConnection connection) {
		this.connection = connection;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisGeoCommands#geoAdd(byte[], org.springframework.data.geo.Point, byte[])
	 */
	@Override
	public Long geoAdd(byte[] key, Point point, byte[] member) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(point, "Point must not be null!");
		Assert.notNull(member, "Member must not be null!");

		return connection.invoke().just(RedisGeoAsyncCommands::geoadd, key, point.getX(), point.getY(), member);
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
		return connection.invoke().just(it -> it.geoadd(key, values.toArray()));
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

		return connection.invoke().from(RedisGeoAsyncCommands::geodist, key, member1, member2, geoUnit)
				.get(distanceConverter);
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

		return connection.invoke().fromMany(RedisGeoAsyncCommands::geohash, key, members)
				.toList(it -> it.getValueOrElse(null));
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

		return connection.invoke().fromMany(RedisGeoAsyncCommands::geopos, key, members)
				.toList(LettuceConverters::geoCoordinatesToPoint);
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

		return connection.invoke()
				.from(it -> it.georadius(key, within.getCenter().getX(), within.getCenter().getY(),
						within.getRadius().getValue(), LettuceConverters.toGeoArgsUnit(within.getRadius().getMetric())))
				.get(geoResultsConverter);
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

		return connection.invoke()
				.from(it -> it.georadius(key, within.getCenter().getX(), within.getCenter().getY(),
						within.getRadius().getValue(), LettuceConverters.toGeoArgsUnit(within.getRadius().getMetric()), geoArgs))
				.get(geoResultsConverter);
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

		return connection.invoke().from(RedisGeoAsyncCommands::georadiusbymember, key, member, radius.getValue(), geoUnit)
				.get(converter);
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

		return connection.invoke()
				.from(RedisGeoAsyncCommands::georadiusbymember, key, member, radius.getValue(), geoUnit, geoArgs)
				.get(geoResultsConverter);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisGeoCommands#geoRemove(byte[], byte[][])
	 */
	@Override
	public Long geoRemove(byte[] key, byte[]... values) {
		return connection.zSetCommands().zRem(key, values);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisGeoCommands#geoSearch(byte[], org.springframework.data.redis.connection.RedisGeoCommands.GeoReference, org.springframework.data.redis.connection.RedisGeoCommands.GeoShape, org.springframework.data.redis.connection.RedisGeoCommands.GeoSearchCommandArgs)
	 */
	@Override
	public GeoResults<GeoLocation<byte[]>> geoSearch(byte[] key, GeoReference<byte[]> reference, GeoShape predicate,
			GeoSearchCommandArgs args) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(reference, "Reference must not be null!");
		Assert.notNull(predicate, "GeoPredicate must not be null!");
		Assert.notNull(args, "GeoSearchCommandArgs must not be null!");

		GeoSearch.GeoRef<byte[]> ref = LettuceConverters.toGeoRef(reference);
		GeoSearch.GeoPredicate lettucePredicate = LettuceConverters.toGeoPredicate(predicate);
		GeoArgs geoArgs = LettuceConverters.toGeoArgs(args);

		return connection.invoke().from(RedisGeoAsyncCommands::geosearch, key, ref, lettucePredicate, geoArgs)
				.get(LettuceConverters.geoRadiusResponseToGeoResultsConverter(predicate.getMetric()));
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisGeoCommands#geoSearchStore(byte[], byte[], org.springframework.data.redis.connection.RedisGeoCommands.GeoReference, org.springframework.data.redis.connection.RedisGeoCommands.GeoShape, org.springframework.data.redis.connection.RedisGeoCommands.GeoSearchStoreCommandArgs)
	 */
	@Override
	public Long geoSearchStore(byte[] destKey, byte[] key, GeoReference<byte[]> reference, GeoShape predicate,
			GeoSearchStoreCommandArgs args) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(reference, "Reference must not be null!");
		Assert.notNull(predicate, "GeoPredicate must not be null!");
		Assert.notNull(args, "GeoSearchCommandArgs must not be null!");

		GeoSearch.GeoRef<byte[]> ref = LettuceConverters.toGeoRef(reference);
		GeoSearch.GeoPredicate lettucePredicate = LettuceConverters.toGeoPredicate(predicate);
		GeoArgs geoArgs = LettuceConverters.toGeoArgs(args);

		return connection.invoke().just(
				connection -> connection.geosearchstore(destKey, key, ref, lettucePredicate, geoArgs, args.isStoreDistance()));
	}

}
