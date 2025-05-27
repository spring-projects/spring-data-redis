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

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullUnmarked;
import org.jspecify.annotations.Nullable;
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
class LettuceGeoCommands implements RedisGeoCommands {

	private final LettuceConnection connection;

	LettuceGeoCommands(@NonNull LettuceConnection connection) {
		this.connection = connection;
	}

	@Override
	public Long geoAdd(byte @NonNull [] key, @NonNull Point point, byte @NonNull [] member) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(point, "Point must not be null");
		Assert.notNull(member, "Member must not be null");

		return connection.invoke().just(RedisGeoAsyncCommands::geoadd, key, point.getX(), point.getY(), member);
	}

	@Override
	public Long geoAdd(byte @NonNull [] key, @NonNull Map<byte @NonNull [], @NonNull Point> memberCoordinateMap) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(memberCoordinateMap, "MemberCoordinateMap must not be null");

		List<Object> values = new ArrayList<>();
		for (Entry<byte[], Point> entry : memberCoordinateMap.entrySet()) {

			values.add(entry.getValue().getX());
			values.add(entry.getValue().getY());
			values.add(entry.getKey());
		}

		return geoAdd(key, values);
	}

	@Override
	public Long geoAdd(byte @NonNull [] key, @NonNull Iterable<@NonNull GeoLocation<byte[]>> locations) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(locations, "Locations must not be null");

		List<Object> values = new ArrayList<>();
		for (GeoLocation<byte[]> location : locations) {

			values.add(location.getPoint().getX());
			values.add(location.getPoint().getY());
			values.add(location.getName());
		}

		return geoAdd(key, values);
	}

	private @Nullable Long geoAdd(byte @NonNull [] key, @NonNull Collection<@NonNull Object> values) {
		return connection.invoke().just(it -> it.geoadd(key, values.toArray()));
	}

	@Override
	public Distance geoDist(byte @NonNull [] key, byte @NonNull [] member1, byte @NonNull [] member2) {
		return geoDist(key, member1, member2, DistanceUnit.METERS);
	}

	@Override
	public Distance geoDist(byte @NonNull [] key, byte @NonNull [] member1, byte @NonNull [] member2,
			@NonNull Metric metric) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(member1, "Member1 must not be null");
		Assert.notNull(member2, "Member2 must not be null");
		Assert.notNull(metric, "Metric must not be null");

		GeoArgs.Unit geoUnit = LettuceConverters.toGeoArgsUnit(metric);
		Converter<Double, Distance> distanceConverter = LettuceConverters.distanceConverterForMetric(metric);

		return connection.invoke().from(RedisGeoAsyncCommands::geodist, key, member1, member2, geoUnit)
				.get(distanceConverter);
	}

	@Override
	public List<String> geoHash(byte @NonNull [] key, byte @NonNull [] @NonNull... members) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(members, "Members must not be null");
		Assert.noNullElements(members, "Members must not contain null");

		return connection.invoke().fromMany(RedisGeoAsyncCommands::geohash, key, members)
				.toList(it -> it.getValueOrElse(null));
	}

	@Override
	public List<Point> geoPos(byte @NonNull [] key, byte @NonNull [] @NonNull... members) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(members, "Members must not be null");
		Assert.noNullElements(members, "Members must not contain null");

		return connection.invoke().fromMany(RedisGeoAsyncCommands::geopos, key, members)
				.toList(LettuceConverters::geoCoordinatesToPoint);
	}

	@Override
	public GeoResults<GeoLocation<byte[]>> geoRadius(byte @NonNull [] key, @NonNull Circle within) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(within, "Within must not be null");

		Converter<Set<byte[]>, GeoResults<GeoLocation<byte[]>>> geoResultsConverter = LettuceConverters
				.bytesSetToGeoResultsConverter();

		return connection.invoke()
				.from(it -> it.georadius(key, within.getCenter().getX(), within.getCenter().getY(),
						within.getRadius().getValue(), LettuceConverters.toGeoArgsUnit(within.getRadius().getMetric())))
				.get(geoResultsConverter);
	}

	@Override
	public GeoResults<GeoLocation<byte[]>> geoRadius(byte @NonNull [] key, @NonNull Circle within,
			@NonNull GeoRadiusCommandArgs args) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(within, "Within must not be null");
		Assert.notNull(args, "Args must not be null");

		GeoArgs geoArgs = LettuceConverters.toGeoArgs(args);
		Converter<List<GeoWithin<byte[]>>, GeoResults<GeoLocation<byte[]>>> geoResultsConverter = LettuceConverters
				.geoRadiusResponseToGeoResultsConverter(within.getRadius().getMetric());

		return connection.invoke()
				.from(it -> it.georadius(key, within.getCenter().getX(), within.getCenter().getY(),
						within.getRadius().getValue(), LettuceConverters.toGeoArgsUnit(within.getRadius().getMetric()), geoArgs))
				.get(geoResultsConverter);
	}

	@Override
	public GeoResults<GeoLocation<byte[]>> geoRadiusByMember(byte @NonNull [] key, byte @NonNull [] member,
			double radius) {
		return geoRadiusByMember(key, member, new Distance(radius, DistanceUnit.METERS));
	}

	@Override
	public GeoResults<GeoLocation<byte[]>> geoRadiusByMember(byte @NonNull [] key, byte @NonNull [] member,
			@NonNull Distance radius) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(member, "Member must not be null");
		Assert.notNull(radius, "Radius must not be null");

		GeoArgs.Unit geoUnit = LettuceConverters.toGeoArgsUnit(radius.getMetric());
		Converter<Set<byte[]>, GeoResults<GeoLocation<byte[]>>> converter = LettuceConverters
				.bytesSetToGeoResultsConverter();

		return connection.invoke().from(RedisGeoAsyncCommands::georadiusbymember, key, member, radius.getValue(), geoUnit)
				.get(converter);
	}

	@Override
	public GeoResults<GeoLocation<byte[]>> geoRadiusByMember(byte @NonNull [] key, byte @NonNull [] member,
			@NonNull Distance radius, @NonNull GeoRadiusCommandArgs args) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(member, "Member must not be null");
		Assert.notNull(radius, "Radius must not be null");
		Assert.notNull(args, "Args must not be null");

		GeoArgs.Unit geoUnit = LettuceConverters.toGeoArgsUnit(radius.getMetric());
		GeoArgs geoArgs = LettuceConverters.toGeoArgs(args);
		Converter<List<GeoWithin<byte[]>>, GeoResults<GeoLocation<byte[]>>> geoResultsConverter = LettuceConverters
				.geoRadiusResponseToGeoResultsConverter(radius.getMetric());

		return connection.invoke()
				.from(RedisGeoAsyncCommands::georadiusbymember, key, member, radius.getValue(), geoUnit, geoArgs)
				.get(geoResultsConverter);
	}

	@Override
	public Long geoRemove(byte @NonNull [] key, byte @NonNull [] @NonNull... values) {
		return connection.zSetCommands().zRem(key, values);
	}

	@Override
	public GeoResults<GeoLocation<byte[]>> geoSearch(byte @NonNull [] key, @NonNull GeoReference<byte[]> reference,
			@NonNull GeoShape predicate, @NonNull GeoSearchCommandArgs args) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(reference, "Reference must not be null");
		Assert.notNull(predicate, "GeoPredicate must not be null");
		Assert.notNull(args, "GeoSearchCommandArgs must not be null");

		GeoSearch.GeoRef<byte[]> ref = LettuceConverters.toGeoRef(reference);
		GeoSearch.GeoPredicate lettucePredicate = LettuceConverters.toGeoPredicate(predicate);
		GeoArgs geoArgs = LettuceConverters.toGeoArgs(args);

		return connection.invoke().from(RedisGeoAsyncCommands::geosearch, key, ref, lettucePredicate, geoArgs)
				.get(LettuceConverters.geoRadiusResponseToGeoResultsConverter(predicate.getMetric()));
	}

	@Override
	public Long geoSearchStore(byte @NonNull [] destKey, byte @NonNull [] key, @NonNull GeoReference<byte[]> reference,
			@NonNull GeoShape predicate, @NonNull GeoSearchStoreCommandArgs args) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(reference, "Reference must not be null");
		Assert.notNull(predicate, "GeoPredicate must not be null");
		Assert.notNull(args, "GeoSearchCommandArgs must not be null");

		GeoSearch.GeoRef<byte[]> ref = LettuceConverters.toGeoRef(reference);
		GeoSearch.GeoPredicate lettucePredicate = LettuceConverters.toGeoPredicate(predicate);
		GeoArgs geoArgs = LettuceConverters.toGeoArgs(args);

		return connection.invoke().just(
				connection -> connection.geosearchstore(destKey, key, ref, lettucePredicate, geoArgs, args.isStoreDistance()));
	}

}
