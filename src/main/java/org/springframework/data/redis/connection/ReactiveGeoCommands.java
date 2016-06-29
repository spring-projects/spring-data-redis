/*
 * Copyright 2016 the original author or authors.
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
package org.springframework.data.redis.connection;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.reactivestreams.Publisher;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.geo.Circle;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.geo.Metric;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.ReactiveRedisConnection.CommandResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.KeyCommand;
import org.springframework.data.redis.connection.ReactiveRedisConnection.MultiValueResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.NumericResponse;
import org.springframework.data.redis.connection.RedisGeoCommands.DistanceUnit;
import org.springframework.data.redis.connection.RedisGeoCommands.GeoLocation;
import org.springframework.data.redis.connection.RedisGeoCommands.GeoRadiusCommandArgs;
import org.springframework.data.redis.connection.RedisGeoCommands.GeoRadiusCommandArgs.Flag;
import org.springframework.util.Assert;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * @author Christoph Strobl
 * @since 2.0
 */
public interface ReactiveGeoCommands {

	/**
	 * @author Christoph Strobl
	 */
	class GeoAddCommand extends KeyCommand {

		private final List<GeoLocation<ByteBuffer>> geoLocations;

		public GeoAddCommand(ByteBuffer key, List<GeoLocation<ByteBuffer>> geoLocations) {

			super(key);
			this.geoLocations = geoLocations;
		}

		public static GeoAddCommand location(GeoLocation<ByteBuffer> geoLocation) {
			return new GeoAddCommand(null, Collections.singletonList(geoLocation));
		}

		public static GeoAddCommand locations(List<GeoLocation<ByteBuffer>> geoLocations) {
			return new GeoAddCommand(null, new ArrayList<>(geoLocations));
		}

		public GeoAddCommand to(ByteBuffer key) {
			return new GeoAddCommand(key, geoLocations);
		}

		public List<GeoLocation<ByteBuffer>> getGeoLocations() {
			return geoLocations;
		}

	}

	/**
	 * Add {@link Point} with given {@literal member} to {@literal key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param point must not be {@literal null}.
	 * @param member must not be {@literal null}.
	 * @return
	 */
	default Mono<Long> geoAdd(ByteBuffer key, Point point, ByteBuffer member) {

		Assert.notNull(key, "key must not be null");
		Assert.notNull(point, "point must not be null");
		Assert.notNull(member, "member must not be null");

		return geoAdd(key, new GeoLocation<>(member, point));
	}

	/**
	 * Add {@link GeoLocation} to {@literal key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param location must not be {@literal null}.
	 * @return
	 */
	default Mono<Long> geoAdd(ByteBuffer key, GeoLocation<ByteBuffer> location) {

		Assert.notNull(key, "key must not be null");
		Assert.notNull(location, "location must not be null");

		return geoAdd(key, Collections.singletonList(location));
	}

	/**
	 * Add {@link GeoLocation} to {@literal key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param locations must not be {@literal null}.
	 * @return
	 */
	default Mono<Long> geoAdd(ByteBuffer key, List<GeoLocation<ByteBuffer>> locations) {

		Assert.notNull(key, "key must not be null");
		Assert.notNull(locations, "locations must not be null");

		return geoAdd(Mono.just(GeoAddCommand.locations(locations).to(key))).next().map(NumericResponse::getOutput);
	}

	/**
	 * Add {@link GeoLocation}s to {@literal key}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 */
	Flux<NumericResponse<GeoAddCommand, Long>> geoAdd(Publisher<GeoAddCommand> commands);

	/**
	 * @author Christoph Strobl
	 */
	class GeoDistCommand extends KeyCommand {

		private final ByteBuffer from;
		private final ByteBuffer to;
		private final Metric metric;

		private GeoDistCommand(ByteBuffer key, ByteBuffer from, ByteBuffer to, Metric metric) {
			super(key);
			this.from = from;
			this.to = to;
			this.metric = metric;
		}

		static GeoDistCommand units(Metric unit) {
			return new GeoDistCommand(null, null, null, unit);
		}

		public static GeoDistCommand meters() {
			return units(DistanceUnit.METERS);
		}

		public static GeoDistCommand kiometers() {
			return units(DistanceUnit.KILOMETERS);
		}

		public static GeoDistCommand miles() {
			return units(DistanceUnit.MILES);
		}

		public static GeoDistCommand feet() {
			return units(DistanceUnit.FEET);
		}

		public GeoDistCommand between(ByteBuffer from) {
			return new GeoDistCommand(getKey(), from, to, metric);
		}

		public GeoDistCommand and(ByteBuffer to) {
			return new GeoDistCommand(getKey(), from, to, metric);
		}

		public GeoDistCommand forKey(ByteBuffer key) {
			return new GeoDistCommand(key, from, to, metric);
		}

		public ByteBuffer getFrom() {
			return from;
		}

		public ByteBuffer getTo() {
			return to;
		}

		public Optional<Metric> getMetric() {
			return Optional.ofNullable(metric);
		}
	}

	/**
	 * Get the {@link Distance} between {@literal from} and {@literal to}.
	 *
	 * @param key must not be {@literal null}.
	 * @param from must not be {@literal null}.
	 * @param to must not be {@literal null}.
	 * @return
	 */
	default Mono<Distance> geoDist(ByteBuffer key, ByteBuffer from, ByteBuffer to) {
		return geoDist(key, from, to, null);
	}

	/**
	 * Get the {@link Distance} between {@literal from} and {@literal to}.
	 *
	 * @param key must not be {@literal null}.
	 * @param from must not be {@literal null}.
	 * @param to must not be {@literal null}.
	 * @param metric can be {@literal null} and defaults to {@link DistanceUnit#METERS}.
	 * @return
	 */
	default Mono<Distance> geoDist(ByteBuffer key, ByteBuffer from, ByteBuffer to, Metric metric) {

		Assert.notNull(key, "key must not be null");
		Assert.notNull(from, "from must not be null");
		Assert.notNull(to, "to must not be null");

		return geoDist(Mono.just(GeoDistCommand.units(metric).between(from).and(to).forKey(key))).next()
				.map(CommandResponse::getOutput);
	}

	/**
	 * Get the {@link Distance} between {@literal from} and {@literal to}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 */
	Flux<CommandResponse<GeoDistCommand, Distance>> geoDist(Publisher<GeoDistCommand> commands);

	/**
	 * @author Christoph Strobl
	 */
	class GeoHashCommand extends KeyCommand {

		private final List<ByteBuffer> members;

		private GeoHashCommand(ByteBuffer key, List<ByteBuffer> members) {

			super(key);
			this.members = members;
		}

		public static GeoHashCommand member(ByteBuffer member) {
			return new GeoHashCommand(null, Collections.singletonList(member));
		}

		public static GeoHashCommand members(List<ByteBuffer> members) {
			return new GeoHashCommand(null, new ArrayList<>(members));
		}

		public GeoHashCommand of(ByteBuffer key) {
			return new GeoHashCommand(key, members);
		}

		public List<ByteBuffer> getMembers() {
			return members;
		}
	}

	/**
	 * Get geohash representation of the position for the one {@literal member}.
	 *
	 * @param key must not be {@literal null}.
	 * @param member must not be {@literal null}.
	 * @return
	 */
	default Mono<String> geoHash(ByteBuffer key, ByteBuffer member) {

		Assert.notNull(member, "member must not be null");

		return geoHash(key, Collections.singletonList(member)).map(vals -> vals.isEmpty() ? null : vals.iterator().next());
	}

	/**
	 * Get geohash representation of the position for one or more {@literal member}s.
	 *
	 * @param key must not be {@literal null}.
	 * @param members must not be {@literal null}.
	 * @return
	 */
	default Mono<List<String>> geoHash(ByteBuffer key, List<ByteBuffer> members) {

		Assert.notNull(key, "key must not be null");
		Assert.notNull(members, "members must not be null");

		return geoHash(Mono.just(GeoHashCommand.members(members).of(key))).next().map(MultiValueResponse::getOutput);
	}

	/**
	 * Get geohash representation of the position for one or more {@literal member}s.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 */
	Flux<MultiValueResponse<GeoHashCommand, String>> geoHash(Publisher<GeoHashCommand> commands);

	/**
	 * @author Christoph Strobl
	 */
	class GeoPosCommand extends KeyCommand {

		private final List<ByteBuffer> members;

		private GeoPosCommand(ByteBuffer key, List<ByteBuffer> members) {

			super(key);
			this.members = members;
		}

		public static GeoPosCommand member(ByteBuffer member) {
			return new GeoPosCommand(null, Collections.singletonList(member));
		}

		public static GeoPosCommand members(List<ByteBuffer> members) {
			return new GeoPosCommand(null, new ArrayList<>(members));
		}

		public GeoPosCommand of(ByteBuffer key) {
			return new GeoPosCommand(key, members);
		}

		public List<ByteBuffer> getMembers() {
			return members;
		}
	}

	/**
	 * Get the {@link Point} representation of positions for the {@literal member}s.
	 *
	 * @param key must not be {@literal null}.
	 * @param member must not be {@literal null}.
	 * @return
	 */
	default Mono<Point> geoPos(ByteBuffer key, ByteBuffer member) {

		Assert.notNull(member, "member must not be null");

		return geoPos(key, Collections.singletonList(member)).map(vals -> vals.isEmpty() ? null : vals.iterator().next());
	}

	/**
	 * Get the {@link Point} representation of positions for one or more {@literal member}s.
	 *
	 * @param key must not be {@literal null}.
	 * @param members must not be {@literal null}.
	 * @return
	 */
	default Mono<List<Point>> geoPos(ByteBuffer key, List<ByteBuffer> members) {

		Assert.notNull(key, "key must not be null");
		Assert.notNull(members, "members must not be null");

		return geoPos(Mono.just(GeoPosCommand.members(members).of(key))).next().map(MultiValueResponse::getOutput);
	}

	/**
	 * Get the {@link Point} representation of positions for one or more {@literal member}s.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 */
	Flux<MultiValueResponse<GeoPosCommand, Point>> geoPos(Publisher<GeoPosCommand> commands);

	/**
	 * @author Christoph Strobl
	 */
	class GeoRadiusCommand extends KeyCommand {

		private final Distance distance;
		private final Point point;
		private final GeoRadiusCommandArgs args;
		private final ByteBuffer store;
		private final ByteBuffer storeDist;

		private GeoRadiusCommand(ByteBuffer key, Point point, Distance distance, GeoRadiusCommandArgs args,
				ByteBuffer store, ByteBuffer storeDist) {
			super(key);
			this.distance = distance;
			this.point = point;
			this.args = args == null ? GeoRadiusCommandArgs.newGeoRadiusArgs() : args;
			this.store = store;
			this.storeDist = storeDist;
		}

		public static GeoRadiusCommand within(Distance distance) {
			return new GeoRadiusCommand(null, null, distance, null, null, null);
		}

		public static GeoRadiusCommand withinMeters(Double distance) {
			return within(new Distance(distance, DistanceUnit.METERS));
		}

		public static GeoRadiusCommand withinKiometers(Double distance) {
			return within(new Distance(distance, DistanceUnit.KILOMETERS));
		}

		public static GeoRadiusCommand withinMiles(Double distance) {
			return within(new Distance(distance, DistanceUnit.MILES));
		}

		public static GeoRadiusCommand withinFeet(Double distance) {
			return within(new Distance(distance, DistanceUnit.FEET));
		}

		public static GeoRadiusCommand within(Circle circle) {
			return within(circle.getRadius()).from(circle.getCenter());
		}

		public GeoRadiusCommand from(Point center) {
			return new GeoRadiusCommand(getKey(), center, distance, args, store, storeDist);
		}

		public GeoRadiusCommand withFlag(Flag flag) {

			GeoRadiusCommandArgs args = cloneArgs();
			args.flags.add(flag);

			return new GeoRadiusCommand(getKey(), point, distance, args, store, storeDist);
		}

		public GeoRadiusCommand withCoord() {
			return withFlag(Flag.WITHCOORD);
		}

		public GeoRadiusCommand withDist() {
			return withFlag(Flag.WITHDIST);
		}

		public GeoRadiusCommand withArgs(GeoRadiusCommandArgs args) {
			return new GeoRadiusCommand(getKey(), point, distance, args, store, storeDist);
		}

		public GeoRadiusCommand limitTo(Long limit) {

			GeoRadiusCommandArgs args = cloneArgs();
			if (limit != null) {
				args = args.limit(limit);
			}

			return new GeoRadiusCommand(getKey(), point, distance, args, store, storeDist);
		}

		public GeoRadiusCommand sort(Direction direction) {

			GeoRadiusCommandArgs args = cloneArgs();
			args.sortDirection = direction;

			return new GeoRadiusCommand(getKey(), point, distance, args, store, storeDist);
		}

		public GeoRadiusCommand orderByDistanceAsc() {
			return sort(Direction.ASC);
		}

		public GeoRadiusCommand orderByDistanceDesc() {
			return sort(Direction.DESC);
		}

		public GeoRadiusCommand forKey(ByteBuffer key) {
			return new GeoRadiusCommand(key, point, distance, args, store, storeDist);
		}

		/**
		 * <b>NOTE:</b> STORE option is not compatible with WITHDIST, WITHHASH and WITHCOORDS options.
		 *
		 * @param key
		 * @return
		 */
		public GeoRadiusCommand storeAt(ByteBuffer key) {
			return new GeoRadiusCommand(getKey(), point, distance, args, key, storeDist);
		}

		/**
		 * <b>NOTE:</b> STOREDIST option is not compatible with WITHDIST, WITHHASH and WITHCOORDS options.
		 *
		 * @param key
		 * @return
		 */
		public GeoRadiusCommand storeDistAt(ByteBuffer key) {
			return new GeoRadiusCommand(getKey(), point, distance, args, store, key);
		}

		public Optional<Direction> getDirection() {
			return Optional.ofNullable(args.getSortDirection());
		}

		public Distance getDistance() {
			return distance;
		}

		public Set<Flag> getFlags() {
			return args.getFlags();
		}

		public Optional<Long> getLimit() {
			return Optional.ofNullable(args.getLimit());
		}

		public Point getPoint() {
			return point;
		}

		public Optional<ByteBuffer> getStore() {
			return Optional.ofNullable(store);
		}

		public Optional<ByteBuffer> getStoreDist() {
			return Optional.ofNullable(storeDist);
		}

		public Optional<GeoRadiusCommandArgs> getArgs() {
			return Optional.ofNullable(args);
		}

		private GeoRadiusCommandArgs cloneArgs() {

			if (args == null) {
				return GeoRadiusCommandArgs.newGeoRadiusArgs();
			}

			return args.clone();
		}
	}

	/**
	 * Get the {@literal member}s within the boundaries of a given {@link Circle}.
	 *
	 * @param key must not be {@literal null}.
	 * @param circle must not be {@literal null}.
	 * @return
	 */
	default Mono<List<GeoLocation<ByteBuffer>>> geoRadius(ByteBuffer key, Circle circle) {
		return geoRadius(key, circle, null)
				.map(res -> res.getContent().stream().map(val -> val.getContent()).collect(Collectors.toList()));
	}

	/**
	 * Get the {@literal member}s within the boundaries of a given {@link Circle} applying given parameters.
	 *
	 * @param key must not be {@literal null}.
	 * @param circle must not be {@literal null}.
	 * @param geoRadiusArgs can be {@literal null}.
	 * @return
	 */
	default Mono<GeoResults<GeoLocation<ByteBuffer>>> geoRadius(ByteBuffer key, Circle circle,
			GeoRadiusCommandArgs geoRadiusArgs) {

		Assert.notNull(key, "key must not be null");
		Assert.notNull(circle, "circle must not be null");

		return geoRadius(Mono.just(GeoRadiusCommand.within(circle).withArgs(geoRadiusArgs).forKey(key))).next()
				.map(CommandResponse::getOutput);
	}

	/**
	 * Get the {@literal member}s within the boundaries of a given {@link Circle} applying given parameters.
	 *
	 * @param commands
	 * @return
	 */
	Flux<CommandResponse<GeoRadiusCommand, GeoResults<GeoLocation<ByteBuffer>>>> geoRadius(
			Publisher<GeoRadiusCommand> commands);

	/**
	 * @author Christoph Strobl
	 */
	class GeoRadiusByMemberCommand extends KeyCommand {

		private final Distance distance;
		private final ByteBuffer member;
		private final GeoRadiusCommandArgs args;
		private final ByteBuffer store;
		private final ByteBuffer storeDist;

		private GeoRadiusByMemberCommand(ByteBuffer key, ByteBuffer member, Distance distance, GeoRadiusCommandArgs args,
				ByteBuffer store, ByteBuffer storeDist) {

			super(key);
			this.distance = distance;
			this.member = member;
			this.args = args == null ? GeoRadiusCommandArgs.newGeoRadiusArgs() : args;
			this.store = store;
			this.storeDist = storeDist;
		}

		public static GeoRadiusByMemberCommand within(Distance distance) {
			return new GeoRadiusByMemberCommand(null, null, distance, GeoRadiusCommandArgs.newGeoRadiusArgs(), null, null);
		}

		public static GeoRadiusByMemberCommand withinMeters(Double distance) {
			return within(new Distance(distance, DistanceUnit.METERS));
		}

		public static GeoRadiusByMemberCommand withinKiometers(Double distance) {
			return within(new Distance(distance, DistanceUnit.KILOMETERS));
		}

		public static GeoRadiusByMemberCommand withinMiles(Double distance) {
			return within(new Distance(distance, DistanceUnit.MILES));
		}

		public static GeoRadiusByMemberCommand withinFeet(Double distance) {
			return within(new Distance(distance, DistanceUnit.FEET));
		}

		public GeoRadiusByMemberCommand from(ByteBuffer member) {
			return new GeoRadiusByMemberCommand(getKey(), member, distance, args, store, storeDist);
		}

		public GeoRadiusByMemberCommand withArgs(GeoRadiusCommandArgs args) {
			return new GeoRadiusByMemberCommand(getKey(), member, distance, args, store, storeDist);
		}

		public GeoRadiusByMemberCommand withFlag(Flag flag) {

			GeoRadiusCommandArgs args = cloneArgs();
			args.flags.add(flag);

			return new GeoRadiusByMemberCommand(getKey(), member, distance, args, store, storeDist);
		}

		public GeoRadiusByMemberCommand withCoord() {
			return withFlag(Flag.WITHCOORD);
		}

		public GeoRadiusByMemberCommand withDist() {
			return withFlag(Flag.WITHDIST);
		}

		public GeoRadiusByMemberCommand limitTo(Long limit) {

			GeoRadiusCommandArgs args = cloneArgs();
			if (limit != null) {
				args = args.limit(limit);
			}

			return new GeoRadiusByMemberCommand(getKey(), member, distance, args, store, storeDist);
		}

		public GeoRadiusByMemberCommand sort(Direction direction) {

			GeoRadiusCommandArgs args = cloneArgs();
			args.sortDirection = direction;

			return new GeoRadiusByMemberCommand(getKey(), member, distance, args, store, storeDist);
		}

		public GeoRadiusByMemberCommand orderByDistanceAsc() {
			return sort(Direction.ASC);
		}

		public GeoRadiusByMemberCommand orderByDistanceDesc() {
			return sort(Direction.DESC);
		}

		public GeoRadiusByMemberCommand forKey(ByteBuffer key) {
			return new GeoRadiusByMemberCommand(key, member, distance, args, store, storeDist);
		}

		/**
		 * <b>NOTE:</b> STORE option is not compatible with WITHDIST, WITHHASH and WITHCOORDS options.
		 *
		 * @param key
		 * @return
		 */
		public GeoRadiusByMemberCommand storeAt(ByteBuffer key) {
			return new GeoRadiusByMemberCommand(getKey(), member, distance, args, key, storeDist);
		}

		/**
		 * <b>NOTE:</b> STOREDIST option is not compatible with WITHDIST, WITHHASH and WITHCOORDS options.
		 *
		 * @param key
		 * @return
		 */
		public GeoRadiusByMemberCommand storeDistAt(ByteBuffer key) {
			return new GeoRadiusByMemberCommand(getKey(), member, distance, args, store, key);
		}

		public Optional<Direction> getDirection() {
			return Optional.ofNullable(args.getSortDirection());
		}

		public Distance getDistance() {
			return distance;
		}

		public Set<Flag> getFlags() {
			return args.getFlags();
		}

		public Optional<Long> getLimit() {
			return Optional.ofNullable(args.getLimit());
		}

		public ByteBuffer getMember() {
			return member;
		}

		public Optional<ByteBuffer> getStore() {
			return Optional.ofNullable(store);
		}

		public Optional<ByteBuffer> getStoreDist() {
			return Optional.ofNullable(storeDist);
		}

		public Optional<GeoRadiusCommandArgs> getArgs() {
			return Optional.ofNullable(args);
		}

		private GeoRadiusCommandArgs cloneArgs() {

			if (args == null) {
				return GeoRadiusCommandArgs.newGeoRadiusArgs();
			}

			return args.clone();
		}

	}

	/**
	 * Get the {@literal member}s within given {@link Distance} from {@literal member} applying given parameters.
	 *
	 * @param key must not be {@literal null}.
	 * @param member must not be {@literal null}.
	 * @return
	 */
	default Mono<List<GeoLocation<ByteBuffer>>> geoRadiusByMember(ByteBuffer key, ByteBuffer member, Distance distance) {
		return geoRadiusByMember(key, member, distance, null)
				.map(res -> res.getContent().stream().map(val -> val.getContent()).collect(Collectors.toList()));
	}

	/**
	 * Get the {@literal member}s within given {@link Distance} from {@literal member} applying given parameters.
	 *
	 * @param key must not be {@literal null}.
	 * @param member must not be {@literal null}.
	 * @param geoRadiusArgs can be {@literal null}.
	 * @return
	 */
	default Mono<GeoResults<GeoLocation<ByteBuffer>>> geoRadiusByMember(ByteBuffer key, ByteBuffer member,
			Distance distance, GeoRadiusCommandArgs geoRadiusArgs) {

		Assert.notNull(key, "key must not be null");
		Assert.notNull(member, "member must not be null");
		Assert.notNull(distance, "distance must not be null");

		return geoRadiusByMember(
				Mono.just(GeoRadiusByMemberCommand.within(distance).from(member).forKey(key).withArgs(geoRadiusArgs))).next()
						.map(CommandResponse::getOutput);
	}

	/**
	 * Get the {@literal member}s within given {@link Distance} from {@literal member} applying given parameters.
	 *
	 * @param commands
	 * @return
	 */
	Flux<CommandResponse<GeoRadiusByMemberCommand, GeoResults<GeoLocation<ByteBuffer>>>> geoRadiusByMember(
			Publisher<GeoRadiusByMemberCommand> commands);
}
