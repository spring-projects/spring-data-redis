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
package org.springframework.data.redis.connection.lettuce;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.reactivestreams.Publisher;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.geo.Metric;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.ReactiveGeoCommands;
import org.springframework.data.redis.connection.ReactiveRedisConnection.CommandResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.MultiValueResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.NumericResponse;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.connection.RedisGeoCommands.GeoLocation;
import org.springframework.util.Assert;

import com.lambdaworks.redis.GeoArgs;
import com.lambdaworks.redis.GeoCoordinates;
import com.lambdaworks.redis.GeoWithin;
import com.lambdaworks.redis.Value;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * @author Christoph Strobl
 * @since 2.0
 */
public class LettuceReactiveGeoCommands implements ReactiveGeoCommands {

	private final LettuceReactiveRedisConnection connection;

	/**
	 * Create new {@link LettuceReactiveGeoCommands}.
	 *
	 * @param connection must not be {@literal null}.
	 */
	public LettuceReactiveGeoCommands(LettuceReactiveRedisConnection connection) {

		Assert.notNull(connection, "Connection must not be null!");
		this.connection = connection;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveGeoCommands#geoAdd(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<NumericResponse<GeoAddCommand, Long>> geoAdd(Publisher<GeoAddCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).flatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null!");
			Assert.notNull(command.getGeoLocations(), "Locations must not be null!");

			List<Object> values = new ArrayList<>();
			for (GeoLocation<ByteBuffer> location : command.getGeoLocations()) {

				Assert.notNull(location.getName(), "Location.Name must not be null!");
				Assert.notNull(location.getPoint(), "Location.Point must not be null!");

				values.add(location.getPoint().getX());
				values.add(location.getPoint().getY());
				values.add(location.getName());
			}

			return cmd.geoadd(command.getKey(), values.toArray()).map(value -> new NumericResponse<>(command, value));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveGeoCommands#geoDist(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<CommandResponse<GeoDistCommand, Distance>> geoDist(Publisher<GeoDistCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).flatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null!");
			Assert.notNull(command.getFrom(), "From member must not be null!");
			Assert.notNull(command.getTo(), "To member must not be null!");

			Metric metric = command.getMetric().isPresent() ? command.getMetric().get()
					: RedisGeoCommands.DistanceUnit.METERS;

			GeoArgs.Unit geoUnit = LettuceConverters.toGeoArgsUnit(metric);
			Converter<Double, Distance> distanceConverter = LettuceConverters.distanceConverterForMetric(metric);

			Mono<Distance> result = cmd.geodist(command.getKey(), command.getFrom(), command.getTo(), geoUnit)
					.map(distanceConverter::convert);

			return result.map(value -> new CommandResponse<>(command, value));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveGeoCommands#geoHash(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<MultiValueResponse<GeoHashCommand, String>> geoHash(Publisher<GeoHashCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).flatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null!");
			Assert.notNull(command.getMembers(), "Members must not be null!");

			return cmd.geohash(command.getKey(), command.getMembers().stream().toArray(ByteBuffer[]::new)).collectList()
					.map(value -> new MultiValueResponse<>(command,
							value.stream().map(v -> v.getValueOrElse(null)).collect(Collectors.toList())));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveGeoCommands#geoPos(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<MultiValueResponse<GeoPosCommand, Point>> geoPos(Publisher<GeoPosCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).flatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null!");
			Assert.notNull(command.getMembers(), "Members must not be null!");

			Mono<List<Value<GeoCoordinates>>> result = cmd
					.geopos(command.getKey(), command.getMembers().stream().toArray(ByteBuffer[]::new)).collectList();

			return result.map(value -> new MultiValueResponse<>(command, value.stream().map(v -> v.getValueOrElse(null))
					.map(LettuceConverters::geoCoordinatesToPoint).collect(Collectors.toList())));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveGeoCommands#geoRadius(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<CommandResponse<GeoRadiusCommand, GeoResults<GeoLocation<ByteBuffer>>>> geoRadius(
			Publisher<GeoRadiusCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).flatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null!");
			Assert.notNull(command.getPoint(), "Point must not be null!");
			Assert.notNull(command.getDistance(), "Distance must not be null!");

			GeoArgs geoArgs = command.getArgs().isPresent() ? LettuceConverters.toGeoArgs(command.getArgs().get())
					: new GeoArgs();

			Mono<GeoResults<GeoLocation<ByteBuffer>>> result = cmd.georadius(command.getKey(), command.getPoint().getX(),
					command.getPoint().getY(), command.getDistance().getValue(),
					LettuceConverters.toGeoArgsUnit(command.getDistance().getMetric()), geoArgs)
					.map(converter(command.getDistance().getMetric())::convert).collectList().map(GeoResults::new);

			return result.map(value -> new CommandResponse<>(command, value));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveGeoCommands#geoRadiusByMember(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<CommandResponse<GeoRadiusByMemberCommand, GeoResults<GeoLocation<ByteBuffer>>>> geoRadiusByMember(
			Publisher<GeoRadiusByMemberCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).flatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null!");
			Assert.notNull(command.getMember(), "Member must not be null!");
			Assert.notNull(command.getDistance(), "Distance must not be null!");

			GeoArgs geoArgs = command.getArgs().isPresent() ? LettuceConverters.toGeoArgs(command.getArgs().get())
					: new GeoArgs();

			Mono<GeoResults<GeoLocation<ByteBuffer>>> result = cmd
					.georadiusbymember(command.getKey(), command.getMember(), command.getDistance().getValue(),
							LettuceConverters.toGeoArgsUnit(command.getDistance().getMetric()), geoArgs)
					.map(converter(command.getDistance().getMetric())::convert).collectList().map(GeoResults::new);

			return result.map(value -> new CommandResponse<>(command, value));
		}));
	}

	private Converter<GeoWithin<ByteBuffer>, GeoResult<GeoLocation<ByteBuffer>>> converter(Metric metric) {

		return (source) -> {

			Point point = LettuceConverters.geoCoordinatesToPoint(source.getCoordinates());

			return new GeoResult<>(new GeoLocation<>(source.getMember(), point),
					new Distance(source.getDistance() != null ? source.getDistance() : 0D, metric));
		};
	}
}
