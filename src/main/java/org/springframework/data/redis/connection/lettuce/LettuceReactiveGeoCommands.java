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
import com.lambdaworks.redis.GeoWithin;

import reactor.core.publisher.Flux;
import rx.Observable;

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
				values.add(location.getName().array());
			}

			return LettuceReactiveRedisConnection.<Long> monoConverter()
					.convert(cmd.geoadd(command.getKey().array(), values.toArray()))
					.map(value -> new NumericResponse<>(command, value));
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

			Metric metric = command.getMetric().isPresent() ? command.getMetric().get() : RedisGeoCommands.DistanceUnit.METERS;

			GeoArgs.Unit geoUnit = LettuceConverters.toGeoArgsUnit(metric);
			Converter<Double, Distance> distanceConverter = LettuceConverters.distanceConverterForMetric(metric);

			Observable<Distance> result = cmd
					.geodist(command.getKey().array(), command.getFrom().array(), command.getTo().array(), geoUnit)
					.map(distanceConverter::convert);

			return LettuceReactiveRedisConnection.<Distance> monoConverter().convert(result)
					.map(value -> new CommandResponse<>(command, value));
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

			return LettuceReactiveRedisConnection.<List<String>> monoConverter()
					.convert(cmd.geohash(command.getKey().array(),
							command.getMembers().stream().map(ByteBuffer::array).toArray(size -> new byte[size][])).toList())
					.map(value -> new MultiValueResponse<>(command, value));
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

			Observable<List<Point>> result = cmd
					.geopos(command.getKey().array(),
							command.getMembers().stream().map(ByteBuffer::array).toArray(size -> new byte[size][]))
					.map(LettuceConverters::geoCoordinatesToPoint).toList();

			return LettuceReactiveRedisConnection.<List<Point>> monoConverter().convert(result)
					.map(value -> new MultiValueResponse<>(command, value));
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

			GeoArgs geoArgs = command.getArgs().isPresent() ? LettuceConverters.toGeoArgs(command.getArgs().get()) : new GeoArgs();

			Observable<GeoResults<GeoLocation<ByteBuffer>>> result = cmd
					.georadius(command.getKey().array(), command.getPoint().getX(), command.getPoint().getY(),
							command.getDistance().getValue(), LettuceConverters.toGeoArgsUnit(command.getDistance().getMetric()),
							geoArgs)
					.map(converter(command.getDistance().getMetric())::convert).toList().map(vals -> new GeoResults<>(vals));

			return LettuceReactiveRedisConnection.<GeoResults<GeoLocation<ByteBuffer>>> monoConverter().convert(result)
					.map(value -> new CommandResponse<>(command, value));
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

			GeoArgs geoArgs = command.getArgs().isPresent() ? LettuceConverters.toGeoArgs(command.getArgs().get()) : new GeoArgs();

			Observable<GeoResults<GeoLocation<ByteBuffer>>> result = cmd
					.georadiusbymember(command.getKey().array(), command.getMember().array(), command.getDistance().getValue(),
							LettuceConverters.toGeoArgsUnit(command.getDistance().getMetric()), geoArgs)
					.map(converter(command.getDistance().getMetric())::convert).toList().map(vals -> new GeoResults<>(vals));

			return LettuceReactiveRedisConnection.<GeoResults<GeoLocation<ByteBuffer>>> monoConverter().convert(result)
					.map(value -> new CommandResponse<>(command, value));
		}));
	}

	private Converter<GeoWithin<byte[]>, GeoResult<GeoLocation<ByteBuffer>>> converter(Metric metric) {

		return (source) -> {

			Point point = LettuceConverters.geoCoordinatesToPoint(source.coordinates);

			return new GeoResult<>(new GeoLocation<>(ByteBuffer.wrap(source.member), point),
					new Distance(source.distance != null ? source.distance : 0D, metric));
		};

	}

}
