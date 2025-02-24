/*
 * Copyright 2016-2025 the original author or authors.
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

import io.lettuce.core.BitFieldArgs;
import io.lettuce.core.GetExArgs;
import io.lettuce.core.SetArgs;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;

import org.reactivestreams.Publisher;

import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.ReactiveRedisConnection.AbsentByteBufferResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.BooleanResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.ByteBufferResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.KeyCommand;
import org.springframework.data.redis.connection.ReactiveRedisConnection.MultiValueResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.NumericResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.RangeCommand;
import org.springframework.data.redis.connection.ReactiveStringCommands;
import org.springframework.data.redis.connection.RedisStringCommands;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.util.Assert;

/**
 * {@link ReactiveStringCommands} implemented using {@literal Lettuce}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Jiahe Cai
 * @author Michele Mancioppi
 * @author John Blum
 * @author Marcin Grzejszczak
 * @since 2.0
 */
class LettuceReactiveStringCommands implements ReactiveStringCommands {

	private final LettuceReactiveRedisConnection connection;

	/**
	 * Create new {@link LettuceReactiveStringCommands}.
	 *
	 * @param connection must not be {@literal null}.
	 */
	LettuceReactiveStringCommands(LettuceReactiveRedisConnection connection) {

		Assert.notNull(connection, "Connection must not be null");

		this.connection = connection;
	}

	@Override
	public Flux<MultiValueResponse<List<ByteBuffer>, ByteBuffer>> mGet(Publisher<List<ByteBuffer>> keyCollections) {

		return this.connection.execute(reactiveCommands -> Flux.from(keyCollections).concatMap((keys) -> {

			Assert.notNull(keys, "Keys must not be null");

			return reactiveCommands.mget(keys.toArray(new ByteBuffer[0])).collectList()
					.map(value -> value.stream().map(keyValue -> keyValue.getValueOrElse(null)).collect(Collectors.toList()))
					.map(values -> new MultiValueResponse<>(keys, values));
		}));
	}

	@Override
	public Flux<BooleanResponse<SetCommand>> set(Publisher<SetCommand> commands) {

		return this.connection.execute(reactiveCommands -> Flux.from(commands).concatMap((command) -> {

			Assert.notNull(command.getKey(), "Key must not be null");
			Assert.notNull(command.getValue(), "Value must not be null");

			SetArgs args = null;

			if (command.getExpiration().isPresent() || command.getOption().isPresent()) {

				Expiration expiration = command.getExpiration().orElse(null);
				RedisStringCommands.SetOption setOption = command.getOption().orElse(null);

				args = LettuceConverters.toSetArgs(expiration, setOption);
			}

			Mono<String> mono = args != null ? reactiveCommands.set(command.getKey(), command.getValue(), args)
					: reactiveCommands.set(command.getKey(), command.getValue());

			return mono.map(LettuceConverters::stringToBoolean).map(value -> new BooleanResponse<>(command, value))
					.switchIfEmpty(Mono.just(new BooleanResponse<>(command, Boolean.FALSE)));
		}));
	}

	@Override
	public Flux<ByteBufferResponse<SetCommand>> setGet(Publisher<SetCommand> commands) {

		return this.connection.execute(reactiveCommands -> Flux.from(commands).concatMap((command) -> {

			Assert.notNull(command.getKey(), "Key must not be null");
			Assert.notNull(command.getValue(), "Value must not be null");

			return reactiveCommands.setGet(command.getKey(), command.getValue())
					.map(v -> new ByteBufferResponse<>(command, v))
					.defaultIfEmpty(new AbsentByteBufferResponse<>(command));
		}));
	}

	@Override
	public Flux<ByteBufferResponse<SetCommand>> getSet(Publisher<SetCommand> commands) {

		return this.connection.execute(reactiveCommands -> Flux.from(commands).concatMap((command) -> {

			Assert.notNull(command.getKey(), "Key must not be null");
			Assert.notNull(command.getValue(), "Value must not be null");

			if (command.getExpiration().isPresent() || command.getOption().isPresent()) {
				throw new IllegalArgumentException("Command must not define expiration nor option for GETSET");
			}

			return reactiveCommands.getset(command.getKey(), command.getValue())
					.map(value -> new ByteBufferResponse<>(command, value))
					.defaultIfEmpty(new AbsentByteBufferResponse<>(command));
		}));
	}

	@Override
	public Flux<ByteBufferResponse<KeyCommand>> get(Publisher<KeyCommand> commands) {

		return this.connection.execute(reactiveCommands -> Flux.from(commands).concatMap((command) -> {

			Assert.notNull(command.getKey(), "Key must not be null");

			return reactiveCommands.get(command.getKey())
					.map(value -> new ByteBufferResponse<>(command, value))
					.defaultIfEmpty(new AbsentByteBufferResponse<>(command));
		}));
	}

	@Override
	public Flux<ByteBufferResponse<KeyCommand>> getDel(Publisher<KeyCommand> commands) {

		return this.connection.execute(reactiveCommands -> Flux.from(commands).concatMap((command) -> {

			Assert.notNull(command.getKey(), "Key must not be null");

			return reactiveCommands.getdel(command.getKey())
					.map(value -> new ByteBufferResponse<>(command, value))
					.defaultIfEmpty(new AbsentByteBufferResponse<>(command));
		}));
	}

	@Override
	public Flux<ByteBufferResponse<GetExCommand>> getEx(Publisher<GetExCommand> commands) {

		return this.connection.execute(reactiveCommands -> Flux.from(commands).concatMap((command) -> {

			Assert.notNull(command.getKey(), "Key must not be null");

			GetExArgs args = LettuceConverters.toGetExArgs(command.getExpiration());

			return reactiveCommands.getex(command.getKey(), args)
					.map(value -> new ByteBufferResponse<>(command, value))
					.defaultIfEmpty(new AbsentByteBufferResponse<>(command));
		}));
	}

	@Override
	public Flux<BooleanResponse<SetCommand>> setNX(Publisher<SetCommand> commands) {

		return this.connection.execute(reactiveCommands -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null");
			Assert.notNull(command.getValue(), "Value must not be null");

			return reactiveCommands.setnx(command.getKey(), command.getValue())
					.map((value) -> new BooleanResponse<>(command, value));
		}));
	}

	@Override
	public Flux<BooleanResponse<SetCommand>> setEX(Publisher<SetCommand> commands) {

		return this.connection.execute(reactiveCommands -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null");
			Assert.notNull(command.getValue(), "Value must not be null");
			Assert.isTrue(command.getExpiration().isPresent(), "Expiration time must not be null");

			long expirationTimeInSeconds = command.getExpiration().get().getExpirationTimeInSeconds();

			return reactiveCommands.setex(command.getKey(), expirationTimeInSeconds, command.getValue())
					.map(LettuceConverters::stringToBoolean)
					.map((value) -> new BooleanResponse<>(command, value));
		}));
	}

	@Override
	public Flux<BooleanResponse<SetCommand>> pSetEX(Publisher<SetCommand> commands) {

		return this.connection.execute(reactiveCommands -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null");
			Assert.notNull(command.getValue(), "Value must not be null");
			Assert.isTrue(command.getExpiration().isPresent(), "Expiration time must not be null");

			long expirationTimeInSeconds = command.getExpiration().get().getExpirationTimeInMilliseconds();

			return reactiveCommands.psetex(command.getKey(), expirationTimeInSeconds, command.getValue())
					.map(LettuceConverters::stringToBoolean)
					.map((value) -> new BooleanResponse<>(command, value));
		}));
	}

	@Override
	public Flux<BooleanResponse<MSetCommand>> mSet(Publisher<MSetCommand> commands) {

		return this.connection.execute(reactiveCommands -> Flux.from(commands).concatMap(command -> {

			Assert.notEmpty(command.getKeyValuePairs(), "Pairs must not be null or empty");

			return reactiveCommands.mset(command.getKeyValuePairs())
					.map(LettuceConverters::stringToBoolean)
					.map((value) -> new BooleanResponse<>(command, value));
		}));
	}

	@Override
	public Flux<BooleanResponse<MSetCommand>> mSetNX(Publisher<MSetCommand> commands) {

		return this.connection.execute(reactiveCommands -> Flux.from(commands).concatMap(command -> {

			Assert.notEmpty(command.getKeyValuePairs(), "Pairs must not be null or empty");

			return reactiveCommands.msetnx(command.getKeyValuePairs())
					.map((value) -> new BooleanResponse<>(command, value));
		}));
	}

	@Override
	public Flux<NumericResponse<AppendCommand, Long>> append(Publisher<AppendCommand> commands) {

		return this.connection.execute(reactiveCommands -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null");
			Assert.notNull(command.getValue(), "Value must not be null");

			return reactiveCommands.append(command.getKey(), command.getValue())
					.map(value -> new NumericResponse<>(command, value));
		}));
	}

	@Override
	public Flux<ByteBufferResponse<RangeCommand>> getRange(Publisher<RangeCommand> commands) {

		return this.connection.execute(reactiveCommands -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null");
			Assert.notNull(command.getRange(), "Range must not be null");

			Range<Long> range = command.getRange();

			Mono<ByteBuffer> result = reactiveCommands.getrange(command.getKey(), //
					LettuceConverters.getLowerBoundIndex(range), //
					LettuceConverters.getUpperBoundIndex(range));

			return result.map(value -> new ByteBufferResponse<>(command, value));
		}));
	}

	@Override
	public Flux<NumericResponse<SetRangeCommand, Long>> setRange(Publisher<SetRangeCommand> commands) {

		return this.connection.execute(reactiveCommands -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null");
			Assert.notNull(command.getValue(), "Value must not be null");
			Assert.notNull(command.getOffset(), "Offset must not be null");

			return reactiveCommands.setrange(command.getKey(), command.getOffset(), command.getValue())
					.map((value) -> new NumericResponse<>(command, value));
		}));
	}

	@Override
	public Flux<BooleanResponse<GetBitCommand>> getBit(Publisher<GetBitCommand> commands) {

		return this.connection.execute(reactiveCommands -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null");
			Assert.notNull(command.getOffset(), "Offset must not be null");

			return reactiveCommands.getbit(command.getKey(), command.getOffset())
					.map(LettuceConverters::toBoolean)
					.map(value -> new BooleanResponse<>(command, value));
		}));
	}

	@Override
	public Flux<BooleanResponse<SetBitCommand>> setBit(Publisher<SetBitCommand> commands) {

		return this.connection.execute(reactiveCommands -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null");
			Assert.notNull(command.getOffset(), "Offset must not be null");

			return reactiveCommands.setbit(command.getKey(), command.getOffset(), command.getValue() ? 1 : 0)
					.map(LettuceConverters::toBoolean).map(respValue -> new BooleanResponse<>(command, respValue));
		}));
	}

	@Override
	public Flux<NumericResponse<BitCountCommand, Long>> bitCount(Publisher<BitCountCommand> commands) {

		return this.connection.execute(reactiveCommands -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null");

			Range<Long> range = command.getRange();

			Mono<Long> bitcount = !Range.unbounded().equals(range)
					? reactiveCommands.bitcount(command.getKey(), LettuceConverters.getLowerBoundIndex(range),
							LettuceConverters.getUpperBoundIndex(range))
					: reactiveCommands.bitcount(command.getKey());

			return bitcount.map(responseValue -> new NumericResponse<>(command, responseValue));
		}));
	}

	@Override
	public Flux<MultiValueResponse<BitFieldCommand, Long>> bitField(Publisher<BitFieldCommand> commands) {

		return this.connection.execute(reactiveCommands -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null");

			BitFieldArgs args = LettuceConverters.toBitFieldArgs(command.getSubCommands());

			return reactiveCommands.bitfield(command.getKey(), args).collectList()
					.map(value -> new MultiValueResponse<>(command, value.stream()
							.map(v -> v.getValueOrElse(null))
							.collect(Collectors.toList())));
		}));
	}

	@Override
	public Flux<NumericResponse<BitOpCommand, Long>> bitOp(Publisher<BitOpCommand> commands) {

		return this.connection.execute(reactiveCommands -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getDestinationKey(), "DestinationKey must not be null");
			Assert.notEmpty(command.getKeys(), "Keys must not be null or empty");

			ByteBuffer destinationKey = command.getDestinationKey();

			ByteBuffer[] sourceKeys = command.getKeys().toArray(new ByteBuffer[0]);

			Mono<Long> result;

			result = switch (command.getBitOp()) {
				case AND -> reactiveCommands.bitopAnd(destinationKey, sourceKeys);
				case OR -> reactiveCommands.bitopOr(destinationKey, sourceKeys);
				case XOR -> reactiveCommands.bitopXor(destinationKey, sourceKeys);
				case NOT -> {
					Assert.isTrue(sourceKeys.length == 1, "BITOP NOT does not allow more than 1 source key.");
					yield reactiveCommands.bitopNot(destinationKey, sourceKeys[0]);
				}
				default -> throw new IllegalArgumentException("Unknown BITOP '%s'".formatted(command.getBitOp()));
			};

			return result.map(value -> new NumericResponse<>(command, value));
		}));
	}

	@Override
	public Flux<NumericResponse<BitPosCommand, Long>> bitPos(Publisher<BitPosCommand> commands) {

		return this.connection.execute(reactiveCommands -> Flux.from(commands).flatMap(command -> {

				Range<Long> range = command.getRange();
				Mono<Long> result;

				if (range.getLowerBound().isBounded()) {

					result = reactiveCommands.bitpos(command.getKey(), command.getBit(), getLowerValue(range));

					if (range.getUpperBound().isBounded()) {
						result = reactiveCommands.bitpos(command.getKey(), command.getBit(),
								getLowerValue(range), getUpperValue(range));
					}
				} else {
					result = reactiveCommands.bitpos(command.getKey(), command.getBit());
				}

				return result.map(respValue -> new NumericResponse<>(command, respValue));
			}));
	}

	@Override
	public Flux<NumericResponse<KeyCommand, Long>> strLen(Publisher<KeyCommand> commands) {

		return this.connection.execute(reactiveCommands -> Flux.from(commands).concatMap(command ->
				reactiveCommands.strlen(command.getKey()).map(respValue -> new NumericResponse<>(command, respValue))));
	}

	protected LettuceReactiveRedisConnection getConnection() {
		return this.connection;
	}

	private static <T extends Comparable<T>> T getUpperValue(Range<T> range) {
		return range.getUpperBound().getValue()
				.orElseThrow(() -> new IllegalArgumentException("Range does not contain upper bound value"));
	}

	private static <T extends Comparable<T>> T getLowerValue(Range<T> range) {
		return range.getLowerBound().getValue()
				.orElseThrow(() -> new IllegalArgumentException("Range does not contain lower bound value"));
	}
}
