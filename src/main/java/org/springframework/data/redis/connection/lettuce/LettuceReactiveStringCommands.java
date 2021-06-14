/*
 * Copyright 2016-2021 the original author or authors.
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
import org.springframework.util.Assert;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Jiahe Cai
 * @author Michele Mancioppi
 * @since 2.0
 */
class LettuceReactiveStringCommands implements ReactiveStringCommands {

	private static final ByteBuffer EMPTY_BYTE_BUFFER = ByteBuffer.wrap(new byte[0]);

	private final LettuceReactiveRedisConnection connection;

	/**
	 * Create new {@link LettuceReactiveStringCommands}.
	 *
	 * @param connection must not be {@literal null}.
	 */
	LettuceReactiveStringCommands(LettuceReactiveRedisConnection connection) {

		Assert.notNull(connection, "Connection must not be null!");

		this.connection = connection;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisConnection.ReactiveStringCommands#mGet(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<MultiValueResponse<List<ByteBuffer>, ByteBuffer>> mGet(Publisher<List<ByteBuffer>> keyCollections) {

		return connection.execute(cmd -> Flux.from(keyCollections).concatMap((keys) -> {

			Assert.notNull(keys, "Keys must not be null!");

			return cmd.mget(keys.toArray(new ByteBuffer[0])).map((value) -> value.getValueOrElse(EMPTY_BYTE_BUFFER))
					.collectList().map((values) -> new MultiValueResponse<>(keys, values));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisConnection.ReactiveStringCommands#set(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<BooleanResponse<SetCommand>> set(Publisher<SetCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).concatMap((command) -> {

			Assert.notNull(command.getKey(), "Key must not be null!");
			Assert.notNull(command.getValue(), "Value must not be null!");

			SetArgs args = null;

			if (command.getExpiration().isPresent() || command.getOption().isPresent()) {
				args = LettuceConverters.toSetArgs(command.getExpiration().isPresent() ? command.getExpiration().get() : null,
						command.getOption().isPresent() ? command.getOption().get() : null);
			}

			Mono<String> mono = args != null ? cmd.set(command.getKey(), command.getValue(), args)
					: cmd.set(command.getKey(), command.getValue());
			return mono.map(LettuceConverters::stringToBoolean).map(value -> new BooleanResponse<>(command, value))
					.switchIfEmpty(Mono.just(new BooleanResponse<>(command, Boolean.FALSE)));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisConnection.ReactiveStringCommands#getSet(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<ByteBufferResponse<SetCommand>> getSet(Publisher<SetCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).concatMap((command) -> {

			Assert.notNull(command.getKey(), "Key must not be null!");
			Assert.notNull(command.getValue(), "Value must not be null!");

			if (command.getExpiration().isPresent() || command.getOption().isPresent()) {
				throw new IllegalArgumentException("Command must not define expiration nor option for GETSET.");
			}

			return cmd.getset(command.getKey(), command.getValue()).map((value) -> new ByteBufferResponse<>(command, value))
					.defaultIfEmpty(new AbsentByteBufferResponse<>(command));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisConnection.ReactiveStringCommands#get(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<ByteBufferResponse<KeyCommand>> get(Publisher<KeyCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).concatMap((command) -> {

			Assert.notNull(command.getKey(), "Key must not be null!");

			return cmd.get(command.getKey()).map((value) -> new ByteBufferResponse<>(command, value))
					.defaultIfEmpty(new AbsentByteBufferResponse<>(command));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisConnection.ReactiveStringCommands#getDel(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<ByteBufferResponse<KeyCommand>> getDel(Publisher<KeyCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).concatMap((command) -> {

			Assert.notNull(command.getKey(), "Key must not be null!");

			return cmd.getdel(command.getKey()).map((value) -> new ByteBufferResponse<>(command, value))
					.defaultIfEmpty(new AbsentByteBufferResponse<>(command));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisConnection.ReactiveStringCommands#getDel(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<ByteBufferResponse<GetExCommand>> getEx(Publisher<GetExCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).concatMap((command) -> {

			Assert.notNull(command.getKey(), "Key must not be null!");

			GetExArgs args = LettuceConverters.toGetExArgs(command.getExpiration());

			return cmd.getex(command.getKey(), args).map((value) -> new ByteBufferResponse<>(command, value))
					.defaultIfEmpty(new AbsentByteBufferResponse<>(command));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisConnection.ReactiveStringCommands#setNX(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<BooleanResponse<SetCommand>> setNX(Publisher<SetCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null!");
			Assert.notNull(command.getValue(), "Value must not be null!");

			return cmd.setnx(command.getKey(), command.getValue()).map((value) -> new BooleanResponse<>(command, value));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisConnection.ReactiveStringCommands#setEX(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<BooleanResponse<SetCommand>> setEX(Publisher<SetCommand> commands) {
		return connection.execute(cmd -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null!");
			Assert.notNull(command.getValue(), "Value must not be null!");
			Assert.isTrue(command.getExpiration().isPresent(), "Expiration time must not be null!");

			return cmd.setex(command.getKey(), command.getExpiration().get().getExpirationTimeInSeconds(), command.getValue())
					.map(LettuceConverters::stringToBoolean).map((value) -> new BooleanResponse<>(command, value));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisConnection.ReactiveStringCommands#pSetEX(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<BooleanResponse<SetCommand>> pSetEX(Publisher<SetCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null!");
			Assert.notNull(command.getValue(), "Value must not be null!");
			Assert.isTrue(command.getExpiration().isPresent(), "Expiration time must not be null!");

			return cmd
					.psetex(command.getKey(), command.getExpiration().get().getExpirationTimeInMilliseconds(), command.getValue())
					.map(LettuceConverters::stringToBoolean).map((value) -> new BooleanResponse<>(command, value));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisConnection.ReactiveStringCommands#mSet(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<BooleanResponse<MSetCommand>> mSet(Publisher<MSetCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).concatMap(command -> {

			Assert.notEmpty(command.getKeyValuePairs(), "Pairs must not be null or empty!");

			return cmd.mset(command.getKeyValuePairs()).map(LettuceConverters::stringToBoolean)
					.map((value) -> new BooleanResponse<>(command, value));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisConnection.ReactiveStringCommands#mSet(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<BooleanResponse<MSetCommand>> mSetNX(Publisher<MSetCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).concatMap(command -> {

			Assert.notEmpty(command.getKeyValuePairs(), "Pairs must not be null or empty!");

			return cmd.msetnx(command.getKeyValuePairs()).map((value) -> new BooleanResponse<>(command, value));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisConnection.ReactiveStringCommands#append(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<NumericResponse<AppendCommand, Long>> append(Publisher<AppendCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null!");
			Assert.notNull(command.getValue(), "Value must not be null!");

			return cmd.append(command.getKey(), command.getValue()).map((value) -> new NumericResponse<>(command, value));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisConnection.ReactiveStringCommands#getRange(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<ByteBufferResponse<RangeCommand>> getRange(Publisher<RangeCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null!");
			Assert.notNull(command.getRange(), "Range must not be null!");

			Range<Long> range = command.getRange();

			Mono<ByteBuffer> result = cmd.getrange(command.getKey(), //
					LettuceConverters.getLowerBoundIndex(range), //
					LettuceConverters.getUpperBoundIndex(range));

			return result.map((value) -> new ByteBufferResponse<>(command, value));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisConnection.ReactiveStringCommands#setRange(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<NumericResponse<SetRangeCommand, Long>> setRange(Publisher<SetRangeCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null!");
			Assert.notNull(command.getValue(), "Value must not be null!");
			Assert.notNull(command.getOffset(), "Offset must not be null!");

			return cmd.setrange(command.getKey(), command.getOffset(), command.getValue())
					.map((value) -> new NumericResponse<>(command, value));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisConnection.ReactiveStringCommands#getBit(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<BooleanResponse<GetBitCommand>> getBit(Publisher<GetBitCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null!");
			Assert.notNull(command.getOffset(), "Offset must not be null!");

			return cmd.getbit(command.getKey(), command.getOffset()).map(LettuceConverters::toBoolean)
					.map(value -> new BooleanResponse<>(command, value));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisConnection.ReactiveStringCommands#setBit(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<BooleanResponse<SetBitCommand>> setBit(Publisher<SetBitCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null!");
			Assert.notNull(command.getValue(), "Value must not be null!");
			Assert.notNull(command.getOffset(), "Offset must not be null!");

			return cmd.setbit(command.getKey(), command.getOffset(), command.getValue() ? 1 : 0)
					.map(LettuceConverters::toBoolean).map(respValue -> new BooleanResponse<>(command, respValue));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisConnection.ReactiveStringCommands#bitCount(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<NumericResponse<BitCountCommand, Long>> bitCount(Publisher<BitCountCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null!");

			Range<Long> range = command.getRange();

			return (!Range.unbounded().equals(range) ? cmd.bitcount(command.getKey(),
					LettuceConverters.getLowerBoundIndex(range), //
					LettuceConverters.getUpperBoundIndex(range)) //
					: cmd.bitcount(command.getKey())).map(responseValue -> new NumericResponse<>(command, responseValue));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisConnection.ReactiveStringCommands#bitField(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<MultiValueResponse<BitFieldCommand, Long>> bitField(Publisher<BitFieldCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null!");

			BitFieldArgs args = LettuceConverters.toBitFieldArgs(command.getSubCommands());

			return cmd.bitfield(command.getKey(), args).collectList().map(value -> new MultiValueResponse<>(command,
					value.stream().map(v -> v.getValueOrElse(null)).collect(Collectors.toList())));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisConnection.ReactiveStringCommands#bitOp(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<NumericResponse<BitOpCommand, Long>> bitOp(Publisher<BitOpCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getDestinationKey(), "DestinationKey must not be null!");
			Assert.notEmpty(command.getKeys(), "Keys must not be null or empty");

			Mono<Long> result = null;
			ByteBuffer destinationKey = command.getDestinationKey();
			ByteBuffer[] sourceKeys = command.getKeys().toArray(new ByteBuffer[0]);

			switch (command.getBitOp()) {
				case AND:
					result = cmd.bitopAnd(destinationKey, sourceKeys);
					break;
				case OR:
					result = cmd.bitopOr(destinationKey, sourceKeys);
					break;
				case XOR:
					result = cmd.bitopXor(destinationKey, sourceKeys);
					break;
				case NOT:

					Assert.isTrue(sourceKeys.length == 1, "BITOP NOT does not allow more than 1 source key.");

					result = cmd.bitopNot(destinationKey, sourceKeys[0]);
					break;
				default:
					throw new IllegalArgumentException(String.format("Unknown BITOP '%s'.", command.getBitOp()));
			}

			return result.map(value -> new NumericResponse<>(command, value));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisConnection.ReactiveStringCommands#bitPos(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<NumericResponse<BitPosCommand, Long>> bitPos(Publisher<BitPosCommand> commands) {

		return connection.execute(cmd -> {

			return Flux.from(commands).flatMap(command -> {

				Mono<Long> result;
				Range<Long> range = command.getRange();

				if (range.getLowerBound().isBounded()) {

					result = cmd.bitpos(command.getKey(), command.getBit(), getLowerValue(range));

					if (range.getUpperBound().isBounded()) {
						result = cmd.bitpos(command.getKey(), command.getBit(), getLowerValue(range), getUpperValue(range));
					}
				} else {
					result = cmd.bitpos(command.getKey(), command.getBit());
				}

				return result.map(respValue -> new NumericResponse<>(command, respValue));
			});
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisConnection.ReactiveStringCommands#strLen(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<NumericResponse<KeyCommand, Long>> strLen(Publisher<KeyCommand> commands) {

		return connection.execute(cmd -> {

			return Flux.from(commands).concatMap(command -> {
				return cmd.strlen(command.getKey()).map(respValue -> new NumericResponse<>(command, respValue));
			});
		});
	}

	protected LettuceReactiveRedisConnection getConnection() {
		return connection;
	}

	private static <T extends Comparable<T>> T getUpperValue(Range<T> range) {
		return range.getUpperBound().getValue()
				.orElseThrow(() -> new IllegalArgumentException("Range does not contain upper bound value!"));
	}

	private static <T extends Comparable<T>> T getLowerValue(Range<T> range) {
		return range.getLowerBound().getValue()
				.orElseThrow(() -> new IllegalArgumentException("Range does not contain lower bound value!"));
	}
}
