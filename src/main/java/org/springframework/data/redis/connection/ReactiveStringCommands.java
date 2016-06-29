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
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.reactivestreams.Publisher;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.ReactiveRedisConnection.BooleanResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.ByteBufferResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.Command;
import org.springframework.data.redis.connection.ReactiveRedisConnection.KeyCommand;
import org.springframework.data.redis.connection.ReactiveRedisConnection.MultiValueResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.NumericResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.RangeCommand;
import org.springframework.data.redis.connection.RedisStringCommands.BitOperation;
import org.springframework.data.redis.connection.RedisStringCommands.SetOption;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.util.Assert;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * @author Christoph Strobl
 * @since 2.0
 */
public interface ReactiveStringCommands {

	/**
	 * @author Christoph Strobl
	 */
	class SetCommand extends KeyCommand {

		private ByteBuffer value;
		private Expiration expiration;
		private SetOption option;

		private SetCommand(ByteBuffer key, ByteBuffer value, Expiration expiration, SetOption option) {

			super(key);
			this.value = value;
			this.expiration = expiration;
			this.option = option;
		}

		public static ReactiveStringCommands.SetCommand set(ByteBuffer key) {
			return new SetCommand(key, null, null, null);
		}

		public ReactiveStringCommands.SetCommand value(ByteBuffer value) {
			return new SetCommand(getKey(), value, expiration, option);
		}

		public ReactiveStringCommands.SetCommand expiring(Expiration expiration) {
			return new SetCommand(getKey(), value, expiration, option);
		}

		public ReactiveStringCommands.SetCommand withSetOption(SetOption option) {
			return new SetCommand(getKey(), value, expiration, option);
		}

		public ByteBuffer getValue() {
			return value;
		}

		public Optional<Expiration> getExpiration() {
			return Optional.ofNullable(expiration);
		}

		public Optional<SetOption> getOption() {
			return Optional.ofNullable(option);
		}
	}

	/**
	 * Set {@literal value} for {@literal key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return
	 */
	default Mono<Boolean> set(ByteBuffer key, ByteBuffer value) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(value, "Value must not be null!");

		return set(Mono.just(SetCommand.set(key).value(value))).next().map(BooleanResponse::getOutput);
	}

	/**
	 * Set {@literal value} for {@literal key} with {@literal expiration} and {@literal options}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @param expiration must not be {@literal null}.
	 * @param option must not be {@literal null}.
	 * @return
	 */
	default Mono<Boolean> set(ByteBuffer key, ByteBuffer value, Expiration expiration, SetOption option) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(value, "Value must not be null!");

		return set(Mono.just(SetCommand.set(key).value(value).withSetOption(option).expiring(expiration))).next()
				.map(BooleanResponse::getOutput);
	}

	/**
	 * Set each and every {@link KeyValue} item separately.
	 *
	 * @param values must not be {@literal null}.
	 * @return {@link Flux} of {@link SetResponse} holding the {@link KeyValue} pair to set along with the command result.
	 */
	Flux<BooleanResponse<ReactiveStringCommands.SetCommand>> set(Publisher<ReactiveStringCommands.SetCommand> commands);

	/**
	 * Get single element stored at {@literal key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return empty {@link ByteBuffer} in case {@literal key} does not exist.
	 */
	default Mono<ByteBuffer> get(ByteBuffer key) {

		Assert.notNull(key, "Key must not be null!");

		return get(Mono.just(new KeyCommand(key))).next().map((result) -> result.getOutput());
	}

	/**
	 * Get elements one by one.
	 *
	 * @param keys must not be {@literal null}.
	 * @return {@link Flux} of {@link GetResponse} holding the {@literal key} to get along with the value retrieved.
	 */
	Flux<ByteBufferResponse<KeyCommand>> get(Publisher<KeyCommand> keys);

	/**
	 * Set {@literal value} for {@literal key} and return the existing value.
	 *
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return
	 */
	default Mono<ByteBuffer> getSet(ByteBuffer key, ByteBuffer value) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(value, "Value must not be null!");

		return getSet(Mono.just(SetCommand.set(key).value(value))).next().map(ByteBufferResponse::getOutput);
	}

	/**
	 * Set {@literal value} for {@literal key} and return the existing value one by one.
	 *
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return {@link Flux} of {@link GetSetResponse} holding the {@link KeyValue} pair to set along with the previously
	 *         existing value.
	 */
	Flux<ByteBufferResponse<ReactiveStringCommands.SetCommand>> getSet(
			Publisher<ReactiveStringCommands.SetCommand> command);

	/**
	 * Get multiple values in one batch.
	 *
	 * @param keys must not be {@literal null}.
	 * @return
	 */
	default Mono<List<ByteBuffer>> mGet(List<ByteBuffer> keys) {

		Assert.notNull(keys, "Keys must not be null!");

		return mGet(Mono.just(keys)).next().map(MultiValueResponse::getOutput);
	}

	/**
	 * Get multiple values at in batches.
	 *
	 * @param keys must not be {@literal null}.
	 * @return
	 */
	Flux<MultiValueResponse<List<ByteBuffer>, ByteBuffer>> mGet(Publisher<List<ByteBuffer>> keysets);

	/**
	 * Set {@code value} for {@code key}, only if {@code key} does not exist.
	 *
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return
	 */
	default Mono<Boolean> setNX(ByteBuffer key, ByteBuffer value) {

		Assert.notNull(key, "Keys must not be null!");
		Assert.notNull(value, "Keys must not be null!");

		return setNX(Mono.just(SetCommand.set(key).value(value))).next().map(BooleanResponse::getOutput);
	}

	/**
	 * Set {@code key value} pairs, only if {@code key} does not exist.
	 *
	 * @param values must not be {@literal null}.
	 * @return
	 */
	Flux<BooleanResponse<ReactiveStringCommands.SetCommand>> setNX(Publisher<ReactiveStringCommands.SetCommand> values);

	/**
	 * Set {@code key value} pair and {@link Expiration}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @param expireTimeout must not be {@literal null}.
	 * @return
	 */
	default Mono<Boolean> setEX(ByteBuffer key, ByteBuffer value, Expiration expireTimeout) {

		Assert.notNull(key, "Keys must not be null!");
		Assert.notNull(value, "Keys must not be null!");
		Assert.notNull(key, "ExpireTimeout must not be null!");

		return setEX(Mono.just(SetCommand.set(key).value(value).expiring(expireTimeout))).next()
				.map(BooleanResponse::getOutput);
	}

	/**
	 * Set {@code key value} pairs and {@link Expiration}.
	 *
	 * @param source must not be {@literal null}.
	 * @param expireTimeout must not be {@literal null}.
	 * @return
	 */
	Flux<BooleanResponse<ReactiveStringCommands.SetCommand>> setEX(Publisher<ReactiveStringCommands.SetCommand> command);

	/**
	 * Set {@code key value} pair and {@link Expiration}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @param expireTimeout must not be {@literal null}.
	 * @return
	 */
	default Mono<Boolean> pSetEX(ByteBuffer key, ByteBuffer value, Expiration expireTimeout) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(value, "Value must not be null!");
		Assert.notNull(key, "ExpireTimeout must not be null!");

		return pSetEX(Mono.just(SetCommand.set(key).value(value).expiring(expireTimeout))).next()
				.map(BooleanResponse::getOutput);
	}

	/**
	 * Set {@code key value} pairs and {@link Expiration}.
	 *
	 * @param source must not be {@literal null}.
	 * @param expireTimeout must not be {@literal null}.
	 * @return
	 */
	Flux<BooleanResponse<ReactiveStringCommands.SetCommand>> pSetEX(Publisher<ReactiveStringCommands.SetCommand> command);

	/**
	 * @author Christoph Strobl
	 */
	class MSetCommand implements Command {

		private Map<ByteBuffer, ByteBuffer> keyValuePairs;

		private MSetCommand(Map<ByteBuffer, ByteBuffer> keyValuePairs) {
			this.keyValuePairs = keyValuePairs;
		}

		@Override
		public ByteBuffer getKey() {
			return null;
		}

		public static ReactiveStringCommands.MSetCommand mset(Map<ByteBuffer, ByteBuffer> keyValuePairs) {
			return new MSetCommand(keyValuePairs);
		}

		public Map<ByteBuffer, ByteBuffer> getKeyValuePairs() {
			return keyValuePairs;
		}
	}

	/**
	 * Set multiple keys to multiple values using key-value pairs provided in {@code tuple}.
	 *
	 * @param tuples must not be {@literal null}.
	 * @return
	 */
	default Mono<Boolean> mSet(Map<ByteBuffer, ByteBuffer> tuples) {

		Assert.notNull(tuples, "Tuples must not be null!");

		return mSet(Mono.just(MSetCommand.mset(tuples))).next().map(BooleanResponse::getOutput);
	}

	/**
	 * Set multiple keys to multiple values using key-value pairs provided in {@code source}.
	 *
	 * @param source must not be {@literal null}.
	 * @return
	 */
	Flux<BooleanResponse<ReactiveStringCommands.MSetCommand>> mSet(Publisher<ReactiveStringCommands.MSetCommand> source);

	/**
	 * Set multiple keys to multiple values using key-value pairs provided in {@code tuples} only if the provided key does
	 * not exist.
	 *
	 * @param tuples must not be {@literal null}.
	 * @return
	 */
	default Mono<Boolean> mSetNX(Map<ByteBuffer, ByteBuffer> tuples) {

		Assert.notNull(tuples, "Tuples must not be null!");

		return mSetNX(Mono.just(MSetCommand.mset(tuples))).next().map(BooleanResponse::getOutput);
	}

	/**
	 * Set multiple keys to multiple values using key-value pairs provided in {@code tuples} only if the provided key does
	 * not exist.
	 *
	 * @param source must not be {@literal null}.
	 * @return
	 */
	Flux<BooleanResponse<ReactiveStringCommands.MSetCommand>> mSetNX(
			Publisher<ReactiveStringCommands.MSetCommand> source);

	/**
	 * @author Christoph Strobl
	 */
	class AppendCommand extends KeyCommand {

		private ByteBuffer value;

		private AppendCommand(ByteBuffer key, ByteBuffer value) {

			super(key);
			this.value = value;
		}

		public static ReactiveStringCommands.AppendCommand key(ByteBuffer key) {
			return new AppendCommand(key, null);
		}

		public ReactiveStringCommands.AppendCommand append(ByteBuffer value) {
			return new AppendCommand(getKey(), value);
		}

		public ByteBuffer getValue() {
			return value;
		}

	}

	/**
	 * Append a {@code value} to {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return
	 */
	default Mono<Long> append(ByteBuffer key, ByteBuffer value) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(value, "Value must not be null!");

		return append(Mono.just(AppendCommand.key(key).append(value))).next().map(NumericResponse::getOutput);
	}

	/**
	 * Append a {@link KeyValue#value} to {@link KeyValue#key}
	 *
	 * @param source must not be {@literal null}.
	 * @return
	 */
	Flux<NumericResponse<ReactiveStringCommands.AppendCommand, Long>> append(
			Publisher<ReactiveStringCommands.AppendCommand> source);

	/**
	 * Get a substring of value of {@code key} between {@code begin} and {@code end}.
	 *
	 * @param key must not be {@literal null}.
	 * @param begin
	 * @param end
	 * @return
	 */
	default Mono<ByteBuffer> getRange(ByteBuffer key, long begin, long end) {

		Assert.notNull(key, "Key must not be null!");

		return getRange(Mono.just(RangeCommand.key(key).fromIndex(begin).toIndex(end))).next()
				.map(ByteBufferResponse::getOutput);
	}

	/**
	 * Get a substring of value of {@code key} between {@code begin} and {@code end}.
	 *
	 * @param keys must not be {@literal null}.
	 * @param begin
	 * @param end
	 * @return
	 */
	Flux<ByteBufferResponse<RangeCommand>> getRange(Publisher<RangeCommand> commands);

	/**
	 * @author Christoph Strobl
	 */
	class SetRangeCommand extends KeyCommand {

		private ByteBuffer value;
		private Long offset;

		private SetRangeCommand(ByteBuffer key, ByteBuffer value, Long offset) {

			super(key);
			this.value = value;
			this.offset = offset;
		}

		public static ReactiveStringCommands.SetRangeCommand overwrite(ByteBuffer key) {
			return new SetRangeCommand(key, null, null);
		}

		public ReactiveStringCommands.SetRangeCommand withValue(ByteBuffer value) {
			return new SetRangeCommand(getKey(), value, offset);
		}

		public ReactiveStringCommands.SetRangeCommand atPosition(Long index) {
			return new SetRangeCommand(getKey(), value, index);
		}

		public ByteBuffer getValue() {
			return value;
		}

		public Long getOffset() {
			return offset;
		}
	}

	/**
	 * Overwrite parts of {@code key} starting at the specified {@code offset} with given {@code value}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @param offset
	 * @return
	 */
	default Mono<Long> setRange(ByteBuffer key, ByteBuffer value, long offset) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(value, "Value must not be null!");

		return setRange(Mono.just(SetRangeCommand.overwrite(key).withValue(value).atPosition(offset))).next()
				.map(NumericResponse::getOutput);
	}

	/**
	 * Overwrite parts of {@link KeyValue#key} starting at the specified {@code offset} with given {@link KeyValue#value}.
	 *
	 * @param keys must not be {@literal null}.
	 * @param offset must not be {@literal null}.
	 * @return
	 */
	Flux<NumericResponse<ReactiveStringCommands.SetRangeCommand, Long>> setRange(
			Publisher<ReactiveStringCommands.SetRangeCommand> commands);

	class GetBitCommand extends KeyCommand {

		public Long offset;

		public GetBitCommand(ByteBuffer key, Long offset) {

			super(key);
			this.offset = offset;
		}

		public static ReactiveStringCommands.GetBitCommand bit(ByteBuffer key) {
			return new GetBitCommand(key, null);
		}

		public ReactiveStringCommands.GetBitCommand atOffset(Long offset) {
			return new GetBitCommand(getKey(), offset);
		}

		public Long getOffset() {
			return offset;
		}
	}

	/**
	 * Get the bit value at {@code offset} of value at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param offset
	 * @return
	 */
	default Mono<Boolean> getBit(ByteBuffer key, long offset) {

		Assert.notNull(key, "Key must not be null!");

		return getBit(Mono.just(GetBitCommand.bit(key).atOffset(offset))).next().map(BooleanResponse::getOutput);
	}

	/**
	 * Get the bit value at {@code offset} of value at {@code key}.
	 *
	 * @param keys must not be {@literal null}.
	 * @param offset must not be {@literal null}.
	 * @return
	 */
	Flux<BooleanResponse<ReactiveStringCommands.GetBitCommand>> getBit(
			Publisher<ReactiveStringCommands.GetBitCommand> commands);

	class SetBitCommand extends KeyCommand {

		private Long offset;
		private Boolean value;

		private SetBitCommand(ByteBuffer key, Long offset, Boolean value) {

			super(key);
			this.offset = offset;
			this.value = value;
		}

		public static ReactiveStringCommands.SetBitCommand bit(ByteBuffer key) {
			return new SetBitCommand(key, null, null);
		}

		public ReactiveStringCommands.SetBitCommand atOffset(Long index) {
			return new ReactiveStringCommands.SetBitCommand(getKey(), index, value);
		}

		public ReactiveStringCommands.SetBitCommand to(Boolean bit) {
			return new ReactiveStringCommands.SetBitCommand(getKey(), offset, bit);
		}

		public Long getOffset() {
			return offset;
		}

		public Boolean getValue() {
			return value;
		}
	}

	/**
	 * Sets the bit at {@code offset} in value stored at {@code key} and return the original value.
	 *
	 * @param key must not be {@literal null}.
	 * @param offset
	 * @param value
	 * @return
	 */
	default Mono<Boolean> setBit(ByteBuffer key, long offset, boolean value) {

		Assert.notNull(key, "Key must not be null!");

		return setBit(Mono.just(SetBitCommand.bit(key).atOffset(offset).to(value))).next().map(BooleanResponse::getOutput);
	}

	/**
	 * Sets the bit at {@code offset} in value stored at {@code key} and return the original value.
	 *
	 * @param keys must not be {@literal null}.
	 * @param offset must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return
	 */
	Flux<BooleanResponse<ReactiveStringCommands.SetBitCommand>> setBit(
			Publisher<ReactiveStringCommands.SetBitCommand> commands);

	/**
	 * @author Christoph Strobl
	 */
	class BitCountCommand extends KeyCommand {

		private Range<Long> range;

		public BitCountCommand(ByteBuffer key, Range<Long> range) {

			super(key);
			this.range = range;
		}

		public static ReactiveStringCommands.BitCountCommand bitCount(ByteBuffer key) {
			return new ReactiveStringCommands.BitCountCommand(key, null);
		}

		public ReactiveStringCommands.BitCountCommand within(Range<Long> range) {
			return new ReactiveStringCommands.BitCountCommand(getKey(), range);
		}

		public Range<Long> getRange() {
			return range;
		}

	}

	/**
	 * Count the number of set bits (population counting) in value stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 */
	default Mono<Long> bitCount(ByteBuffer key) {

		Assert.notNull(key, "Key must not be null");

		return bitCount(Mono.just(BitCountCommand.bitCount(key))).next().map(NumericResponse::getOutput);
	}

	/**
	 * Count the number of set bits (population counting) of value stored at {@code key} between {@code begin} and
	 * {@code end}.
	 *
	 * @param key must not be {@literal null}.
	 * @param begin
	 * @param end
	 * @return
	 */
	default Mono<Long> bitCount(ByteBuffer key, long begin, long end) {

		Assert.notNull(key, "Key must not be null");

		return bitCount(Mono.just(BitCountCommand.bitCount(key).within(new Range<>(begin, end)))).next()
				.map(NumericResponse::getOutput);
	}

	/**
	 * Count the number of set bits (population counting) of value stored at {@code key} between {@code begin} and
	 * {@code end}.
	 *
	 * @param keys must not be {@literal null}.
	 * @param begin must not be {@literal null}.
	 * @param end must not be {@literal null}.
	 * @return
	 */
	Flux<NumericResponse<ReactiveStringCommands.BitCountCommand, Long>> bitCount(
			Publisher<ReactiveStringCommands.BitCountCommand> commands);

	/**
	 * @author Christoph Strobl
	 */
	class BitOpCommand {

		private List<ByteBuffer> keys;
		private BitOperation bitOp;
		private ByteBuffer destinationKey;

		private BitOpCommand(List<ByteBuffer> keys, BitOperation bitOp, ByteBuffer destinationKey) {

			this.keys = keys;
			this.bitOp = bitOp;
			this.destinationKey = destinationKey;
		}

		public static ReactiveStringCommands.BitOpCommand perform(BitOperation bitOp) {
			return new ReactiveStringCommands.BitOpCommand(null, bitOp, null);
		}

		public BitOperation getBitOp() {
			return bitOp;
		}

		public ReactiveStringCommands.BitOpCommand onKeys(List<ByteBuffer> keys) {
			return new ReactiveStringCommands.BitOpCommand(keys, bitOp, destinationKey);
		}

		public List<ByteBuffer> getKeys() {
			return keys;
		}

		public ReactiveStringCommands.BitOpCommand andSaveAs(ByteBuffer destinationKey) {
			return new ReactiveStringCommands.BitOpCommand(keys, bitOp, destinationKey);
		}

		public ByteBuffer getDestinationKey() {
			return destinationKey;
		}

	}

	/**
	 * Perform bitwise operations between strings.
	 *
	 * @param keys must not be {@literal null}.
	 * @param bitOp must not be {@literal null}.
	 * @param destination must not be {@literal null}.
	 * @return
	 */
	default Mono<Long> bitOp(List<ByteBuffer> keys, BitOperation bitOp, ByteBuffer destination) {

		Assert.notNull(keys, "keys must not be null");

		return bitOp(Mono.just(BitOpCommand.perform(bitOp).onKeys(keys).andSaveAs(destination))).next()
				.map(NumericResponse::getOutput);
	}

	/**
	 * Perform bitwise operations between strings.
	 *
	 * @param keys must not be {@literal null}.
	 * @param bitOp must not be {@literal null}.
	 * @param destination must not be {@literal null}.
	 * @return
	 */
	Flux<NumericResponse<ReactiveStringCommands.BitOpCommand, Long>> bitOp(
			Publisher<ReactiveStringCommands.BitOpCommand> commands);

	/**
	 * Get the length of the value stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 */
	default Mono<Long> strLen(ByteBuffer key) {

		Assert.notNull(key, "key must not be null");

		return strLen(Mono.just(new KeyCommand(key))).next().map(NumericResponse::getOutput);
	}

	/**
	 * Get the length of the value stored at {@code key}.
	 *
	 * @param keys must not be {@literal null}.
	 * @return
	 */
	Flux<NumericResponse<KeyCommand, Long>> strLen(Publisher<KeyCommand> keys);
}
