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
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.reactivestreams.Publisher;
import org.springframework.data.redis.connection.ReactiveRedisConnection.BooleanResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.ByteBufferResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.Command;
import org.springframework.data.redis.connection.ReactiveRedisConnection.KeyCommand;
import org.springframework.data.redis.connection.ReactiveRedisConnection.MultiValueResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.NumericResponse;
import org.springframework.util.Assert;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * @author Christoph Strobl
 * @since 2.0
 */
public interface ReactiveSetCommands {

	/**
	 * @author Christoph Strobl
	 */
	class SAddCommand extends KeyCommand {

		private List<ByteBuffer> values;

		private SAddCommand(ByteBuffer key, List<ByteBuffer> values) {

			super(key);
			this.values = values;
		}

		public static SAddCommand value(ByteBuffer values) {
			return values(Collections.singletonList(values));
		}

		public static SAddCommand values(List<ByteBuffer> values) {
			return new SAddCommand(null, values);
		}

		public SAddCommand to(ByteBuffer key) {
			return new SAddCommand(key, values);
		}

		public List<ByteBuffer> getValues() {
			return values;
		}
	}

	/**
	 * Add given {@code value} to set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return
	 */
	default Mono<Long> sAdd(ByteBuffer key, ByteBuffer value) {

		Assert.notNull(value, "value must not be null");

		return sAdd(key, Collections.singletonList(value));
	}

	/**
	 * Add given {@code values} to set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param values must not be {@literal null}.
	 * @return
	 */
	default Mono<Long> sAdd(ByteBuffer key, List<ByteBuffer> values) {

		Assert.notNull(key, "key must not be null");
		Assert.notNull(values, "values must not be null");

		return sAdd(Mono.just(SAddCommand.values(values).to(key))).next().map(NumericResponse::getOutput);
	}

	/**
	 * Add given {@link SAddCommand#getValues()} to set at {@link SAddCommand#getKey()}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 */
	Flux<NumericResponse<SAddCommand, Long>> sAdd(Publisher<SAddCommand> commands);

	/**
	 * @author Christoph Strobl
	 */
	class SRemCommand extends KeyCommand {

		private final List<ByteBuffer> values;

		public SRemCommand(ByteBuffer key, List<ByteBuffer> values) {

			super(key);
			this.values = values;
		}

		public static SRemCommand value(ByteBuffer values) {
			return values(Collections.singletonList(values));
		}

		public static SRemCommand values(List<ByteBuffer> values) {
			return new SRemCommand(null, values);
		}

		public SRemCommand from(ByteBuffer key) {
			return new SRemCommand(key, values);
		}

		public List<ByteBuffer> getValues() {
			return values;
		}
	}

	/**
	 * Remove given {@code value} from set at {@code key} and return the number of removed elements.
	 *
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return
	 */
	default Mono<Long> sRem(ByteBuffer key, ByteBuffer value) {

		Assert.notNull(value, "value must not be null");

		return sRem(key, Collections.singletonList(value));
	}

	/**
	 * Remove given {@code values} from set at {@code key} and return the number of removed elements.
	 *
	 * @param key must not be {@literal null}.
	 * @param values must not be {@literal null}.
	 * @return
	 */
	default Mono<Long> sRem(ByteBuffer key, List<ByteBuffer> values) {

		Assert.notNull(key, "key must not be null");
		Assert.notNull(values, "values must not be null");

		return sRem(Mono.just(SRemCommand.values(values).from(key))).next().map(NumericResponse::getOutput);
	}

	/**
	 * Remove given {@link SRemCommand#getValues()} from set at {@link SRemCommand#getKey()}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 */
	Flux<NumericResponse<SRemCommand, Long>> sRem(Publisher<SRemCommand> commands);

	/**
	 * Remove and return a random member from set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 */
	default Mono<ByteBuffer> sPop(ByteBuffer key) {

		Assert.notNull(key, "key must not be null");

		return sPop(Mono.just(new KeyCommand(key))).next().map(ByteBufferResponse::getOutput);
	}

	/**
	 * Remove and return a random member from set at {@link KeyCommand#getKey()}
	 *
	 * @param commands
	 * @return
	 */
	Flux<ByteBufferResponse<KeyCommand>> sPop(Publisher<KeyCommand> commands);

	/**
	 * @author Christoph Strobl
	 */
	class SMoveCommand extends KeyCommand {

		private final ByteBuffer destination;
		private final ByteBuffer value;

		private SMoveCommand(ByteBuffer key, ByteBuffer destination, ByteBuffer value) {

			super(key);
			this.destination = destination;
			this.value = value;
		}

		public static SMoveCommand value(ByteBuffer value) {
			return new SMoveCommand(null, null, value);
		}

		public SMoveCommand from(ByteBuffer source) {
			return new SMoveCommand(source, destination, value);
		}

		public SMoveCommand to(ByteBuffer destination) {
			return new SMoveCommand(getKey(), destination, value);
		}

		public ByteBuffer getDestination() {
			return destination;
		}

		public ByteBuffer getValue() {
			return value;
		}
	}

	/**
	 * Move {@code value} from {@code sourceKey} to {@code destinationKey}
	 *
	 * @param sourceKey must not be {@literal null}.
	 * @param destinationKey must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return
	 */
	default Mono<Boolean> sMove(ByteBuffer sourceKey, ByteBuffer destinationKey, ByteBuffer value) {

		Assert.notNull(sourceKey, "sourceKey must not be null");
		Assert.notNull(destinationKey, "destinationKey must not be null");
		Assert.notNull(value, "value must not be null");

		return sMove(Mono.just(SMoveCommand.value(value).from(sourceKey).to(destinationKey))).next()
				.map(BooleanResponse::getOutput);
	}

	/**
	 * Move {@link SMoveCommand#getValue()} from {@link SMoveCommand#getKey()} to {@link SMoveCommand#getDestination()}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 */
	Flux<BooleanResponse<SMoveCommand>> sMove(Publisher<SMoveCommand> commands);

	/**
	 * Get size of set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 */
	default Mono<Long> sCard(ByteBuffer key) {

		Assert.notNull(key, "key must not be null");

		return sCard(Mono.just(new KeyCommand(key))).next().map(NumericResponse::getOutput);
	}

	/**
	 * Get size of set at {@link KeyCommand#getKey()}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 */
	Flux<NumericResponse<KeyCommand, Long>> sCard(Publisher<KeyCommand> commands);

	/**
	 * @author Christoph Strobl
	 */
	class SIsMemberCommand extends KeyCommand {

		private final ByteBuffer value;

		private SIsMemberCommand(ByteBuffer key, ByteBuffer value) {

			super(key);
			this.value = value;
		}

		public static SIsMemberCommand value(ByteBuffer value) {
			return new SIsMemberCommand(null, value);
		}

		public SIsMemberCommand of(ByteBuffer set) {
			return new SIsMemberCommand(set, value);
		}

		public ByteBuffer getValue() {
			return value;
		}
	}

	/**
	 * Check if set at {@code key} contains {@code value}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return
	 */
	default Mono<Boolean> sIsMember(ByteBuffer key, ByteBuffer value) {

		Assert.notNull(key, "key must not be null");
		Assert.notNull(value, "value must not be null");

		return sIsMember(Mono.just(SIsMemberCommand.value(value).of(key))).next().map(BooleanResponse::getOutput);
	}

	/**
	 * Check if set at {@link SIsMemberCommand#getKey()} contains {@link SIsMemberCommand#getKey()}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 */
	Flux<BooleanResponse<SIsMemberCommand>> sIsMember(Publisher<SIsMemberCommand> commands);

	/**
	 * @author Christoph Strobl
	 */
	class SInterCommand implements Command {

		private final List<ByteBuffer> keys;

		private SInterCommand(List<ByteBuffer> keys) {
			this.keys = keys;
		}

		public static SInterCommand keys(List<ByteBuffer> keys) {
			return new SInterCommand(keys);
		}

		@Override
		public ByteBuffer getKey() {
			return null;
		}

		public List<ByteBuffer> getKeys() {
			return keys;
		}
	}

	/**
	 * Returns the members intersecting all given sets at {@code keys}.
	 *
	 * @param keys must not be {@literal null}.
	 * @return
	 */
	default Mono<List<ByteBuffer>> sInter(List<ByteBuffer> keys) {

		Assert.notNull(keys, "keys must not be null");

		return sInter(Mono.just(SInterCommand.keys(keys))).next().map(MultiValueResponse::getOutput);
	}

	/**
	 * Returns the members intersecting all given sets at {@link SInterCommand#getKeys()}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 */
	Flux<MultiValueResponse<SInterCommand, ByteBuffer>> sInter(Publisher<SInterCommand> commands);

	/**
	 * @author Christoph Strobl
	 */
	class SInterStoreCommand extends KeyCommand {

		private final List<ByteBuffer> keys;

		private SInterStoreCommand(ByteBuffer key, List<ByteBuffer> keys) {

			super(key);
			this.keys = keys;
		}

		public static SInterStoreCommand keys(List<ByteBuffer> keys) {
			return new SInterStoreCommand(null, keys);
		}

		public SInterStoreCommand storeAt(ByteBuffer key) {
			return new SInterStoreCommand(key, keys);
		}

		public List<ByteBuffer> getKeys() {
			return keys;
		}
	}

	/**
	 * Intersect all given sets at {@code keys} and store result in {@code destinationKey}.
	 *
	 * @param destinationKey must not be {@literal null}.
	 * @param keys must not be {@literal null}.
	 * @return size of set stored a {@code destinationKey}.
	 */
	default Mono<Long> sInterStore(ByteBuffer destinationKey, List<ByteBuffer> keys) {

		Assert.notNull(destinationKey, "destinationKey must not be null");
		Assert.notNull(keys, "keys must not be null");

		return sInterStore(Mono.just(SInterStoreCommand.keys(keys).storeAt(destinationKey))).next()
				.map(NumericResponse::getOutput);
	}

	/**
	 * Intersect all given sets at {@code keys} and store result in {@code destinationKey}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 */
	Flux<NumericResponse<SInterStoreCommand, Long>> sInterStore(Publisher<SInterStoreCommand> commands);

	/**
	 * @author Christoph Strobl
	 */
	class SUnionCommand implements Command {

		private final List<ByteBuffer> keys;

		private SUnionCommand(List<ByteBuffer> keys) {
			this.keys = keys;
		}

		public static SUnionCommand keys(List<ByteBuffer> keys) {
			return new SUnionCommand(keys);
		}

		@Override
		public ByteBuffer getKey() {
			return null;
		}

		public List<ByteBuffer> getKeys() {
			return keys;
		}
	}

	/**
	 * Returns the members intersecting all given sets at {@code keys}.
	 *
	 * @param keys must not be {@literal null}.
	 * @return
	 */
	default Mono<List<ByteBuffer>> sUnion(List<ByteBuffer> keys) {

		Assert.notNull(keys, "keys must not be null");

		return sUnion(Mono.just(SUnionCommand.keys(keys))).next().map(MultiValueResponse::getOutput);
	}

	/**
	 * Returns the members intersecting all given sets at {@link SInterCommand#getKeys()}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 */
	Flux<MultiValueResponse<SUnionCommand, ByteBuffer>> sUnion(Publisher<SUnionCommand> commands);

	/**
	 * @author Christoph Strobl
	 */
	class SUnionStoreCommand extends KeyCommand {

		private final List<ByteBuffer> keys;

		private SUnionStoreCommand(ByteBuffer key, List<ByteBuffer> keys) {

			super(key);
			this.keys = keys;
		}

		public static SUnionStoreCommand keys(List<ByteBuffer> keys) {
			return new SUnionStoreCommand(null, keys);
		}

		public SUnionStoreCommand storeAt(ByteBuffer key) {
			return new SUnionStoreCommand(key, keys);
		}

		public List<ByteBuffer> getKeys() {
			return keys;
		}
	}

	/**
	 * Union all given sets at {@code keys} and store result in {@code destinationKey}.
	 *
	 * @param destinationKey must not be {@literal null}.
	 * @param keys must not be {@literal null}.
	 * @return size of set stored a {@code destinationKey}.
	 */
	default Mono<Long> sUnionStore(ByteBuffer destinationKey, List<ByteBuffer> keys) {

		Assert.notNull(destinationKey, "destinationKey must not be null");
		Assert.notNull(keys, "keys must not be null");

		return sUnionStore(Mono.just(SUnionStoreCommand.keys(keys).storeAt(destinationKey))).next()
				.map(NumericResponse::getOutput);
	}

	/**
	 * Union all given sets at {@code keys} and store result in {@code destinationKey}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 */
	Flux<NumericResponse<SUnionStoreCommand, Long>> sUnionStore(Publisher<SUnionStoreCommand> commands);

	/**
	 * @author Christoph Strobl
	 */
	class SDiffCommand implements Command {

		private final List<ByteBuffer> keys;

		private SDiffCommand(List<ByteBuffer> keys) {
			this.keys = keys;
		}

		public static SDiffCommand keys(List<ByteBuffer> keys) {
			return new SDiffCommand(keys);
		}

		@Override
		public ByteBuffer getKey() {
			return null;
		}

		public List<ByteBuffer> getKeys() {
			return keys;
		}
	}

	/**
	 * Returns the diff of the members of all given sets at {@code keys}.
	 *
	 * @param keys must not be {@literal null}.
	 * @return
	 */
	default Mono<List<ByteBuffer>> sDiff(List<ByteBuffer> keys) {

		Assert.notNull(keys, "keys must not be null");

		return sDiff(Mono.just(SDiffCommand.keys(keys))).next().map(MultiValueResponse::getOutput);
	}

	/**
	 * Returns the diff of the members of all given sets at {@link SInterCommand#getKeys()}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 */
	Flux<MultiValueResponse<SDiffCommand, ByteBuffer>> sDiff(Publisher<SDiffCommand> commands);

	/**
	 * @author Christoph Strobl
	 */
	class SDiffStoreCommand extends KeyCommand {

		private final List<ByteBuffer> keys;

		private SDiffStoreCommand(ByteBuffer key, List<ByteBuffer> keys) {

			super(key);
			this.keys = keys;
		}

		public static SDiffStoreCommand keys(List<ByteBuffer> keys) {
			return new SDiffStoreCommand(null, keys);
		}

		public SDiffStoreCommand storeAt(ByteBuffer key) {
			return new SDiffStoreCommand(key, keys);
		}

		public List<ByteBuffer> getKeys() {
			return keys;
		}
	}

	/**
	 * Diff all given sets at {@code keys} and store result in {@code destinationKey}.
	 *
	 * @param destinationKey must not be {@literal null}.
	 * @param keys must not be {@literal null}.
	 * @return size of set stored a {@code destinationKey}.
	 */
	default Mono<Long> sDiffStore(ByteBuffer destinationKey, List<ByteBuffer> keys) {

		Assert.notNull(destinationKey, "destinationKey must not be null");
		Assert.notNull(keys, "keys must not be null");

		return sDiffStore(Mono.just(SDiffStoreCommand.keys(keys).storeAt(destinationKey))).next()
				.map(NumericResponse::getOutput);
	}

	/**
	 * Diff all given sets at {@code keys} and store result in {@code destinationKey}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 */
	Flux<NumericResponse<SDiffStoreCommand, Long>> sDiffStore(Publisher<SDiffStoreCommand> commands);

	/**
	 * Get all elements of set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 */
	default Mono<List<ByteBuffer>> sMembers(ByteBuffer key) {

		Assert.notNull(key, "key must not be null");

		return sMembers(Mono.just(new KeyCommand(key))).next().map(MultiValueResponse::getOutput);
	}

	/**
	 * Get all elements of set at {@link KeyCommand#getKey()}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 */
	Flux<MultiValueResponse<KeyCommand, ByteBuffer>> sMembers(Publisher<KeyCommand> commands);

	/**
	 * @author Christoph Strobl
	 */
	class SRandMembersCommand extends KeyCommand {

		private final Long count;

		private SRandMembersCommand(ByteBuffer key, Long count) {

			super(key);
			this.count = count;
		}

		public static SRandMembersCommand valueCount(Long nrValuesToRetrieve) {
			return new SRandMembersCommand(null, nrValuesToRetrieve);
		}

		public static SRandMembersCommand singleValue() {
			return new SRandMembersCommand(null, null);
		}

		public SRandMembersCommand from(ByteBuffer key) {
			return new SRandMembersCommand(key, count);
		}

		public Optional<Long> getCount() {
			return Optional.ofNullable(count);
		}
	}

	/**
	 * Get random element from set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 */
	default Mono<ByteBuffer> sRandMember(ByteBuffer key) {
		return sRandMember(key, 1L).map(vals -> vals.isEmpty() ? null : vals.iterator().next());
	}

	/**
	 * Get {@code count} random elements from set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param count must not be {@literal null}.
	 * @return
	 */
	default Mono<List<ByteBuffer>> sRandMember(ByteBuffer key, Long count) {

		Assert.notNull(key, "key must not be null");
		Assert.notNull(count, "count must not be null");

		return sRandMember(Mono.just(SRandMembersCommand.valueCount(count).from(key))).next()
				.map(MultiValueResponse::getOutput);
	}

	/**
	 * Get {@link SRandMembersCommand#getCount()} random elements from set at {@link SRandMembersCommand#getKey()}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 */
	Flux<MultiValueResponse<SRandMembersCommand, ByteBuffer>> sRandMember(Publisher<SRandMembersCommand> commands);
}
