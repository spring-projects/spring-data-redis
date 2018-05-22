/*
 * Copyright 2016-2018 the original author or authors.
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

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.reactivestreams.Publisher;
import org.springframework.data.redis.connection.ReactiveRedisConnection.BooleanResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.ByteBufferResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.Command;
import org.springframework.data.redis.connection.ReactiveRedisConnection.CommandResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.KeyCommand;
import org.springframework.data.redis.connection.ReactiveRedisConnection.KeyScanCommand;
import org.springframework.data.redis.connection.ReactiveRedisConnection.NumericResponse;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Redis Set commands executed using reactive infrastructure.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.0
 */
public interface ReactiveSetCommands {

	/**
	 * {@code SADD} command parameters.
	 *
	 * @author Christoph Strobl
	 * @see <a href="http://redis.io/commands/sadd">Redis Documentation: SADD</a>
	 */
	class SAddCommand extends KeyCommand {

		private List<ByteBuffer> values;

		private SAddCommand(@Nullable ByteBuffer key, List<ByteBuffer> values) {

			super(key);

			this.values = values;
		}

		/**
		 * Creates a new {@link SAddCommand} given a {@literal value}.
		 *
		 * @param value must not be {@literal null}.
		 * @return a new {@link SAddCommand} for a {@literal value}.
		 */
		public static SAddCommand value(ByteBuffer value) {

			Assert.notNull(value, "Value must not be null!");

			return values(Collections.singletonList(value));
		}

		/**
		 * Creates a new {@link SAddCommand} given a {@link Collection} of values.
		 *
		 * @param values must not be {@literal null}.
		 * @return a new {@link SAddCommand} for a {@link Collection} of values.
		 */
		public static SAddCommand values(Collection<ByteBuffer> values) {

			Assert.notNull(values, "Values must not be null!");

			return new SAddCommand(null, new ArrayList<>(values));
		}

		/**
		 * Applies the {@literal key}. Constructs a new command instance with all previously configured properties.
		 *
		 * @param key must not be {@literal null}.
		 * @return a new {@link SAddCommand} with {@literal key} applied.
		 */
		public SAddCommand to(ByteBuffer key) {

			Assert.notNull(key, "Key must not be null!");

			return new SAddCommand(key, values);
		}

		/**
		 * @return
		 */
		public List<ByteBuffer> getValues() {
			return values;
		}
	}

	/**
	 * Add given {@literal value} to set at {@literal key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/sadd">Redis Documentation: SADD</a>
	 */
	default Mono<Long> sAdd(ByteBuffer key, ByteBuffer value) {

		Assert.notNull(value, "Value must not be null!");

		return sAdd(key, Collections.singletonList(value));
	}

	/**
	 * Add given {@literal values} to set at {@literal key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param values must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/sadd">Redis Documentation: SADD</a>
	 */
	default Mono<Long> sAdd(ByteBuffer key, Collection<ByteBuffer> values) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(values, "Values must not be null!");

		return sAdd(Mono.just(SAddCommand.values(values).to(key))).next().map(NumericResponse::getOutput);
	}

	/**
	 * Add given {@link SAddCommand#getValues()} to set at {@link SAddCommand#getKey()}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/sadd">Redis Documentation: SADD</a>
	 */
	Flux<NumericResponse<SAddCommand, Long>> sAdd(Publisher<SAddCommand> commands);

	/**
	 * {@code SREM} command parameters.
	 *
	 * @author Christoph Strobl
	 * @see <a href="http://redis.io/commands/srem">Redis Documentation: SREM</a>
	 */
	class SRemCommand extends KeyCommand {

		private final List<ByteBuffer> values;

		private SRemCommand(@Nullable ByteBuffer key, List<ByteBuffer> values) {

			super(key);

			this.values = values;
		}

		/**
		 * Creates a new {@link SRemCommand} given a {@literal value}.
		 *
		 * @param value must not be {@literal null}.
		 * @return a new {@link SRemCommand} for a {@literal value}.
		 */
		public static SRemCommand value(ByteBuffer value) {

			Assert.notNull(value, "Value must not be null!");

			return values(Collections.singletonList(value));
		}

		/**
		 * Creates a new {@link SRemCommand} given a {@link Collection} of values.
		 *
		 * @param values must not be {@literal null}.
		 * @return a new {@link SRemCommand} for a {@link Collection} of values.
		 */
		public static SRemCommand values(Collection<ByteBuffer> values) {

			Assert.notNull(values, "Values must not be null!");

			return new SRemCommand(null, new ArrayList<>(values));
		}

		/**
		 * Applies the {@literal key}. Constructs a new command instance with all previously configured properties.
		 *
		 * @param key must not be {@literal null}.
		 * @return a new {@link SRemCommand} with {@literal key} applied.
		 */
		public SRemCommand from(ByteBuffer key) {

			Assert.notNull(key, "Key must not be null!");

			return new SRemCommand(key, values);
		}

		/**
		 * @return never {@literal null}.
		 */
		public List<ByteBuffer> getValues() {
			return values;
		}
	}

	/**
	 * Remove given {@literal value} from set at {@literal key} and return the number of removed elements.
	 *
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/srem">Redis Documentation: SREM</a>
	 */
	default Mono<Long> sRem(ByteBuffer key, ByteBuffer value) {

		Assert.notNull(value, "Value must not be null!");

		return sRem(key, Collections.singletonList(value));
	}

	/**
	 * Remove given {@literal values} from set at {@literal key} and return the number of removed elements.
	 *
	 * @param key must not be {@literal null}.
	 * @param values must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/srem">Redis Documentation: SREM</a>
	 */
	default Mono<Long> sRem(ByteBuffer key, Collection<ByteBuffer> values) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(values, "Values must not be null!");

		return sRem(Mono.just(SRemCommand.values(values).from(key))).next().map(NumericResponse::getOutput);
	}

	/**
	 * Remove given {@link SRemCommand#getValues()} from set at {@link SRemCommand#getKey()}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/srem">Redis Documentation: SREM</a>
	 */
	Flux<NumericResponse<SRemCommand, Long>> sRem(Publisher<SRemCommand> commands);

	/**
	 * {@code SPOP} command parameters.
	 *
	 * @author Christoph Strobl
	 * @see <a href="http://redis.io/commands/spop">Redis Documentation: SPOP</a>
	 */
	class SPopCommand extends KeyCommand {

		private final long count;

		private SPopCommand(@Nullable ByteBuffer key, long count) {

			super(key);
			this.count = count;
		}

		/**
		 * Creates a new {@link SPopCommand} for a single member.
		 *
		 * @return a new {@link SPopCommand} for a single member.
		 */
		public static SPopCommand one() {
			return new SPopCommand(null, 1L);
		}

		/**
		 * Creates a new {@link SPopCommand} for {@code count} members.
		 *
		 * @return a new {@link SPopCommand} for {@code count} members.
		 */
		public static SPopCommand members(long count) {
			return new SPopCommand(null, count);
		}

		/**
		 * Applies the {@literal key}. Constructs a new command instance with all previously configured properties.
		 *
		 * @param key must not be {@literal null}.
		 * @return a new {@link SRemCommand} with {@literal key} applied.
		 */
		public SPopCommand from(ByteBuffer key) {

			Assert.notNull(key, "Key must not be null!");

			return new SPopCommand(key, count);
		}

		public long getCount() {
			return count;
		}
	}

	/**
	 * Remove and return a random member from set at {@literal key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/spop">Redis Documentation: SPOP</a>
	 */
	default Mono<ByteBuffer> sPop(ByteBuffer key) {

		Assert.notNull(key, "Key must not be null!");

		return sPop(Mono.just(SPopCommand.one().from(key))).next().map(ByteBufferResponse::getOutput);
	}

	/**
	 * Remove and return {@code count} random members from set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param count number of random members to pop from the set.
	 * @return
	 * @see <a href="http://redis.io/commands/spop">Redis Documentation: SPOP</a>
	 */
	default Flux<ByteBuffer> sPop(ByteBuffer key, long count) {

		Assert.notNull(key, "Key must not be null!");

		return sPop(SPopCommand.members(count).from(key));
	}

	/**
	 * Remove and return a random member from set at {@literal key}.
	 *
	 * @param command must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/spop">Redis Documentation: SPOP</a>
	 */
	Flux<ByteBuffer> sPop(SPopCommand command);

	/**
	 * Remove and return a random member from set at {@link KeyCommand#getKey()}
	 *
	 * @param commands
	 * @return
	 * @see <a href="http://redis.io/commands/spop">Redis Documentation: SPOP</a>
	 */
	Flux<ByteBufferResponse<KeyCommand>> sPop(Publisher<KeyCommand> commands);

	/**
	 * {@code SMOVE} command parameters.
	 *
	 * @author Christoph Strobl
	 * @see <a href="http://redis.io/commands/smove">Redis Documentation: SMOVE</a>
	 */
	class SMoveCommand extends KeyCommand {

		private final @Nullable ByteBuffer destination;
		private final ByteBuffer value;

		private SMoveCommand(@Nullable ByteBuffer key, @Nullable ByteBuffer destination, ByteBuffer value) {

			super(key);
			this.destination = destination;
			this.value = value;
		}

		/**
		 * Creates a new {@link SMoveCommand} given a {@literal value}.
		 *
		 * @param value must not be {@literal null}.
		 * @return a new {@link SMoveCommand} for a {@literal value}.
		 */
		public static SMoveCommand value(ByteBuffer value) {

			Assert.notNull(value, "Value must not be null!");

			return new SMoveCommand(null, null, value);
		}

		/**
		 * Applies the {@literal source} key. Constructs a new command instance with all previously configured properties.
		 *
		 * @param source must not be {@literal null}.
		 * @return a new {@link SMoveCommand} with {@literal source} applied.
		 */
		public SMoveCommand from(ByteBuffer source) {

			Assert.notNull(source, "Source key must not be null!");

			return new SMoveCommand(source, destination, value);
		}

		/**
		 * Applies the {@literal destination} key. Constructs a new command instance with all previously configured
		 * properties.
		 *
		 * @param destination must not be {@literal null}.
		 * @return a new {@link SMoveCommand} with {@literal destination} applied.
		 */
		public SMoveCommand to(ByteBuffer destination) {

			Assert.notNull(destination, "Destination key must not be null!");

			return new SMoveCommand(getKey(), destination, value);
		}

		/**
		 * @return can be {@literal null}.
		 */
		@Nullable
		public ByteBuffer getDestination() {
			return destination;
		}

		/**
		 * @return never {@literal null}.
		 */
		public ByteBuffer getValue() {
			return value;
		}
	}

	/**
	 * Move {@literal value} from {@literal sourceKey} to {@literal destinationKey}
	 *
	 * @param sourceKey must not be {@literal null}.
	 * @param destinationKey must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/smove">Redis Documentation: SMOVE</a>
	 */
	default Mono<Boolean> sMove(ByteBuffer sourceKey, ByteBuffer destinationKey, ByteBuffer value) {

		Assert.notNull(sourceKey, "SourceKey must not be null!");
		Assert.notNull(destinationKey, "DestinationKey must not be null!");
		Assert.notNull(value, "Value must not be null!");

		return sMove(Mono.just(SMoveCommand.value(value).from(sourceKey).to(destinationKey))).next()
				.map(BooleanResponse::getOutput);
	}

	/**
	 * Move {@link SMoveCommand#getValue()} from {@link SMoveCommand#getKey()} to {@link SMoveCommand#getDestination()}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/smove">Redis Documentation: SMOVE</a>
	 */
	Flux<BooleanResponse<SMoveCommand>> sMove(Publisher<SMoveCommand> commands);

	/**
	 * Get size of set at {@literal key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/scard">Redis Documentation: SCARD</a>
	 */
	default Mono<Long> sCard(ByteBuffer key) {

		Assert.notNull(key, "Key must not be null!");

		return sCard(Mono.just(new KeyCommand(key))).next().map(NumericResponse::getOutput);
	}

	/**
	 * Get size of set at {@link KeyCommand#getKey()}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/scard">Redis Documentation: SCARD</a>
	 */
	Flux<NumericResponse<KeyCommand, Long>> sCard(Publisher<KeyCommand> commands);

	/**
	 * {@code SISMEMBER} command parameters.
	 *
	 * @author Christoph Strobl
	 * @see <a href="http://redis.io/commands/sismember">Redis Documentation: SISMEMBER</a>
	 */
	class SIsMemberCommand extends KeyCommand {

		private final ByteBuffer value;

		private SIsMemberCommand(@Nullable ByteBuffer key, ByteBuffer value) {

			super(key);

			this.value = value;
		}

		/**
		 * Creates a new {@link SIsMemberCommand} given a {@literal value}.
		 *
		 * @param value must not be {@literal null}.
		 * @return a new {@link SIsMemberCommand} for a {@literal value}.
		 */
		public static SIsMemberCommand value(ByteBuffer value) {

			Assert.notNull(value, "Value must not be null!");

			return new SIsMemberCommand(null, value);
		}

		/**
		 * Applies the {@literal set} key. Constructs a new command instance with all previously configured properties.
		 *
		 * @param set must not be {@literal null}.
		 * @return a new {@link SIsMemberCommand} with {@literal set} applied.
		 */
		public SIsMemberCommand of(ByteBuffer set) {

			Assert.notNull(set, "Set key must not be null!");

			return new SIsMemberCommand(set, value);
		}

		/**
		 * @return
		 */
		public ByteBuffer getValue() {
			return value;
		}
	}

	/**
	 * Check if set at {@literal key} contains {@literal value}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/sismember">Redis Documentation: SISMEMBER</a>
	 */
	default Mono<Boolean> sIsMember(ByteBuffer key, ByteBuffer value) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(value, "Value must not be null!");

		return sIsMember(Mono.just(SIsMemberCommand.value(value).of(key))).next().map(BooleanResponse::getOutput);
	}

	/**
	 * Check if set at {@link SIsMemberCommand#getKey()} contains {@link SIsMemberCommand#getKey()}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/sismember">Redis Documentation: SISMEMBER</a>
	 */
	Flux<BooleanResponse<SIsMemberCommand>> sIsMember(Publisher<SIsMemberCommand> commands);

	/**
	 * {@code SINTER} command parameters.
	 *
	 * @author Christoph Strobl
	 * @see <a href="http://redis.io/commands/sinter">Redis Documentation: SINTER</a>
	 */
	class SInterCommand implements Command {

		private final List<ByteBuffer> keys;

		private SInterCommand(List<ByteBuffer> keys) {
			this.keys = keys;
		}

		/**
		 * Creates a new {@link SInterCommand} given a {@link Collection} of keys.
		 *
		 * @param keys must not be {@literal null}.
		 * @return a new {@link SInterCommand} for a {@link Collection} of values.
		 */
		public static SInterCommand keys(Collection<ByteBuffer> keys) {

			Assert.notNull(keys, "Keys must not be null!");

			return new SInterCommand(new ArrayList<>(keys));
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.connection.ReactiveRedisConnection.Command#getKey()
		 */
		@Override
		@Nullable
		public ByteBuffer getKey() {
			return null;
		}

		/**
		 * @return
		 */
		public List<ByteBuffer> getKeys() {
			return keys;
		}
	}

	/**
	 * Returns the members intersecting all given sets at {@literal keys}.
	 *
	 * @param keys must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/sinter">Redis Documentation: SINTER</a>
	 */
	default Flux<ByteBuffer> sInter(Collection<ByteBuffer> keys) {

		Assert.notNull(keys, "Keys must not be null!");

		return sInter(Mono.just(SInterCommand.keys(keys))).flatMap(CommandResponse::getOutput);
	}

	/**
	 * Returns the members intersecting all given sets at {@link SInterCommand#getKeys()}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/sinter">Redis Documentation: SINTER</a>
	 */
	Flux<CommandResponse<SInterCommand, Flux<ByteBuffer>>> sInter(Publisher<SInterCommand> commands);

	/**
	 * {@code SINTERSTORE} command parameters.
	 *
	 * @author Christoph Strobl
	 * @see <a href="http://redis.io/commands/sinterstore">Redis Documentation: SINTERSTORE</a>
	 */
	class SInterStoreCommand extends KeyCommand {

		private final List<ByteBuffer> keys;

		private SInterStoreCommand(@Nullable ByteBuffer key, List<ByteBuffer> keys) {

			super(key);

			this.keys = keys;
		}

		/**
		 * Creates a new {@link SInterStoreCommand} given a {@link Collection} of keys.
		 *
		 * @param keys must not be {@literal null}.
		 * @return a new {@link SInterStoreCommand} for a {@link Collection} of values.
		 */
		public static SInterStoreCommand keys(Collection<ByteBuffer> keys) {

			Assert.notNull(keys, "Keys must not be null!");

			return new SInterStoreCommand(null, new ArrayList<>(keys));
		}

		/**
		 * Applies the {@literal key} at which the result is stored. Constructs a new command instance with all previously
		 * configured properties.
		 *
		 * @param key must not be {@literal null}.
		 * @return a new {@link SInterStoreCommand} with {@literal key} applied.
		 */
		public SInterStoreCommand storeAt(ByteBuffer key) {

			Assert.notNull(key, "Key must not be null!");

			return new SInterStoreCommand(key, keys);
		}

		/**
		 * @return
		 */
		public List<ByteBuffer> getKeys() {
			return keys;
		}
	}

	/**
	 * Intersect all given sets at {@literal keys} and store result in {@literal destinationKey}.
	 *
	 * @param destinationKey must not be {@literal null}.
	 * @param keys must not be {@literal null}.
	 * @return size of set stored a {@literal destinationKey}.
	 * @see <a href="http://redis.io/commands/sinterstore">Redis Documentation: SINTERSTORE</a>
	 */
	default Mono<Long> sInterStore(ByteBuffer destinationKey, Collection<ByteBuffer> keys) {

		Assert.notNull(destinationKey, "DestinationKey must not be null!");
		Assert.notNull(keys, "Keys must not be null!");

		return sInterStore(Mono.just(SInterStoreCommand.keys(keys).storeAt(destinationKey))).next()
				.map(NumericResponse::getOutput);
	}

	/**
	 * Intersect all given sets at {@literal keys} and store result in {@literal destinationKey}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/sinterstore">Redis Documentation: SINTERSTORE</a>
	 */
	Flux<NumericResponse<SInterStoreCommand, Long>> sInterStore(Publisher<SInterStoreCommand> commands);

	/**
	 * {@code SUNION} command parameters.
	 *
	 * @author Christoph Strobl
	 * @see <a href="http://redis.io/commands/sunion">Redis Documentation: SUNION</a>
	 */
	class SUnionCommand implements Command {

		private final List<ByteBuffer> keys;

		private SUnionCommand(List<ByteBuffer> keys) {
			this.keys = keys;
		}

		/**
		 * Creates a new {@link SUnionCommand} given a {@link Collection} of keys.
		 *
		 * @param keys must not be {@literal null}.
		 * @return a new {@link SUnionCommand} for a {@link Collection} of values.
		 */
		public static SUnionCommand keys(Collection<ByteBuffer> keys) {

			Assert.notNull(keys, "Keys must not be null!");

			return new SUnionCommand(new ArrayList<>(keys));
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.connection.ReactiveRedisConnection.Command#getKey()
		 */
		@Override
		@Nullable
		public ByteBuffer getKey() {
			return null;
		}

		/**
		 * @return
		 */
		public List<ByteBuffer> getKeys() {
			return keys;
		}
	}

	/**
	 * Returns the members intersecting all given sets at {@literal keys}.
	 *
	 * @param keys must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/sunion">Redis Documentation: SUNION</a>
	 */
	default Flux<ByteBuffer> sUnion(Collection<ByteBuffer> keys) {

		Assert.notNull(keys, "Keys must not be null!");

		return sUnion(Mono.just(SUnionCommand.keys(keys))).flatMap(CommandResponse::getOutput);
	}

	/**
	 * Returns the members intersecting all given sets at {@link SInterCommand#getKeys()}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/sunion">Redis Documentation: SUNION</a>
	 */
	Flux<CommandResponse<SUnionCommand, Flux<ByteBuffer>>> sUnion(Publisher<SUnionCommand> commands);

	/**
	 * {@code SUNIONSTORE} command parameters.
	 *
	 * @author Christoph Strobl
	 * @see <a href="http://redis.io/commands/sunionstore">Redis Documentation: SUNIONSTORE</a>
	 */
	class SUnionStoreCommand extends KeyCommand {

		private final List<ByteBuffer> keys;

		private SUnionStoreCommand(@Nullable ByteBuffer key, List<ByteBuffer> keys) {

			super(key);

			this.keys = keys;
		}

		/**
		 * Creates a new {@link SUnionStoreCommand} given a {@link Collection} of keys.
		 *
		 * @param keys must not be {@literal null}.
		 * @return a new {@link SUnionStoreCommand} for a {@link Collection} of values.
		 */
		public static SUnionStoreCommand keys(Collection<ByteBuffer> keys) {

			Assert.notNull(keys, "Keys must not be null!");

			return new SUnionStoreCommand(null, new ArrayList<>(keys));
		}

		/**
		 * Applies the {@literal key} at which the result is stored. Constructs a new command instance with all previously
		 * configured properties.
		 *
		 * @param key must not be {@literal null}.
		 * @return a new {@link SUnionStoreCommand} with {@literal key} applied.
		 */
		public SUnionStoreCommand storeAt(ByteBuffer key) {

			Assert.notNull(key, "Key must not be null!");

			return new SUnionStoreCommand(key, keys);
		}

		/**
		 * @return
		 */
		public List<ByteBuffer> getKeys() {
			return keys;
		}
	}

	/**
	 * Union all given sets at {@literal keys} and store result in {@literal destinationKey}.
	 *
	 * @param destinationKey must not be {@literal null}.
	 * @param keys must not be {@literal null}.
	 * @return size of set stored a {@literal destinationKey}.
	 * @see <a href="http://redis.io/commands/sunionstore">Redis Documentation: SUNIONSTORE</a>
	 */
	default Mono<Long> sUnionStore(ByteBuffer destinationKey, Collection<ByteBuffer> keys) {

		Assert.notNull(destinationKey, "DestinationKey must not be null!");
		Assert.notNull(keys, "Keys must not be null!");

		return sUnionStore(Mono.just(SUnionStoreCommand.keys(keys).storeAt(destinationKey))).next()
				.map(NumericResponse::getOutput);
	}

	/**
	 * Union all given sets at {@literal keys} and store result in {@literal destinationKey}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/sunionstore">Redis Documentation: SUNIONSTORE</a>
	 */
	Flux<NumericResponse<SUnionStoreCommand, Long>> sUnionStore(Publisher<SUnionStoreCommand> commands);

	/**
	 * {@code SDIFF} command parameters.
	 *
	 * @author Christoph Strobl
	 * @see <a href="http://redis.io/commands/sdiff">Redis Documentation: SDIFF</a>
	 */
	class SDiffCommand implements Command {

		private final List<ByteBuffer> keys;

		private SDiffCommand(List<ByteBuffer> keys) {
			this.keys = keys;
		}

		/**
		 * Creates a new {@link SDiffCommand} given a {@link Collection} of keys.
		 *
		 * @param keys must not be {@literal null}.
		 * @return a new {@link SDiffCommand} for a {@link Collection} of values.
		 */
		public static SDiffCommand keys(Collection<ByteBuffer> keys) {

			Assert.notNull(keys, "Keys must not be null!");

			return new SDiffCommand(new ArrayList<>(keys));
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.connection.ReactiveRedisConnection.Command#getKey()
		 */
		@Override
		@Nullable
		public ByteBuffer getKey() {
			return null;
		}

		/**
		 * @return
		 */
		public List<ByteBuffer> getKeys() {
			return keys;
		}
	}

	/**
	 * Returns the diff of the members of all given sets at {@literal keys}.
	 *
	 * @param keys must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/sdiff">Redis Documentation: SDIFF</a>
	 */
	default Flux<ByteBuffer> sDiff(Collection<ByteBuffer> keys) {

		Assert.notNull(keys, "Keys must not be null!");

		return sDiff(Mono.just(SDiffCommand.keys(keys))).flatMap(CommandResponse::getOutput);
	}

	/**
	 * Returns the diff of the members of all given sets at {@link SInterCommand#getKeys()}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/sdiff">Redis Documentation: SDIFF</a>
	 */
	Flux<CommandResponse<SDiffCommand, Flux<ByteBuffer>>> sDiff(Publisher<SDiffCommand> commands);

	/**
	 * {@code SDIFFSTORE} command parameters.
	 *
	 * @author Christoph Strobl
	 * @see <a href="http://redis.io/commands/sdiffstore">Redis Documentation: SDIFFSTORE</a>
	 */
	class SDiffStoreCommand extends KeyCommand {

		private final List<ByteBuffer> keys;

		private SDiffStoreCommand(@Nullable ByteBuffer key, List<ByteBuffer> keys) {

			super(key);

			this.keys = keys;
		}

		/**
		 * Creates a new {@link SDiffStoreCommand} given a {@link Collection} of keys.
		 *
		 * @param keys must not be {@literal null}.
		 * @return a new {@link SDiffStoreCommand} for a {@link Collection} of values.
		 */
		public static SDiffStoreCommand keys(Collection<ByteBuffer> keys) {

			Assert.notNull(keys, "Keys must not be null!");

			return new SDiffStoreCommand(null, new ArrayList<>(keys));
		}

		/**
		 * Applies the {@literal key} at which the result is stored. Constructs a new command instance with all previously
		 * configured properties.
		 *
		 * @param key must not be {@literal null}.
		 * @return a new {@link SDiffStoreCommand} with {@literal key} applied.
		 */
		public SDiffStoreCommand storeAt(ByteBuffer key) {

			Assert.notNull(key, "Key must not be null!");

			return new SDiffStoreCommand(key, keys);
		}

		/**
		 * @return
		 */
		public List<ByteBuffer> getKeys() {
			return keys;
		}
	}

	/**
	 * Diff all given sets at {@literal keys} and store result in {@literal destinationKey}.
	 *
	 * @param destinationKey must not be {@literal null}.
	 * @param keys must not be {@literal null}.
	 * @return size of set stored a {@literal destinationKey}.
	 * @see <a href="http://redis.io/commands/sdiffstore">Redis Documentation: SDIFFSTORE</a>
	 */
	default Mono<Long> sDiffStore(ByteBuffer destinationKey, Collection<ByteBuffer> keys) {

		Assert.notNull(destinationKey, "DestinationKey must not be null!");
		Assert.notNull(keys, "Keys must not be null!");

		return sDiffStore(Mono.just(SDiffStoreCommand.keys(keys).storeAt(destinationKey))).next()
				.map(NumericResponse::getOutput);
	}

	/**
	 * Diff all given sets at {@literal keys} and store result in {@literal destinationKey}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/sdiffstore">Redis Documentation: SDIFFSTORE</a>
	 */
	Flux<NumericResponse<SDiffStoreCommand, Long>> sDiffStore(Publisher<SDiffStoreCommand> commands);

	/**
	 * Get all elements of set at {@literal key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/smembers">Redis Documentation: SMEMBERS</a>
	 */
	default Flux<ByteBuffer> sMembers(ByteBuffer key) {

		Assert.notNull(key, "Key must not be null!");

		return sMembers(Mono.just(new KeyCommand(key))).flatMap(CommandResponse::getOutput);
	}

	/**
	 * Get all elements of set at {@link KeyCommand#getKey()}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/smembers">Redis Documentation: SMEMBERS</a>
	 */
	Flux<CommandResponse<KeyCommand, Flux<ByteBuffer>>> sMembers(Publisher<KeyCommand> commands);

	/**
	 * Use a {@link Flux} to iterate over members in the set at {@code key}. The resulting {@link Flux} acts as a cursor
	 * and issues {@code SSCAN} commands itself as long as the subscriber signals demand.
	 *
	 * @param key must not be {@literal null}.
	 * @return the {@link Flux} emitting the raw {@link ByteBuffer members} one by one.
	 * @throws IllegalArgumentException when options is {@literal null}.
	 * @see <a href="http://redis.io/commands/sscan">Redis Documentation: SSCAN</a>
	 * @since 2.1
	 */
	default Flux<ByteBuffer> sScan(ByteBuffer key) {
		return sScan(key, ScanOptions.NONE);
	}

	/**
	 * Use a {@link Flux} to iterate over members in the set at {@code key} given {@link ScanOptions}. The resulting
	 * {@link Flux} acts as a cursor and issues {@code SSCAN} commands itself as long as the subscriber signals demand.
	 *
	 * @param key must not be {@literal null}.
	 * @param options must not be {@literal null}. Use {@link ScanOptions#NONE} instead.
	 * @return the {@link Flux} emitting the raw {@link ByteBuffer members} one by one.
	 * @throws IllegalArgumentException when one of the required arguments is {@literal null}.
	 * @see <a href="http://redis.io/commands/sscan">Redis Documentation: SSCAN</a>
	 * @since 2.1
	 */
	default Flux<ByteBuffer> sScan(ByteBuffer key, ScanOptions options) {

		return sScan(Mono.just(KeyScanCommand.key(key).withOptions(options))).map(CommandResponse::getOutput)
				.flatMap(it -> it);
	}

	/**
	 * Use a {@link Flux} to iterate over members in the set at {@code key}. The resulting {@link Flux} acts as a cursor
	 * and issues {@code SSCAN} commands itself as long as the subscriber signals demand.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/sscan">Redis Documentation: SSCAN</a>
	 * @since 2.1
	 */
	Flux<CommandResponse<KeyCommand, Flux<ByteBuffer>>> sScan(Publisher<KeyScanCommand> commands);

	/**
	 * {@code SRANDMEMBER} command parameters.
	 *
	 * @author Christoph Strobl
	 * @see <a href="http://redis.io/commands/srandmember">Redis Documentation: SRANDMEMBER</a>
	 */
	class SRandMembersCommand extends KeyCommand {

		private final @Nullable Long count;

		private SRandMembersCommand(@Nullable ByteBuffer key, @Nullable Long count) {

			super(key);
			this.count = count;
		}

		/**
		 * Creates a new {@link SRandMembersCommand} given the number of values to retrieve.
		 *
		 * @param nrValuesToRetrieve
		 * @return a new {@link SRandMembersCommand} for a number of values to retrieve.
		 */
		public static SRandMembersCommand valueCount(long nrValuesToRetrieve) {
			return new SRandMembersCommand(null, nrValuesToRetrieve);
		}

		/**
		 * Creates a new {@link SRandMembersCommand} to retrieve one random member.
		 *
		 * @return a new {@link SRandMembersCommand} to retrieve one random member.
		 */
		public static SRandMembersCommand singleValue() {
			return new SRandMembersCommand(null, null);
		}

		/**
		 * Applies the {@literal key}. Constructs a new command instance with all previously configured properties.
		 *
		 * @param key must not be {@literal null}.
		 * @return a new {@link SRandMembersCommand} with {@literal key} applied.
		 */
		public SRandMembersCommand from(ByteBuffer key) {

			Assert.notNull(key, "Key must not be null!");

			return new SRandMembersCommand(key, count);
		}

		/**
		 * @return
		 */
		public Optional<Long> getCount() {
			return Optional.ofNullable(count);
		}
	}

	/**
	 * Get random element from set at {@literal key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/srandmember">Redis Documentation: SRANDMEMBER</a>
	 */
	default Mono<ByteBuffer> sRandMember(ByteBuffer key) {
		return sRandMember(key, 1L).singleOrEmpty();
	}

	/**
	 * Get {@literal count} random elements from set at {@literal key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param count must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/srandmember">Redis Documentation: SRANDMEMBER</a>
	 */
	default Flux<ByteBuffer> sRandMember(ByteBuffer key, Long count) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(count, "Count must not be null!");

		return sRandMember(Mono.just(SRandMembersCommand.valueCount(count).from(key))).flatMap(CommandResponse::getOutput);
	}

	/**
	 * Get {@link SRandMembersCommand#getCount()} random elements from set at {@link SRandMembersCommand#getKey()}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/srandmember">Redis Documentation: SRANDMEMBER</a>
	 */
	Flux<CommandResponse<SRandMembersCommand, Flux<ByteBuffer>>> sRandMember(Publisher<SRandMembersCommand> commands);
}
