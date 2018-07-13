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
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;

import org.reactivestreams.Publisher;
import org.springframework.data.redis.connection.ReactiveRedisConnection.BooleanResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.CommandResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.KeyCommand;
import org.springframework.data.redis.connection.ReactiveRedisConnection.MultiValueResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.NumericResponse;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Redis Key commands executed using reactive infrastructure.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.0
 */
public interface ReactiveKeyCommands {

	/**
	 * Determine if given {@literal key} exists.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/exists">Redis Documentation: EXISTS</a>
	 */
	default Mono<Boolean> exists(ByteBuffer key) {

		Assert.notNull(key, "Key must not be null!");

		return exists(Mono.just(new KeyCommand(key))).next().map(BooleanResponse::getOutput);
	}

	/**
	 * Determine if given {@literal key} exists.
	 *
	 * @param keys must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/exists">Redis Documentation: EXISTS</a>
	 */
	Flux<BooleanResponse<KeyCommand>> exists(Publisher<KeyCommand> keys);

	/**
	 * Determine the type stored at {@literal key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/type">Redis Documentation: TYPE</a>
	 */
	default Mono<DataType> type(ByteBuffer key) {

		Assert.notNull(key, "Key must not be null!");

		return type(Mono.just(new KeyCommand(key))).next().map(CommandResponse::getOutput);
	}

	/**
	 * Determine the type stored at {@literal key}.
	 *
	 * @param keys must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/type">Redis Documentation: TYPE</a>
	 */
	Flux<CommandResponse<KeyCommand, DataType>> type(Publisher<KeyCommand> keys);

	/**
	 * Alter the last access time of given {@code key(s)}.
	 *
	 * @param keys must not be {@literal null}.
	 * @return {@link Mono} emitting the number of keys touched.
	 * @see <a href="http://redis.io/commands/touch">Redis Documentation: TOUCH</a>
	 * @since 2.1
	 */
	default Mono<Long> touch(Collection<ByteBuffer> keys) {
		return touch(Mono.just(keys)).next().map(NumericResponse::getOutput);
	}

	/**
	 * Alter the last access time of given {@code key(s)}.
	 *
	 * @param keys must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/touch">Redis Documentation: TOUCH</a>
	 * @since 2.1
	 */
	Flux<NumericResponse<Collection<ByteBuffer>, Long>> touch(Publisher<Collection<ByteBuffer>> keys);

	/**
	 * Find all keys matching the given {@literal pattern}.<br />
	 * It is recommended to use {@link #scan(ScanOptions)} to iterate over the keyspace as {@link #keys(ByteBuffer)} is a
	 * non-interruptible and expensive Redis operation.
	 *
	 * @param pattern must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/keys">Redis Documentation: KEYS</a>
	 */
	default Mono<List<ByteBuffer>> keys(ByteBuffer pattern) {

		Assert.notNull(pattern, "Pattern must not be null!");

		return keys(Mono.just(pattern)).next().map(MultiValueResponse::getOutput);
	}

	/**
	 * Find all keys matching the given {@literal pattern}.<br />
	 * It is recommended to use {@link #scan(ScanOptions)} to iterate over the keyspace as {@link #keys(Publisher)} is a
	 * non-interruptible and expensive Redis operation.
	 * 
	 * @param patterns must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/keys">Redis Documentation: KEYS</a>
	 */
	Flux<MultiValueResponse<ByteBuffer, ByteBuffer>> keys(Publisher<ByteBuffer> patterns);

	/**
	 * Use a {@link Flux} to iterate over keys. The resulting {@link Flux} acts as a cursor and issues {@code SCAN}
	 * commands itself as long as the subscriber signals demand.
	 *
	 * @return never {@literal null}.
	 * @see <a href="http://redis.io/commands/scan">Redis Documentation: SCAN</a>
	 * @since 2.1
	 */
	default Flux<ByteBuffer> scan() {
		return scan(ScanOptions.NONE);
	}

	/**
	 * Use a {@link Flux} to iterate over keys. The resulting {@link Flux} acts as a cursor and issues {@code SCAN}
	 * commands itself as long as the subscriber signals demand.
	 *
	 * @param options must not be {@literal null}.
	 * @return the {@link Flux} emitting {@link ByteBuffer keys} one by one.
	 * @throws IllegalArgumentException when options is {@literal null}.
	 * @see <a href="http://redis.io/commands/scan">Redis Documentation: SCAN</a>
	 * @since 2.1
	 */
	Flux<ByteBuffer> scan(ScanOptions options);

	/**
	 * Return a random key from the keyspace.
	 *
	 * @return
	 * @see <a href="http://redis.io/commands/randomkey">Redis Documentation: RANDOMKEY</a>
	 */
	Mono<ByteBuffer> randomKey();

	/**
	 * {@code RENAME} command parameters.
	 *
	 * @author Christoph Strobl
	 * @see <a href="http://redis.io/commands/rename">Redis Documentation: RENAME</a>
	 */
	class RenameCommand extends KeyCommand {

		private @Nullable ByteBuffer newName;

		private RenameCommand(ByteBuffer key, @Nullable ByteBuffer newName) {

			super(key);

			this.newName = newName;
		}

		/**
		 * Creates a new {@link RenameCommand} given a {@link ByteBuffer key}.
		 *
		 * @param key must not be {@literal null}.
		 * @return a new {@link RenameCommand} for {@link ByteBuffer key}.
		 */
		public static RenameCommand key(ByteBuffer key) {

			Assert.notNull(key, "Key must not be null!");

			return new RenameCommand(key, null);
		}

		/**
		 * Applies the {@literal newName}. Constructs a new command instance with all previously configured properties.
		 *
		 * @param newName must not be {@literal null}.
		 * @return a new {@link RenameCommand} with {@literal newName} applied.
		 */
		public RenameCommand to(ByteBuffer newName) {

			Assert.notNull(newName, "New name must not be null!");

			return new RenameCommand(getKey(), newName);
		}

		/**
		 * @return can be {@literal null}.
		 */
		@Nullable
		public ByteBuffer getNewName() {
			return newName;
		}
	}

	/**
	 * Rename key {@literal oleName} to {@literal newName}.
	 *
	 * @param key must not be {@literal null}.
	 * @param newName must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/rename">Redis Documentation: RENAME</a>
	 */
	default Mono<Boolean> rename(ByteBuffer key, ByteBuffer newName) {

		Assert.notNull(key, "Key must not be null!");

		return rename(Mono.just(RenameCommand.key(key).to(newName))).next().map(BooleanResponse::getOutput);
	}

	/**
	 * Rename key {@literal oleName} to {@literal newName}.
	 *
	 * @param command must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/rename">Redis Documentation: RENAME</a>
	 */
	Flux<BooleanResponse<RenameCommand>> rename(Publisher<RenameCommand> command);

	/**
	 * Rename key {@literal oleName} to {@literal newName} only if {@literal newName} does not exist.
	 *
	 * @param key must not be {@literal null}.
	 * @param newName must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/renamenx">Redis Documentation: RENAMENX</a>
	 */
	default Mono<Boolean> renameNX(ByteBuffer key, ByteBuffer newName) {

		Assert.notNull(key, "Key must not be null!");

		return renameNX(Mono.just(RenameCommand.key(key).to(newName))).next().map(BooleanResponse::getOutput);
	}

	/**
	 * Rename key {@literal oleName} to {@literal newName} only if {@literal newName} does not exist.
	 *
	 * @param command must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/renamenx">Redis Documentation: RENAMENX</a>
	 */
	Flux<BooleanResponse<RenameCommand>> renameNX(Publisher<RenameCommand> command);

	/**
	 * Delete {@literal key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/del">Redis Documentation: DEL</a>
	 */
	default Mono<Long> del(ByteBuffer key) {

		Assert.notNull(key, "Key must not be null!");

		return del(Mono.just(new KeyCommand(key))).next().map(NumericResponse::getOutput);
	}

	/**
	 * Delete {@literal keys} one by one.
	 *
	 * @param keys must not be {@literal null}.
	 * @return {@link Flux} of {@link NumericResponse} holding the {@literal key} removed along with the deletion result.
	 * @see <a href="http://redis.io/commands/del">Redis Documentation: DEL</a>
	 */
	Flux<NumericResponse<KeyCommand, Long>> del(Publisher<KeyCommand> keys);

	/**
	 * Delete multiple {@literal keys} one in one batch.
	 *
	 * @param keys must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/del">Redis Documentation: DEL</a>
	 */
	default Mono<Long> mDel(List<ByteBuffer> keys) {

		Assert.notEmpty(keys, "Keys must not be empty or null!");

		return mDel(Mono.just(keys)).next().map(NumericResponse::getOutput);
	}

	/**
	 * Delete multiple {@literal keys} in batches.
	 *
	 * @param keys must not be {@literal null}.
	 * @return {@link Flux} of {@link NumericResponse} holding the {@literal keys} removed along with the deletion result.
	 * @see <a href="http://redis.io/commands/del">Redis Documentation: DEL</a>
	 */
	Flux<NumericResponse<List<ByteBuffer>, Long>> mDel(Publisher<List<ByteBuffer>> keys);

	/**
	 * Unlink the {@code key} from the keyspace. Unlike with {@link #del(ByteBuffer)} the actual memory reclaiming here
	 * happens asynchronously.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/unlink">Redis Documentation: UNLINK</a>
	 * @since 2.1
	 */
	default Mono<Long> unlink(ByteBuffer key) {

		Assert.notNull(key, "Keys must not be null!");

		return unlink(Mono.just(key).map(KeyCommand::new)).next().map(NumericResponse::getOutput);
	}

	/**
	 * Unlink the {@code key} from the keyspace. Unlike with {@link #del(ByteBuffer)} the actual memory reclaiming here
	 * happens asynchronously.
	 *
	 * @param keys must not be {@literal null}.
	 * @return {@link Flux} of {@link NumericResponse} holding the {@literal key} removed along with the unlink result.
	 * @see <a href="http://redis.io/commands/unlink">Redis Documentation: UNLINK</a>
	 * @since 2.1
	 */
	Flux<NumericResponse<KeyCommand, Long>> unlink(Publisher<KeyCommand> keys);

	/**
	 * Unlink the {@code keys} from the keyspace. Unlike with {@link #mDel(List)} the actual memory reclaiming here
	 * happens asynchronously.
	 *
	 * @param keys must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/unlink">Redis Documentation: UNLINK</a>
	 * @since 2.1
	 */
	default Mono<Long> mUnlink(List<ByteBuffer> keys) {

		Assert.notNull(keys, "Keys must not be null!");

		return mUnlink(Mono.just(keys)).next().map(NumericResponse::getOutput);
	}

	/**
	 * Unlink the {@code keys} from the keyspace. Unlike with {@link #mDel(Publisher)} the actual memory reclaiming here
	 * happens asynchronously.
	 *
	 * @param keys must not be {@literal null}.
	 * @return {@link Flux} of {@link NumericResponse} holding the {@literal key} removed along with the deletion result.
	 * @see <a href="http://redis.io/commands/unlink">Redis Documentation: UNLINK</a>
	 * @since 2.1
	 */
	Flux<NumericResponse<List<ByteBuffer>, Long>> mUnlink(Publisher<List<ByteBuffer>> keys);

	/**
	 * {@code EXPIRE}/{@code PEXPIRE} command parameters.
	 *
	 * @author Mark Paluch
	 * @see <a href="http://redis.io/commands/expire">Redis Documentation: EXPIRE</a>
	 * @see <a href="http://redis.io/commands/pexpire">Redis Documentation: PEXPIRE</a>
	 */
	class ExpireCommand extends KeyCommand {

		private @Nullable Duration timeout;

		private ExpireCommand(ByteBuffer key, @Nullable Duration timeout) {

			super(key);

			this.timeout = timeout;
		}

		/**
		 * Creates a new {@link ExpireCommand} given a {@link ByteBuffer key}.
		 *
		 * @param key must not be {@literal null}.
		 * @return a new {@link ExpireCommand} for {@link ByteBuffer key}.
		 */
		public static ExpireCommand key(ByteBuffer key) {

			Assert.notNull(key, "Key must not be null!");

			return new ExpireCommand(key, null);
		}

		/**
		 * Applies the {@literal timeout}. Constructs a new command instance with all previously configured properties.
		 *
		 * @param timeout must not be {@literal null}.
		 * @return a new {@link ExpireCommand} with {@literal timeout} applied.
		 */
		public ExpireCommand timeout(Duration timeout) {

			Assert.notNull(timeout, "Timeout must not be null!");

			return new ExpireCommand(getKey(), timeout);
		}

		/**
		 * @return can be {@literal null}.
		 */
		@Nullable
		public Duration getTimeout() {
			return timeout;
		}
	}

	/**
	 * Set time to live for given {@code key} in seconds.
	 *
	 * @param key must not be {@literal null}.
	 * @param timeout must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/expire">Redis Documentation: EXPIRE</a>
	 */
	default Mono<Boolean> expire(ByteBuffer key, Duration timeout) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(timeout, "Timeout must not be null!");

		return expire(Mono.just(new ExpireCommand(key, timeout))).next().map(BooleanResponse::getOutput);
	}

	/**
	 * Expire {@literal keys} one by one.
	 *
	 * @param commands must not be {@literal null}.
	 * @return {@link Flux} of {@link BooleanResponse} holding the {@literal key} removed along with the expiration
	 *         result.
	 * @see <a href="http://redis.io/commands/expire">Redis Documentation: EXPIRE</a>
	 */
	Flux<BooleanResponse<ExpireCommand>> expire(Publisher<ExpireCommand> commands);

	/**
	 * Set time to live for given {@code key} in milliseconds.
	 *
	 * @param key must not be {@literal null}.
	 * @param timeout must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/pexpire">Redis Documentation: PEXPIRE</a>
	 */
	default Mono<Boolean> pExpire(ByteBuffer key, Duration timeout) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(timeout, "Timeout must not be null!");

		return expire(Mono.just(new ExpireCommand(key, timeout))).next().map(BooleanResponse::getOutput);
	}

	/**
	 * Expire {@literal keys} one by one.
	 *
	 * @param commands must not be {@literal null}.
	 * @return {@link Flux} of {@link BooleanResponse} holding the {@literal key} removed along with the expiration
	 *         result.
	 * @see <a href="http://redis.io/commands/pexpire">Redis Documentation: PEXPIRE</a>
	 */
	Flux<BooleanResponse<ExpireCommand>> pExpire(Publisher<ExpireCommand> commands);

	/**
	 * {@code EXPIREAT}/{@code PEXPIREAT} command parameters.
	 *
	 * @author Mark Paluch
	 * @see <a href="http://redis.io/commands/expire">Redis Documentation: EXPIREAT</a>
	 * @see <a href="http://redis.io/commands/pexpire">Redis Documentation: PEXPIREAT</a>
	 */
	class ExpireAtCommand extends KeyCommand {

		private @Nullable Instant expireAt;

		private ExpireAtCommand(ByteBuffer key, @Nullable Instant expireAt) {

			super(key);

			this.expireAt = expireAt;
		}

		/**
		 * Creates a new {@link ExpireAtCommand} given a {@link ByteBuffer key}.
		 *
		 * @param key must not be {@literal null}.
		 * @return a new {@link ExpireCommand} for {@link ByteBuffer key}.
		 */
		public static ExpireAtCommand key(ByteBuffer key) {

			Assert.notNull(key, "Key must not be null!");

			return new ExpireAtCommand(key, null);
		}

		/**
		 * Applies the {@literal expireAt}. Constructs a new command instance with all previously configured properties.
		 *
		 * @param expireAt must not be {@literal null}.
		 * @return a new {@link ExpireAtCommand} with {@literal expireAt} applied.
		 */
		public ExpireAtCommand timeout(Instant expireAt) {

			Assert.notNull(expireAt, "Expire at must not be null!");

			return new ExpireAtCommand(getKey(), expireAt);
		}

		/**
		 * @return can be {@literal null}.
		 */
		@Nullable
		public Instant getExpireAt() {
			return expireAt;
		}
	}

	/**
	 * Set the expiration for given {@code key} as a {@literal UNIX} timestamp.
	 *
	 * @param key must not be {@literal null}.
	 * @param expireAt must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/expireat">Redis Documentation: EXPIREAT</a>
	 */
	default Mono<Boolean> expireAt(ByteBuffer key, Instant expireAt) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(expireAt, "Expire at must not be null!");

		return expireAt(Mono.just(new ExpireAtCommand(key, expireAt))).next().map(BooleanResponse::getOutput);
	}

	/**
	 * Set one-by-one the expiration for given {@code key} as a {@literal UNIX} timestamp.
	 *
	 * @param commands must not be {@literal null}.
	 * @return {@link Flux} of {@link BooleanResponse} holding the {@literal key} removed along with the expiration
	 *         result.
	 * @see <a href="http://redis.io/commands/expireat">Redis Documentation: EXPIREAT</a>
	 */
	Flux<BooleanResponse<ExpireAtCommand>> expireAt(Publisher<ExpireAtCommand> commands);

	/**
	 * Set the expiration for given {@code key} as a {@literal UNIX} timestamp.
	 *
	 * @param key must not be {@literal null}.
	 * @param expireAt must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/pexpireat">Redis Documentation: PEXPIREAT</a>
	 */
	default Mono<Boolean> pExpireAt(ByteBuffer key, Instant expireAt) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(expireAt, "Expire at must not be null!");

		return pExpireAt(Mono.just(new ExpireAtCommand(key, expireAt))).next().map(BooleanResponse::getOutput);
	}

	/**
	 * Set one-by-one the expiration for given {@code key} as a {@literal UNIX} timestamp in milliseconds.
	 *
	 * @param commands must not be {@literal null}.
	 * @return {@link Flux} of {@link BooleanResponse} holding the {@literal key} removed along with the expiration
	 *         result.
	 * @see <a href="http://redis.io/commands/pexpireat">Redis Documentation: PEXPIREAT</a>
	 */
	Flux<BooleanResponse<ExpireAtCommand>> pExpireAt(Publisher<ExpireAtCommand> commands);

	/**
	 * Remove the expiration from given {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/persist">Redis Documentation: PERSIST</a>
	 */
	default Mono<Boolean> persist(ByteBuffer key) {

		Assert.notNull(key, "Key must not be null!");

		return persist(Mono.just(new KeyCommand(key))).next().map(BooleanResponse::getOutput);
	}

	/**
	 * Remove one-by-one the expiration from given {@code key}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return {@link Flux} of {@link BooleanResponse} holding the {@literal key} persisted along with the persist result.
	 * @see <a href="http://redis.io/commands/persist">Redis Documentation: PERSIST</a>
	 */
	Flux<BooleanResponse<KeyCommand>> persist(Publisher<KeyCommand> commands);

	/**
	 * Get the time to live for {@code key} in seconds.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/ttl">Redis Documentation: TTL</a>
	 */
	default Mono<Long> ttl(ByteBuffer key) {

		Assert.notNull(key, "Key must not be null!");

		return ttl(Mono.just(new KeyCommand(key))).next().map(NumericResponse::getOutput);
	}

	/**
	 * Get one-by-one the time to live for keys.
	 *
	 * @param commands must not be {@literal null}.
	 * @return {@link Flux} of {@link NumericResponse} holding the {@literal key} along with the time to live result.
	 * @see <a href="http://redis.io/commands/ttl">Redis Documentation: TTL</a>
	 */
	Flux<NumericResponse<KeyCommand, Long>> ttl(Publisher<KeyCommand> commands);

	/**
	 * Get the time to live for {@code key} in milliseconds.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/ttl">Redis Documentation: TTL</a>
	 */
	default Mono<Long> pTtl(ByteBuffer key) {

		Assert.notNull(key, "Key must not be null!");

		return pTtl(Mono.just(new KeyCommand(key))).next().map(NumericResponse::getOutput);
	}

	/**
	 * Get one-by-one the time to live for keys.
	 *
	 * @param commands must not be {@literal null}.
	 * @return {@link Flux} of {@link NumericResponse} holding the {@literal key} along with the time to live result.
	 * @see <a href="http://redis.io/commands/pttl">Redis Documentation: PTTL</a>
	 */
	Flux<NumericResponse<KeyCommand, Long>> pTtl(Publisher<KeyCommand> commands);

	/**
	 * {@code MOVE} command parameters.
	 *
	 * @author Mark Paluch
	 * @see <a href="http://redis.io/commands/move">Redis Documentation: MOVE</a>
	 */
	class MoveCommand extends KeyCommand {

		private @Nullable Integer database;

		private MoveCommand(ByteBuffer key, @Nullable Integer database) {

			super(key);

			this.database = database;
		}

		/**
		 * Creates a new {@link MoveCommand} given a {@link ByteBuffer key}.
		 *
		 * @param key must not be {@literal null}.
		 * @return a new {@link ExpireCommand} for {@link ByteBuffer key}.
		 */
		public static MoveCommand key(ByteBuffer key) {

			Assert.notNull(key, "Key must not be null!");

			return new MoveCommand(key, null);
		}

		/**
		 * Applies the {@literal database} index. Constructs a new command instance with all previously configured
		 * properties.
		 *
		 * @param database
		 * @return a new {@link MoveCommand} with {@literal database} applied.
		 */
		public MoveCommand timeout(int database) {
			return new MoveCommand(getKey(), database);
		}

		/**
		 * @return can be {@literal null}.
		 */
		@Nullable
		public Integer getDatabase() {
			return database;
		}
	}

	/**
	 * Move given {@code key} to database with {@code index}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/move">Redis Documentation: MOVE</a>
	 */
	default Mono<Boolean> move(ByteBuffer key, int database) {

		Assert.notNull(key, "Key must not be null!");

		return move(Mono.just(new MoveCommand(key, database))).next().map(BooleanResponse::getOutput);
	}

	/**
	 * Move keys one-by-one between databases.
	 *
	 * @param commands must not be {@literal null}.
	 * @return {@link Flux} of {@link BooleanResponse} holding the {@literal key} to move along with the move result.
	 * @see <a href="http://redis.io/commands/move">Redis Documentation: MOVE</a>
	 */
	Flux<BooleanResponse<MoveCommand>> move(Publisher<MoveCommand> commands);

	/**
	 * Get the type of internal representation used for storing the value at the given {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return the {@link Mono} emitting {@link org.springframework.data.redis.connection.ValueEncoding}.
	 * @throws IllegalArgumentException if {@code key} is {@literal null}.
	 * @see <a href="http://redis.io/commands/object">Redis Documentation: OBJECT ENCODING</a>
	 * @since 2.1
	 */
	Mono<ValueEncoding> encodingOf(ByteBuffer key);

	/**
	 * Get the {@link Duration} since the object stored at the given {@code key} is idle.
	 *
	 * @param key must not be {@literal null}.
	 * @return the {@link Mono} emitting the idletime of the key of {@link Mono#empty()} if the key does not exist.
	 * @throws IllegalArgumentException if {@code key} is {@literal null}.
	 * @see <a href="http://redis.io/commands/object">Redis Documentation: OBJECT IDLETIME</a>
	 * @since 2.1
	 */
	Mono<Duration> idletime(ByteBuffer key);

	/**
	 * Get the number of references of the value associated with the specified {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@link Mono#empty()} if key does not exist.
	 * @throws IllegalArgumentException if {@code key} is {@literal null}.
	 * @see <a href="http://redis.io/commands/object">Redis Documentation: OBJECT REFCOUNT</a>
	 * @since 2.1
	 */
	Mono<Long> refcount(ByteBuffer key);
}
