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
package org.springframework.data.redis.connection;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.jspecify.annotations.Nullable;
import org.reactivestreams.Publisher;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.connection.ReactiveRedisConnection.BooleanResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.Command;
import org.springframework.data.redis.connection.ReactiveRedisConnection.CommandResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.KeyCommand;
import org.springframework.data.redis.connection.ReactiveRedisConnection.KeyScanCommand;
import org.springframework.data.redis.connection.ReactiveRedisConnection.MultiValueResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.NumericResponse;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.util.Assert;

/**
 * Redis Hash commands executed using reactive infrastructure.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Tihomir Mateev
 * @since 2.0
 */
public interface ReactiveHashCommands {

	/**
	 * {@link Command} for hash-bound operations.
	 *
	 * @author Christoph Strobl
	 * @author Tihomir Mateev
     * @author Viktoriya Kutsarova
	 */
	class HashFieldsCommand extends KeyCommand {

		private final List<ByteBuffer> fields;

		private HashFieldsCommand(@Nullable ByteBuffer key, List<ByteBuffer> fields) {
			super(key);
			this.fields = fields;
		}

		/**
		 * @return never {@literal null}.
		 */
		public List<ByteBuffer> getFields() {
			return fields;
		}
	}

	/**
	 * {@literal HSET} {@link Command}.
	 *
	 * @author Christoph Strobl
	 * @see <a href="https://redis.io/commands/hset">Redis Documentation: HSET</a>
	 */
	class HSetCommand extends KeyCommand {

		private static final ByteBuffer SINGLE_VALUE_KEY = ByteBuffer.allocate(0);
		private final Map<ByteBuffer, ByteBuffer> fieldValueMap;
		private final boolean upsert;

		private HSetCommand(@Nullable ByteBuffer key, Map<ByteBuffer, ByteBuffer> keyValueMap, boolean upsert) {

			super(key);

			this.fieldValueMap = keyValueMap;
			this.upsert = upsert;
		}

		/**
		 * Creates a new {@link HSetCommand} given a {@link ByteBuffer key}.
		 *
		 * @param value must not be {@literal null}.
		 * @return a new {@link HSetCommand} for {@link ByteBuffer key}.
		 */
		public static HSetCommand value(ByteBuffer value) {

			Assert.notNull(value, "Value must not be null");

			return new HSetCommand(null, Collections.singletonMap(SINGLE_VALUE_KEY, value), Boolean.TRUE);
		}

		/**
		 * Creates a new {@link HSetCommand} given a {@link Map} of field values.
		 *
		 * @param fieldValueMap must not be {@literal null}.
		 * @return a new {@link HSetCommand} for a {@link Map} of field values.
		 */
		public static HSetCommand fieldValues(Map<ByteBuffer, ByteBuffer> fieldValueMap) {

			Assert.notNull(fieldValueMap, "Field values map must not be null");

			return new HSetCommand(null, fieldValueMap, Boolean.TRUE);
		}

		/**
		 * Applies a field. Constructs a new command instance with all previously configured properties.
		 *
		 * @param field must not be {@literal null}.
		 * @return a new {@link HSetCommand} with {@literal field} applied.
		 */
		public HSetCommand ofField(ByteBuffer field) {

			if (!fieldValueMap.containsKey(SINGLE_VALUE_KEY)) {
				throw new InvalidDataAccessApiUsageException("Value has not been set.");
			}

			Assert.notNull(field, "Field not be null");

			return new HSetCommand(getKey(), Collections.singletonMap(field, fieldValueMap.get(SINGLE_VALUE_KEY)), upsert);
		}

		/**
		 * Applies the {@literal key}. Constructs a new command instance with all previously configured properties.
		 *
		 * @param key must not be {@literal null}.
		 * @return a new {@link HSetCommand} with {@literal key} applied.
		 */
		public HSetCommand forKey(ByteBuffer key) {

			Assert.notNull(key, "Key not be null");

			return new HSetCommand(key, fieldValueMap, upsert);
		}

		/**
		 * Disable upsert. Constructs a new command instance with all previously configured properties.
		 *
		 * @return a new {@link HSetCommand} with upsert disabled.
		 */
		public HSetCommand ifValueNotExists() {
			return new HSetCommand(getKey(), fieldValueMap, Boolean.FALSE);
		}

		/**
		 * @return
		 */
		public boolean isUpsert() {
			return upsert;
		}

		/**
		 * @return never {@literal null}.
		 */
		public Map<ByteBuffer, ByteBuffer> getFieldValueMap() {
			return fieldValueMap;
		}
	}

	/**
	 * Set the {@literal value} of a hash {@literal field}.
	 *
	 * @param key must not be {@literal null}.
	 * @param field must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/hset">Redis Documentation: HSET</a>
	 */
	default Mono<Boolean> hSet(ByteBuffer key, ByteBuffer field, ByteBuffer value) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(field, "Field must not be null");
		Assert.notNull(value, "Value must not be null");

		return hSet(Mono.just(HSetCommand.value(value).ofField(field).forKey(key))).next().map(BooleanResponse::getOutput);
	}

	/**
	 * Set the {@literal value} of a hash {@literal field}.
	 *
	 * @param key must not be {@literal null}.
	 * @param field must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/hsetnx">Redis Documentation: HSETNX</a>
	 */
	default Mono<Boolean> hSetNX(ByteBuffer key, ByteBuffer field, ByteBuffer value) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(field, "Field must not be null");
		Assert.notNull(value, "Value must not be null");

		return hSet(Mono.just(HSetCommand.value(value).ofField(field).forKey(key).ifValueNotExists())).next()
				.map(BooleanResponse::getOutput);
	}

	/**
	 * Set multiple hash fields to multiple values using data provided in {@literal fieldValueMap}.
	 *
	 * @param key must not be {@literal null}.
	 * @param fieldValueMap must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/hmset">Redis Documentation: HMSET</a>
	 */
	default Mono<Boolean> hMSet(ByteBuffer key, Map<ByteBuffer, ByteBuffer> fieldValueMap) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(fieldValueMap, "Field must not be null");

		return hSet(Mono.just(HSetCommand.fieldValues(fieldValueMap).forKey(key))).next().map(it -> true);
	}

	/**
	 * Set the {@literal value} of a hash {@literal field}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/hset">Redis Documentation: HSET</a>
	 */
	Flux<BooleanResponse<HSetCommand>> hSet(Publisher<HSetCommand> commands);

	/**
	 * {@literal HGET} {@link Command}.
	 *
	 * @author Christoph Strobl
	 * @see <a href="https://redis.io/commands/hget">Redis Documentation: HGET</a>
	 */
	class HGetCommand extends HashFieldsCommand {

		private HGetCommand(@Nullable ByteBuffer key, List<ByteBuffer> fields) {
			super(key, fields);
		}

		/**
		 * Creates a new {@link HGetCommand} given a {@link ByteBuffer field name}.
		 *
		 * @param field must not be {@literal null}.
		 * @return a new {@link HGetCommand} for a {@link ByteBuffer field name}.
		 */
		public static HGetCommand field(ByteBuffer field) {

			Assert.notNull(field, "Field must not be null");

			return new HGetCommand(null, Collections.singletonList(field));
		}

		/**
		 * Creates a new {@link HGetCommand} given a {@link Collection} of field names.
		 *
		 * @param fields must not be {@literal null}.
		 * @return a new {@link HGetCommand} for a {@link Collection} of field names.
		 */
		public static HGetCommand fields(Collection<ByteBuffer> fields) {

			Assert.notNull(fields, "Fields must not be null");

			return new HGetCommand(null, new ArrayList<>(fields));
		}

		/**
		 * Applies the hash {@literal key}. Constructs a new command instance with all previously configured properties.
		 *
		 * @param key must not be {@literal null}.
		 * @return a new {@link HGetCommand} with {@literal key} applied.
		 */
		public HGetCommand from(ByteBuffer key) {

			Assert.notNull(key, "Key must not be null");

			return new HGetCommand(key, getFields());
		}
	}

	/**
	 * Get value for given {@literal field} from hash at {@literal key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param field must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/hget">Redis Documentation: HGET</a>
	 */
	default Mono<ByteBuffer> hGet(ByteBuffer key, ByteBuffer field) {
		return hMGet(key, Collections.singletonList(field)).filter(it -> !it.contains(null))
				.flatMapIterable(Function.identity()).next();
	}

	/**
	 * Get values for given {@literal fields} from hash at {@literal key}. Values are in the order of the requested keys.
	 * Absent field values are represented using {@literal null} in the resulting {@link List}.
	 *
	 * @param key must not be {@literal null}.
	 * @param fields must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/hmget">Redis Documentation: HMGET</a>
	 */
	default Mono<List<ByteBuffer>> hMGet(ByteBuffer key, Collection<ByteBuffer> fields) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(fields, "Fields must not be null");

		return hMGet(Mono.just(HGetCommand.fields(fields).from(key))).next().map(MultiValueResponse::getOutput);
	}

	/**
	 * Get values for given {@literal fields} from hash at {@literal key}. Values are in the order of the requested keys.
	 * Absent field values are represented using {@literal null} in the resulting {@link List}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/hmget">Redis Documentation: HMGET</a>
	 */
	Flux<MultiValueResponse<HGetCommand, ByteBuffer>> hMGet(Publisher<HGetCommand> commands);

	/**
	 * {@literal HEXISTS} {@link Command}.
	 *
	 * @author Christoph Strobl
	 * @see <a href="https://redis.io/commands/hexists">Redis Documentation: HEXISTS</a>
	 */
	class HExistsCommand extends KeyCommand {

		private final ByteBuffer field;

		private HExistsCommand(@Nullable ByteBuffer key, ByteBuffer field) {

			super(key);

			this.field = field;
		}

		/**
		 * Creates a new {@link HExistsCommand} given a {@link ByteBuffer field name}.
		 *
		 * @param field must not be {@literal null}.
		 * @return a new {@link HExistsCommand} for a {@link ByteBuffer field name}.
		 */
		public static HExistsCommand field(ByteBuffer field) {

			Assert.notNull(field, "Field must not be null");

			return new HExistsCommand(null, field);
		}

		/**
		 * Applies the hash {@literal key}. Constructs a new command instance with all previously configured properties.
		 *
		 * @param key must not be {@literal null}.
		 * @return a new {@link HExistsCommand} with {@literal key} applied.
		 */
		public HExistsCommand in(ByteBuffer key) {

			Assert.notNull(key, "Key must not be null");

			return new HExistsCommand(key, field);
		}

		/**
		 * @return never {@literal null}.
		 */
		public ByteBuffer getField() {
			return field;
		}
	}

	/**
	 * Determine if given hash {@literal field} exists.
	 *
	 * @param key must not be {@literal null}.
	 * @param field must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/hexists">Redis Documentation: HEXISTS</a>
	 */
	default Mono<Boolean> hExists(ByteBuffer key, ByteBuffer field) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(field, "Field must not be null");

		return hExists(Mono.just(HExistsCommand.field(field).in(key))).next().map(BooleanResponse::getOutput);
	}

	/**
	 * Determine if given hash {@literal field} exists.
	 *
	 * @param commands
	 * @return
	 * @see <a href="https://redis.io/commands/hexists">Redis Documentation: HEXISTS</a>
	 */
	Flux<BooleanResponse<HExistsCommand>> hExists(Publisher<HExistsCommand> commands);

	/**
	 * @author Christoph Strobl
	 * @see <a href="https://redis.io/commands/hdel">Redis Documentation: HDEL</a>
	 */
	class HDelCommand extends HashFieldsCommand {

		private HDelCommand(@Nullable ByteBuffer key, List<ByteBuffer> fields) {
			super(key, fields);
		}

		/**
		 * Creates a new {@link HDelCommand} given a {@link ByteBuffer field name}.
		 *
		 * @param field must not be {@literal null}.
		 * @return a new {@link HDelCommand} for a {@link ByteBuffer field name}.
		 */
		public static HDelCommand field(ByteBuffer field) {

			Assert.notNull(field, "Field must not be null");

			return new HDelCommand(null, Collections.singletonList(field));
		}

		/**
		 * Creates a new {@link HDelCommand} given a {@link Collection} of field names.
		 *
		 * @param fields must not be {@literal null}.
		 * @return a new {@link HDelCommand} for a {@link Collection} of field names.
		 */
		public static HDelCommand fields(Collection<ByteBuffer> fields) {

			Assert.notNull(fields, "Fields must not be null");

			return new HDelCommand(null, new ArrayList<>(fields));
		}

		/**
		 * Applies the hash {@literal key}. Constructs a new command instance with all previously configured properties.
		 *
		 * @param key must not be {@literal null}.
		 * @return a new {@link HDelCommand} with {@literal key} applied.
		 */
		public HDelCommand from(ByteBuffer key) {

			Assert.notNull(key, "Key must not be null");

			return new HDelCommand(key, getFields());
		}
	}

	/**
	 * Delete given hash {@literal field}.
	 *
	 * @param key must not be {@literal null}.
	 * @param field must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/hdel">Redis Documentation: HDEL</a>
	 */
	default Mono<Boolean> hDel(ByteBuffer key, ByteBuffer field) {

		Assert.notNull(field, "Field must not be null");

		return hDel(key, Collections.singletonList(field)).map(val -> val > 0 ? Boolean.TRUE : Boolean.FALSE);
	}

	/**
	 * Delete given hash {@literal fields}.
	 *
	 * @param key must not be {@literal null}.
	 * @param fields must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/hdel">Redis Documentation: HDEL</a>
	 */
	default Mono<Long> hDel(ByteBuffer key, Collection<ByteBuffer> fields) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(fields, "Fields must not be null");

		return hDel(Mono.just(HDelCommand.fields(fields).from(key))).next().map(NumericResponse::getOutput);
	}

	/**
	 * Delete given hash {@literal fields}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/hdel">Redis Documentation: HDEL</a>
	 */
	Flux<NumericResponse<HDelCommand, Long>> hDel(Publisher<HDelCommand> commands);

	/**
	 * Get size of hash at {@literal key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/hlen">Redis Documentation: HLEN</a>
	 */
	default Mono<Long> hLen(ByteBuffer key) {

		Assert.notNull(key, "Key must not be null");

		return hLen(Mono.just(new KeyCommand(key))).next().map(NumericResponse::getOutput);
	}

	/**
	 * Get size of hash at {@literal key}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/hlen">Redis Documentation: HLEN</a>
	 */
	Flux<NumericResponse<KeyCommand, Long>> hLen(Publisher<KeyCommand> commands);

	/**
	 * {@literal HRANDFIELD} {@link Command}.
	 *
	 * @author Mark Paluch
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/hrandfield">Redis Documentation: HRANDFIELD</a>
	 */
	class HRandFieldCommand extends KeyCommand {

		private long count;

		private HRandFieldCommand(@Nullable ByteBuffer key, long count) {

			super(key);

			this.count = count;
		}

		/**
		 * Applies the hash {@literal key}. Constructs a new command instance with all previously configured properties.
		 *
		 * @param key must not be {@literal null}.
		 * @return a new {@link HRandFieldCommand} with {@literal key} applied.
		 */
		public static HRandFieldCommand key(ByteBuffer key) {

			Assert.notNull(key, "Key must not be null");

			return new HRandFieldCommand(key, 1);
		}

		/**
		 * Applies the {@literal count}. Constructs a new command instance with all previously configured properties. If the
		 * provided {@code count} argument is positive, return a list of distinct fields, capped either at {@code count} or
		 * the hash size. If {@code count} is negative, the behavior changes and the command is allowed to return the same
		 * field multiple times. In this case, the number of returned fields is the absolute value of the specified count.
		 *
		 * @param count
		 * @return a new {@link HRandFieldCommand} with {@literal key} applied.
		 */
		public HRandFieldCommand count(long count) {
			return new HRandFieldCommand(getKey(), count);
		}

		public long getCount() {
			return count;
		}
	}

	/**
	 * Return a random field from the hash stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/hrandfield">Redis Documentation: HRANDFIELD</a>
	 */
	default Mono<ByteBuffer> hRandField(ByteBuffer key) {

		Assert.notNull(key, "Key must not be null");

		return hRandField(Mono.just(HRandFieldCommand.key(key).count(1))).flatMap(CommandResponse::getOutput).next();
	}

	/**
	 * Return a random field from the hash along with its value stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/hrandfield">Redis Documentation: HRANDFIELD</a>
	 */
	default Mono<Map.Entry<ByteBuffer, ByteBuffer>> hRandFieldWithValues(ByteBuffer key) {

		Assert.notNull(key, "Key must not be null");

		return hRandFieldWithValues(Mono.just(HRandFieldCommand.key(key).count(1))).flatMap(CommandResponse::getOutput)
				.next();
	}

	/**
	 * Return a random field from the hash stored at {@code key}. If the provided {@code count} argument is positive,
	 * return a list of distinct fields, capped either at {@code count} or the hash size. If {@code count} is negative,
	 * the behavior changes and the command is allowed to return the same field multiple times. In this case, the number
	 * of returned fields is the absolute value of the specified count.
	 *
	 * @param key must not be {@literal null}.
	 * @param count number of fields to return.
	 * @return
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/hrandfield">Redis Documentation: HRANDFIELD</a>
	 */
	default Flux<ByteBuffer> hRandField(ByteBuffer key, long count) {

		Assert.notNull(key, "Key must not be null");

		return hRandField(Mono.just(HRandFieldCommand.key(key).count(count))).flatMap(CommandResponse::getOutput);
	}

	/**
	 * Return a random field from the hash along with its value stored at {@code key}. If the provided {@code count}
	 * argument is positive, return a list of distinct fields, capped either at {@code count} or the hash size. If
	 * {@code count} is negative, the behavior changes and the command is allowed to return the same field multiple times.
	 * In this case, the number of returned fields is the absolute value of the specified count.
	 *
	 * @param key must not be {@literal null}.
	 * @param count number of fields to return.
	 * @return {@literal null} if key does not exist or when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/hrandfield">Redis Documentation: HRANDFIELD</a>
	 */
	default Flux<Map.Entry<ByteBuffer, ByteBuffer>> hRandFieldWithValues(ByteBuffer key, long count) {

		Assert.notNull(key, "Key must not be null");

		return hRandFieldWithValues(Mono.just(HRandFieldCommand.key(key).count(count))).flatMap(CommandResponse::getOutput);
	}

	/**
	 * Get random fields of hash at {@literal key}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/hrandfield">Redis Documentation: HRANDFIELD</a>
	 */
	Flux<CommandResponse<HRandFieldCommand, Flux<ByteBuffer>>> hRandField(Publisher<HRandFieldCommand> commands);

	/**
	 * Get random fields along their values of hash at {@literal key}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/hrandfield">Redis Documentation: HRANDFIELD</a>
	 */
	Flux<CommandResponse<HRandFieldCommand, Flux<Map.Entry<ByteBuffer, ByteBuffer>>>> hRandFieldWithValues(
			Publisher<HRandFieldCommand> commands);

	/**
	 * Get key set (fields) of hash at {@literal key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/hkeys">Redis Documentation: HKEYS</a>
	 */
	default Flux<ByteBuffer> hKeys(ByteBuffer key) {

		Assert.notNull(key, "Key must not be null");

		return hKeys(Mono.just(new KeyCommand(key))).flatMap(CommandResponse::getOutput);
	}

	/**
	 * Get key set (fields) of hash at {@literal key}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/hkeys">Redis Documentation: HKEYS</a>
	 */
	Flux<CommandResponse<KeyCommand, Flux<ByteBuffer>>> hKeys(Publisher<KeyCommand> commands);

	/**
	 * Get entry set (values) of hash at {@literal key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/hvals">Redis Documentation: HVALS</a>
	 */
	default Flux<ByteBuffer> hVals(ByteBuffer key) {

		Assert.notNull(key, "Key must not be null");

		return hVals(Mono.just(new KeyCommand(key))).flatMap(CommandResponse::getOutput);
	}

	/**
	 * Get entry set (values) of hash at {@literal key}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/hvals">Redis Documentation: HVALS</a>
	 */
	Flux<CommandResponse<KeyCommand, Flux<ByteBuffer>>> hVals(Publisher<KeyCommand> commands);

	/**
	 * Get entire hash stored at {@literal key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/hgetall">Redis Documentation: HGETALL</a>
	 */
	default Flux<Map.Entry<ByteBuffer, ByteBuffer>> hGetAll(ByteBuffer key) {

		Assert.notNull(key, "Key must not be null");

		return hGetAll(Mono.just(new KeyCommand(key))).flatMap(CommandResponse::getOutput);
	}

	/**
	 * Get entire hash stored at {@literal key}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/hgetall">Redis Documentation: HGETALL</a>
	 */
	Flux<CommandResponse<KeyCommand, Flux<Map.Entry<ByteBuffer, ByteBuffer>>>> hGetAll(Publisher<KeyCommand> commands);

	/**
	 * Use a {@link Flux} to iterate over entries in the hash at {@code key}. The resulting {@link Flux} acts as a cursor
	 * and issues {@code HSCAN} commands itself as long as the subscriber signals demand.
	 *
	 * @param key must not be {@literal null}.
	 * @return the {@link Flux} emitting {@link java.util.Map.Entry entries} one by one.
	 * @throws IllegalArgumentException in case the given key is {@literal null}.
	 * @see <a href="https://redis.io/commands/hscan">Redis Documentation: HSCAN</a>
	 * @since 2.1
	 */
	default Flux<Map.Entry<ByteBuffer, ByteBuffer>> hScan(ByteBuffer key) {
		return hScan(key, ScanOptions.NONE);
	}

	/**
	 * Use a {@link Flux} to iterate over entries in the hash at {@code key} given {@link ScanOptions}. The resulting
	 * {@link Flux} acts as a cursor and issues {@code HSCAN} commands itself as long as the subscriber signals demand.
	 *
	 * @param key must not be {@literal null}.
	 * @param options must not be {@literal null}. Use {@link ScanOptions#NONE} instead.
	 * @return the {@link Flux} emitting the raw {@link java.util.Map.Entry entries} one by one.
	 * @throws IllegalArgumentException in case one of the required arguments is {@literal null}.
	 * @see <a href="https://redis.io/commands/hscan">Redis Documentation: HSCAN</a>
	 * @since 2.1
	 */
	default Flux<Map.Entry<ByteBuffer, ByteBuffer>> hScan(ByteBuffer key, ScanOptions options) {

		return hScan(Mono.just(KeyScanCommand.key(key).withOptions(options))).map(CommandResponse::getOutput)
				.flatMap(it -> it);
	}

	/**
	 * Use a {@link Flux} to iterate over entries in the hash at {@code key}. The resulting {@link Flux} acts as a cursor
	 * and issues {@code HSCAN} commands itself as long as the subscriber signals demand.
	 *
	 * @param commands must not be {@literal null}.
	 * @return the {@link Flux} emitting {@link CommandResponse} one by one.
	 * @see <a href="https://redis.io/commands/hscan">Redis Documentation: HSCAN</a>
	 * @since 2.1
	 */
	Flux<CommandResponse<KeyCommand, Flux<Map.Entry<ByteBuffer, ByteBuffer>>>> hScan(Publisher<KeyScanCommand> commands);

	/**
	 * @author Christoph Strobl
	 * @see <a href="https://redis.io/commands/hstrlen">Redis Documentation: HSTRLEN</a>
	 * @since 2.1
	 */
	class HStrLenCommand extends KeyCommand {

		private ByteBuffer field;

		/**
		 * Creates a new {@link HStrLenCommand} given a {@code key}.
		 *
		 * @param key can be {@literal null}.
		 * @param field must not be {@literal null}.
		 */
		private HStrLenCommand(@Nullable ByteBuffer key, ByteBuffer field) {

			super(key);
			this.field = field;
		}

		/**
		 * Specify the {@code field} within the hash to get the length of the {@code value} of.ø
		 *
		 * @param field must not be {@literal null}.
		 * @return new instance of {@link HStrLenCommand}.
		 */
		public static HStrLenCommand lengthOf(ByteBuffer field) {

			Assert.notNull(field, "Field must not be null");
			return new HStrLenCommand(null, field);
		}

		/**
		 * Define the {@code key} the hash is stored at.
		 *
		 * @param key must not be {@literal null}.
		 * @return new instance of {@link HStrLenCommand}.
		 */
		public HStrLenCommand from(ByteBuffer key) {
			return new HStrLenCommand(key, field);
		}

		/**
		 * @return the field.
		 */
		public ByteBuffer getField() {
			return field;
		}
	}

	/**
	 * Get the length of the value associated with {@code field}. If either the {@code key} or the {@code field} do not
	 * exist, {@code 0} is emitted.
	 *
	 * @param key must not be {@literal null}.
	 * @param field must not be {@literal null}.
	 * @return never {@literal null}.
	 * @since 2.1
	 */
	default Mono<Long> hStrLen(ByteBuffer key, ByteBuffer field) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(field, "Field must not be null");

		return hStrLen(Mono.just(HStrLenCommand.lengthOf(field).from(key))).next().map(NumericResponse::getOutput);
	}

	/**
	 * Get the length of the value associated with {@code field}. If either the {@code key} or the {@code field} do not
	 * exist, {@code 0} is emitted.
	 *
	 * @param commands must not be {@literal null}.
	 * @return never {@literal null}.
	 * @since 2.1
	 */
	Flux<NumericResponse<HStrLenCommand, Long>> hStrLen(Publisher<HStrLenCommand> commands);

	/**
	 * @since 3.5
	 */
	class HashExpireCommand extends HashFieldsCommand {

		private final Expiration expiration;
		private final ExpirationOptions options;

		private HashExpireCommand(@Nullable ByteBuffer key, List<ByteBuffer> fields, Expiration expiration,
				ExpirationOptions options) {

			super(key, fields);

			this.expiration = expiration;
			this.options = options;
		}

		/**
		 * Creates a new {@link HashExpireCommand}.
		 *
		 * @param fields the {@code field} names to apply expiration to
		 * @param timeout the actual timeout
		 * @param unit the unit of measure for the {@code timeout}.
		 * @return new instance of {@link HashExpireCommand}.
		 */
		public static HashExpireCommand expire(List<ByteBuffer> fields, long timeout, TimeUnit unit) {

			Assert.notNull(fields, "Field must not be null");
			return expire(fields, Expiration.from(timeout, unit));
		}

		/**
		 * Creates a new {@link HashExpireCommand}.
		 *
		 * @param fields the {@code field} names to apply expiration to.
		 * @param ttl the actual timeout.
		 * @return new instance of {@link HashExpireCommand}.
		 */
		public static HashExpireCommand expire(List<ByteBuffer> fields, Duration ttl) {

			Assert.notNull(fields, "Field must not be null");
			return expire(fields, Expiration.from(ttl));
		}

		/**
		 * Creates a new {@link HashExpireCommand}.
		 *
		 * @param fields the {@code field} names to apply expiration to
		 * @param expiration the {@link Expiration} to apply to the given {@literal fields}.
		 * @return new instance of {@link HashExpireCommand}.
		 */
		public static HashExpireCommand expire(List<ByteBuffer> fields, Expiration expiration) {
			return new HashExpireCommand(null, fields, expiration, ExpirationOptions.none());
		}

		/**
		 * Creates a new {@link HashExpireCommand}.
		 *
		 * @param fields the {@code field} names to apply expiration to
		 * @param ttl the unix point in time when to expire the given {@literal fields}.
		 * @param precision can be {@link TimeUnit#SECONDS} or {@link TimeUnit#MILLISECONDS}.
		 * @return new instance of {@link HashExpireCommand}.
		 */
		public static HashExpireCommand expireAt(List<ByteBuffer> fields, Instant ttl, TimeUnit precision) {

			if (precision.compareTo(TimeUnit.MILLISECONDS) > 0) {
				return expire(fields, Expiration.unixTimestamp(ttl.getEpochSecond(), TimeUnit.SECONDS));
			}

			return expire(fields, Expiration.unixTimestamp(ttl.toEpochMilli(), TimeUnit.MILLISECONDS));
		}

		/**
		 * @param key the {@literal key} from which to expire the {@literal fields} from.
		 * @return new instance of {@link HashExpireCommand}.
		 */
		public HashExpireCommand from(ByteBuffer key) {
			return new HashExpireCommand(key, getFields(), expiration, options);
		}

		/**
		 * @param options additional options to be sent along with the command.
		 * @return new instance of {@link HashExpireCommand}.
		 */
		public HashExpireCommand withOptions(ExpirationOptions options) {
			return new HashExpireCommand(getKey(), getFields(), getExpiration(), options);
		}

		public Expiration getExpiration() {
			return expiration;
		}

		public ExpirationOptions getOptions() {
			return options;
		}
	}

	/**
	 * Expire a given {@literal field} after a given {@link Duration} of time, measured in milliseconds, has passed.
	 *
	 * @param key must not be {@literal null}.
	 * @param field must not be {@literal null}.
	 * @param duration must not be {@literal null}.
	 * @return a {@link Mono} emitting the expiration result - {@code 2} indicating the specific field is deleted already
	 *         due to expiration, or provided expiry interval is 0; {@code 1} indicating expiration time is set/updated;
	 *         {@code 0} indicating the expiration time is not set (a provided NX | XX | GT | LT condition is not met);
	 *         {@code -2} indicating there is no such field;
	 * @see <a href="https://redis.io/commands/hexpire">Redis Documentation: HEXPIRE</a>
	 * @since 3.5
	 */
	default Mono<Long> hExpire(ByteBuffer key, Duration duration, ByteBuffer field) {

		Assert.notNull(duration, "Duration must not be null");

		return hExpire(key, duration, Collections.singletonList(field)).singleOrEmpty();
	}

	/**
	 * Expire a {@link List} of {@literal field} after a given {@link Duration} of time, measured in milliseconds, has
	 * passed.
	 *
	 * @param key must not be {@literal null}.
	 * @param fields must not be {@literal null}.
	 * @param duration must not be {@literal null}.
	 * @return a {@link Flux} emitting the expiration results one by one, {@code 2} indicating the specific field is
	 *         deleted already due to expiration, or provided expiry interval is 0; {@code 1} indicating expiration time
	 *         is set/updated; {@code 0} indicating the expiration time is not set (a provided NX | XX | GT | LT condition
	 *         is not met); {@code -2} indicating there is no such field;
	 * @see <a href="https://redis.io/commands/hexpire">Redis Documentation: HEXPIRE</a>
	 * @since 3.5
	 */
	default Flux<Long> hExpire(ByteBuffer key, Duration duration, List<ByteBuffer> fields) {

		Assert.notNull(duration, "Duration must not be null");

		return applyHashFieldExpiration(Flux.just(HashExpireCommand.expire(fields, duration).from(key)))
				.mapNotNull(NumericResponse::getOutput);
	}

	/**
	 * Expire a {@link List} of {@literal field} after a given {@link Duration} of time, measured in milliseconds, has
	 * passed.
	 *
	 * @param commands must not be {@literal null}.
	 * @return a {@link Flux} emitting the expiration results one by one, {@code 2} indicating the specific field is
	 *         deleted already due to expiration, or provided expiry interval is 0; {@code 1} indicating expiration time
	 *         is set/updated; {@code 0} indicating the expiration time is not set (a provided NX | XX | GT | LT condition
	 *         is not met); {@code -2} indicating there is no such field;
	 * @since 3.5
	 * @see <a href="https://redis.io/commands/hexpire">Redis Documentation: HEXPIRE</a>
	 */
	Flux<NumericResponse<HashExpireCommand, Long>> applyHashFieldExpiration(Publisher<HashExpireCommand> commands);

	/**
	 * Expire a given {@literal field} after a given {@link Duration} of time, measured in milliseconds, has passed.
	 *
	 * @param key must not be {@literal null}.
	 * @param field must not be {@literal null}.
	 * @param duration must not be {@literal null}.
	 * @return a {@link Mono} emitting the expiration result - {@code 2} indicating the specific field is deleted already
	 *         due to expiration, or provided expiry interval is 0; {@code 1} indicating expiration time is set/updated;
	 *         {@code 0} indicating the expiration time is not set (a provided NX | XX | GT | LT condition is not met);
	 *         {@code -2} indicating there is no such field;
	 * @see <a href="https://redis.io/commands/hexpire">Redis Documentation: HEXPIRE</a>
	 * @since 3.5
	 */
	default Mono<Long> hpExpire(ByteBuffer key, Duration duration, ByteBuffer field) {

		Assert.notNull(duration, "Duration must not be null");

		return hpExpire(key, duration, Collections.singletonList(field)).singleOrEmpty();
	}

	/**
	 * Expire a {@link List} of {@literal field} after a given {@link Duration} of time, measured in milliseconds, has
	 * passed.
	 *
	 * @param key must not be {@literal null}.
	 * @param fields must not be {@literal null}.
	 * @param duration must not be {@literal null}.
	 * @return a {@link Flux} emitting the expiration results one by one, {@code 2} indicating the specific field is
	 *         deleted already due to expiration, or provided expiry interval is 0; {@code 1} indicating expiration time
	 *         is set/updated; {@code 0} indicating the expiration time is not set (a provided NX | XX | GT | LT condition
	 *         is not met); {@code -2} indicating there is no such field;
	 * @see <a href="https://redis.io/commands/hexpire">Redis Documentation: HEXPIRE</a>
	 * @since 3.5
	 */
	default Flux<Long> hpExpire(ByteBuffer key, Duration duration, List<ByteBuffer> fields) {

		Assert.notNull(duration, "Duration must not be null");

		return applyHashFieldExpiration(Flux.just(new HashExpireCommand(key, fields,
				Expiration.from(duration.toMillis(), TimeUnit.MILLISECONDS), ExpirationOptions.none())))
				.mapNotNull(NumericResponse::getOutput);
	}

	/**
	 * Expire a given {@literal field} in a given {@link Instant} of time, indicated as an absolute
	 * <a href="https://en.wikipedia.org/wiki/Unix_time">Unix timestamp</a> in seconds since Unix epoch
	 *
	 * @param key must not be {@literal null}.
	 * @param field must not be {@literal null}.
	 * @param expireAt must not be {@literal null}.
	 * @return a {@link Mono} emitting the expiration result - {@code 2} indicating the specific field is deleted already
	 *         due to expiration, or provided expiry interval is in the past; {@code 1} indicating expiration time is
	 *         set/updated; {@code 0} indicating the expiration time is not set (a provided NX | XX | GT | LT condition is
	 *         not met); {@code -2} indicating there is no such field;
	 * @see <a href="https://redis.io/commands/hexpireat">Redis Documentation: HEXPIREAT</a>
	 * @since 3.5
	 */
	default Mono<Long> hExpireAt(ByteBuffer key, Instant expireAt, ByteBuffer field) {

		Assert.notNull(expireAt, "Duration must not be null");

		return hExpireAt(key, expireAt, Collections.singletonList(field)).singleOrEmpty();
	}

	/**
	 * Expire a {@link List} of {@literal field} in a given {@link Instant} of time, indicated as an absolute
	 * <a href="https://en.wikipedia.org/wiki/Unix_time">Unix timestamp</a> in seconds since Unix epoch
	 *
	 * @param key must not be {@literal null}.
	 * @param fields must not be {@literal null}.
	 * @param expireAt must not be {@literal null}.
	 * @return a {@link Flux} emitting the expiration results one by one, {@code 2} indicating the specific field is
	 *         deleted already due to expiration, or provided expiry interval is in the past; {@code 1} indicating
	 *         expiration time is set/updated; {@code 0} indicating the expiration time is not set (a provided NX | XX |
	 *         GT | LT condition is not met); {@code -2} indicating there is no such field;
	 * @see <a href="https://redis.io/commands/hexpireat">Redis Documentation: HEXPIREAT</a>
	 * @since 3.5
	 */
	default Flux<Long> hExpireAt(ByteBuffer key, Instant expireAt, List<ByteBuffer> fields) {

		Assert.notNull(expireAt, "Duration must not be null");

		return applyHashFieldExpiration(Flux.just(HashExpireCommand.expireAt(fields, expireAt, TimeUnit.SECONDS).from(key)))
				.mapNotNull(NumericResponse::getOutput);
	}

	/**
	 * Expire a given {@literal field} in a given {@link Instant} of time, indicated as an absolute
	 * <a href="https://en.wikipedia.org/wiki/Unix_time">Unix timestamp</a> in milliseconds since Unix epoch
	 *
	 * @param key must not be {@literal null}.
	 * @param field must not be {@literal null}.
	 * @param expireAt must not be {@literal null}.
	 * @return a {@link Mono} emitting the expiration result - {@code 2} indicating the specific field is deleted already
	 *         due to expiration, or provided expiry interval is in the past; {@code 1} indicating expiration time is
	 *         set/updated; {@code 0} indicating the expiration time is not set (a provided NX | XX | GT | LT condition is
	 *         not met); {@code -2} indicating there is no such field;
	 * @see <a href="https://redis.io/commands/hpexpireat">Redis Documentation: HPEXPIREAT</a>
	 * @since 3.5
	 */
	default Mono<Long> hpExpireAt(ByteBuffer key, Instant expireAt, ByteBuffer field) {

		Assert.notNull(expireAt, "Duration must not be null");

		return hpExpireAt(key, expireAt, Collections.singletonList(field)).singleOrEmpty();
	}

	/**
	 * Expire a {@link List} of {@literal field} in a given {@link Instant} of time, indicated as an absolute
	 * <a href="https://en.wikipedia.org/wiki/Unix_time">Unix timestamp</a> in milliseconds since Unix epoch
	 *
	 * @param key must not be {@literal null}.
	 * @param fields must not be {@literal null}.
	 * @param expireAt must not be {@literal null}.
	 * @return a {@link Flux} emitting the expiration results one by one, {@code 2} indicating the specific field is
	 *         deleted already due to expiration, or provided expiry interval is in the past; {@code 1} indicating
	 *         expiration time is set/updated; {@code 0} indicating the expiration time is not set (a provided NX | XX |
	 *         GT | LT condition is not met); {@code -2} indicating there is no such field;
	 * @see <a href="https://redis.io/commands/hpexpireat">Redis Documentation: HPEXPIREAT</a>
	 * @since 3.5
	 */
	default Flux<Long> hpExpireAt(ByteBuffer key, Instant expireAt, List<ByteBuffer> fields) {

		Assert.notNull(expireAt, "Duration must not be null");

		return applyHashFieldExpiration(
				Flux.just(HashExpireCommand.expireAt(fields, expireAt, TimeUnit.MILLISECONDS).from(key)))
				.mapNotNull(NumericResponse::getOutput);
	}

	/**
	 * Persist a given {@literal field} removing any associated expiration, measured as absolute
	 * <a href="https://en.wikipedia.org/wiki/Unix_time">Unix timestamp</a> in seconds since Unix epoch
	 *
	 * @param key must not be {@literal null}.
	 * @param field must not be {@literal null}.
	 * @return a {@link Mono} emitting the persist result - {@code 1} indicating expiration time is removed; {@code -1}
	 *         field has no expiration time to be removed; {@code -2} indicating there is no such field;
	 * @see <a href="https://redis.io/commands/hpersist">Redis Documentation: HPERSIST</a>
	 * @since 3.5
	 */
	default Mono<Long> hPersist(ByteBuffer key, ByteBuffer field) {
		return hPersist(key, Collections.singletonList(field)).singleOrEmpty();
	}

	/**
	 * Persist a given {@link List} of {@literal field} removing any associated expiration.
	 *
	 * @param key must not be {@literal null}.
	 * @param fields must not be {@literal null}.
	 * @return a {@link Flux} emitting the persisting results one by one - {@code 1} indicating expiration time is
	 *         removed; {@code -1} field has no expiration time to be removed; {@code -2} indicating there is no such
	 *         field;
	 * @see <a href="https://redis.io/commands/hpersist">Redis Documentation: HPERSIST</a>
	 * @since 3.5
	 */
	default Flux<Long> hPersist(ByteBuffer key, List<ByteBuffer> fields) {
		return hPersist(Flux.just(new HashFieldsCommand(key, fields))).mapNotNull(NumericResponse::getOutput);
	}

	/**
	 * Persist a given {@link List} of {@literal field} removing any associated expiration.
	 *
	 * @param commands must not be {@literal null}.
	 * @return a {@link Flux} emitting the persisting results one by one - {@code 1} indicating expiration time is
	 *         removed; {@code -1} field has no expiration time to be removed; {@code -2} indicating there is no such
	 *         field; * @since 3.5
	 * @see <a href="https://redis.io/commands/hpersist">Redis Documentation: HPERSIST</a>
	 */
	Flux<NumericResponse<HashFieldsCommand, Long>> hPersist(Publisher<HashFieldsCommand> commands);

	/**
	 * Returns the time-to-live of a given {@literal field} in seconds.
	 *
	 * @param key must not be {@literal null}.
	 * @param field must not be {@literal null}.
	 * @return a {@link Mono} emitting the TTL result - the time to live in seconds; or a negative value to signal an
	 *         error. The command returns {@code -1} if the key exists but has no associated expiration time. The command
	 *         returns {@code -2} if the key does not exist;
	 * @see <a href="https://redis.io/commands/httl">Redis Documentation: HTTL</a>
	 * @since 3.5
	 */
	default Mono<Long> hTtl(ByteBuffer key, ByteBuffer field) {
		return hTtl(key, Collections.singletonList(field)).singleOrEmpty();
	}

	/**
	 * Returns the time-to-live of all the given {@literal field} in the {@link List} in seconds.
	 *
	 * @param key must not be {@literal null}.
	 * @param fields must not be {@literal null}.
	 * @return a {@link Flux} emitting the TTL results one by one - the time to live in seconds; or a negative value to
	 *         signal an error. The command returns {@code -1} if the key exists but has no associated expiration time.
	 *         The command returns {@code -2} if the key does not exist;
	 * @see <a href="https://redis.io/commands/httl">Redis Documentation: HTTL</a>
	 * @since 3.5
	 */
	default Flux<Long> hTtl(ByteBuffer key, List<ByteBuffer> fields) {
		return hTtl(Flux.just(new HashFieldsCommand(key, fields))).mapNotNull(NumericResponse::getOutput);
	}

	/**
	 * Returns the time-to-live of all the given {@literal field} in the {@link List} in seconds.
	 *
	 * @param commands must not be {@literal null}.
	 * @return a {@link Flux} emitting the persisting results one by one - the time to live in seconds; or a negative
	 *         value to signal an error. The command returns {@code -1} if the key exists but has no associated expiration
	 *         time. The command returns {@code -2} if the key does not exist;
	 * @since 3.5
	 * @see <a href="https://redis.io/commands/httl">Redis Documentation: HTTL</a>
	 */
	Flux<NumericResponse<HashFieldsCommand, Long>> hTtl(Publisher<HashFieldsCommand> commands);

	/**
	 * Returns the time-to-live of a given {@literal field} in milliseconds.
	 *
	 * @param key must not be {@literal null}.
	 * @param field must not be {@literal null}.
	 * @return a {@link Mono} emitting the TTL result - the time to live in milliseconds; or a negative value to signal an
	 *         error. The command returns {@code -1} if the key exists but has no associated expiration time. The command
	 *         returns {@code -2} if the key does not exist;
	 * @see <a href="https://redis.io/commands/hpttl">Redis Documentation: HPTTL</a>
	 * @since 3.5
	 */
	default Mono<Long> hpTtl(ByteBuffer key, ByteBuffer field) {
		return hpTtl(key, Collections.singletonList(field)).singleOrEmpty();
	}

	/**
	 * Returns the time-to-live of all the given {@literal field} in the {@link List} in milliseconds.
	 *
	 * @param key must not be {@literal null}.
	 * @param fields must not be {@literal null}.
	 * @return a {@link Flux} emitting the TTL results one by one - the time to live in milliseconds; or a negative value
	 *         to signal an error. The command returns {@code -1} if the key exists but has no associated expiration time.
	 *         The command returns {@code -2} if the key does not exist;
	 * @see <a href="https://redis.io/commands/hpttl">Redis Documentation: HPTTL</a>
	 * @since 3.5
	 */
	default Flux<Long> hpTtl(ByteBuffer key, List<ByteBuffer> fields) {
		return hpTtl(Flux.just(new HashFieldsCommand(key, fields))).mapNotNull(NumericResponse::getOutput);
	}

	/**
	 * Returns the time-to-live of all the given {@literal field} in the {@link List} in milliseconds.
	 *
	 * @param commands must not be {@literal null}.
	 * @return a {@link Flux} emitting the persisting results one by one - the time to live in milliseconds; or a negative
	 *         value to signal an error. The command returns {@code -1} if the key exists but has no associated expiration
	 *         time. The command returns {@code -2} if the key does not exist;
	 * @since 3.5
	 * @see <a href="https://redis.io/commands/hpttl">Redis Documentation: HPTTL</a>
	 */
	Flux<NumericResponse<HashFieldsCommand, Long>> hpTtl(Publisher<HashFieldsCommand> commands);


    /**
     * {@literal HGETDEL} {@link Command}.
     *
     * @author Viktoriya Kutsarova
     * @see <a href="https://redis.io/commands/hgetdel">Redis Documentation: HGETDEL</a>
     */
    class HGetDelCommand extends HashFieldsCommand {

        private HGetDelCommand(@Nullable ByteBuffer key, List<ByteBuffer> fields) {
            super(key, fields);
        }

        /**
         * Creates a new {@link HGetDelCommand} given a {@link ByteBuffer field name}.
         *
         * @param field must not be {@literal null}.
         * @return a new {@link HGetDelCommand} for a {@link ByteBuffer field name}.
         */
        public static HGetDelCommand field(ByteBuffer field) {

            Assert.notNull(field, "Field must not be null");

            return new HGetDelCommand(null, Collections.singletonList(field));
        }

        /**
         * Creates a new {@link HGetDelCommand} given a {@link Collection} of field names.
         *
         * @param fields must not be {@literal null}.
         * @return a new {@link HGetDelCommand} for a {@link Collection} of field names.
         */
        public static HGetDelCommand fields(Collection<ByteBuffer> fields) {

            Assert.notNull(fields, "Fields must not be null");

            return new HGetDelCommand(null, new ArrayList<>(fields));
        }

        /**
         * Applies the hash {@literal key}. Constructs a new command instance with all previously configured properties.
         *
         * @param key must not be {@literal null}.
         * @return a new {@link HGetDelCommand} with {@literal key} applied.
         */
        public HGetDelCommand from(ByteBuffer key) {

            Assert.notNull(key, "Key must not be null");

            return new HGetDelCommand(key, getFields());
        }
    }


    /**
     * Get and delete the value of one or more {@literal fields} from hash at {@literal key}. Values are returned in the
     * order of the requested keys. Absent field values are represented using {@literal null} in the resulting {@link List}.
     * When the last field is deleted, the key will also be deleted.
     *
     * @param key must not be {@literal null}.
     * @param fields must not be {@literal null}.
     * @return never {@literal null}.
     * @see <a href="https://redis.io/commands/hgetdel">Redis Documentation: HGETDEL</a>
     */
    default Mono<List<ByteBuffer>> hGetDel(ByteBuffer key, Collection<ByteBuffer> fields) {

        Assert.notNull(key, "Key must not be null");
        Assert.notNull(fields, "Fields must not be null");

        return hGetDel(Mono.just(HGetDelCommand.fields(fields).from(key))).next().map(MultiValueResponse::getOutput);
    }

    /**
     * Get and delete the value of one or more {@literal fields} from hash at {@literal key}. Values are returned in the
     * order of the requested keys. Absent field values are represented using {@literal null} in the resulting {@link List}.
     * When the last field is deleted, the key will also be deleted.
     *
     * @param commands must not be {@literal null}.
     * @return never {@literal null}.
     * @see <a href="https://redis.io/commands/hgetdel">Redis Documentation: HGETDEL</a>
     */
    Flux<MultiValueResponse<HGetDelCommand, ByteBuffer>> hGetDel(Publisher<HGetDelCommand> commands);

    class HGetExCommand extends HashFieldsCommand {

        private final Expiration expiration;

        private HGetExCommand(@Nullable ByteBuffer key, List<ByteBuffer> fields, Expiration expiration) {

            super(key, fields);

            this.expiration = expiration;
        }

        /**
         * Creates a new {@link HGetExCommand}.
         *
         * @param fields the {@code fields} names to apply expiration to
         * @param expiration the {@link Expiration} to apply to the given {@literal fields}.
         * @return new instance of {@link HGetExCommand}.
         */
        public static HGetExCommand expire(List<ByteBuffer> fields, Expiration expiration) {
            return new HGetExCommand(null, fields, expiration);
        }

        /**
         * @param key the {@literal key} from which to expire the {@literal fields} from.
         * @return new instance of {@link HashExpireCommand}.
         */
        public HGetExCommand from(ByteBuffer key) {
            return new HGetExCommand(key, getFields(), expiration);
        }

        /**
         * Creates a new {@link HGetExCommand}.
         *
         * @param fields the {@code fields} names to apply expiration to
         * @return new instance of {@link HGetExCommand}.
         */
        public HGetExCommand fields(Collection<ByteBuffer> fields) {
            return new HGetExCommand(getKey(), new ArrayList<>(fields), expiration);
        }

        public Expiration getExpiration() {
            return expiration;
        }
    }

    /**
     * Get the value of one or more {@literal fields} from hash at {@literal key} and optionally set expiration time or
     * time-to-live (TTL) for given {@literal fields}.
     *
     * @param key must not be {@literal null}.
     * @param fields must not be {@literal null}.
     * @return never {@literal null}.
     * @see <a href="https://redis.io/commands/hgetex">Redis Documentation: HGETEX</a>
     */
    default Mono<List<ByteBuffer>> hGetEx(ByteBuffer key, Expiration expiration, List<ByteBuffer> fields) {

        Assert.notNull(key, "Key must not be null");
        Assert.notNull(fields, "Fields must not be null");

        return hGetEx(Mono.just(HGetExCommand.expire(fields, expiration).from(key))).next().map(MultiValueResponse::getOutput);
    }

    /**
     * Get the value of one or more {@literal fields} from hash at {@literal key} and optionally set expiration time or
     * time-to-live (TTL) for given {@literal fields}.
     *
     * @param commands must not be {@literal null}.
     * @return never {@literal null}.
     * @see <a href="https://redis.io/commands/hgetex">Redis Documentation: HGETEX</a>
     */
    Flux<MultiValueResponse<HGetExCommand, ByteBuffer>> hGetEx(Publisher<HGetExCommand> commands);

    /**
     * {@literal HSETEX} {@link Command}.
     *
     * @author Viktoriya Kutsarova
     * @see <a href="https://redis.io/commands/hsetex">Redis Documentation: HSETEX</a>
     */
    class HSetExCommand extends KeyCommand {

        private final Map<ByteBuffer, ByteBuffer> fieldValueMap;
        private final RedisHashCommands.HashFieldSetOption condition;
        private final Expiration expiration;

        private HSetExCommand(@Nullable ByteBuffer key, Map<ByteBuffer, ByteBuffer> fieldValueMap,
                              RedisHashCommands.HashFieldSetOption condition, Expiration expiration) {
            super(key);
            this.fieldValueMap = fieldValueMap;
            this.condition = condition;
            this.expiration = expiration;
        }

        /**
         * Creates a new {@link HSetExCommand} for setting field-value pairs with condition and expiration.
         *
         * @param fieldValueMap the field-value pairs to set; must not be {@literal null}.
         * @param condition the condition for setting fields; must not be {@literal null}.
         * @param expiration the expiration to apply; must not be {@literal null}.
         * @return new instance of {@link HSetExCommand}.
         */
        public static HSetExCommand setWithConditionAndExpiration(Map<ByteBuffer, ByteBuffer> fieldValueMap,
                                                                  RedisHashCommands.HashFieldSetOption condition, Expiration expiration) {
            return new HSetExCommand(null, fieldValueMap, condition, expiration);
        }

        /**
         * Applies the hash {@literal key}. Constructs a new command instance with all previously configured properties.
         *
         * @param key must not be {@literal null}.
         * @return a new {@link HSetExCommand} with {@literal key} applied.
         */
        public HSetExCommand from(ByteBuffer key) {
            Assert.notNull(key, "Key must not be null");
            return new HSetExCommand(key, fieldValueMap, condition, expiration);
        }

        /**
         * @return the field-value map.
         */
        public Map<ByteBuffer, ByteBuffer> getFieldValueMap() {
            return fieldValueMap;
        }

        /**
         * @return the condition for setting fields.
         */
        public RedisHashCommands.HashFieldSetOption getCondition() {
            return condition;
        }

        /**
         * @return the expiration to apply.
         */
        public Expiration getExpiration() {
            return expiration;
        }
    }

    /**
     * Set field-value pairs in hash at {@literal key} with condition and expiration.
     *
     * @param key must not be {@literal null}.
     * @param fieldValueMap the field-value pairs to set; must not be {@literal null}.
     * @param condition the condition for setting fields; must not be {@literal null}.
     * @param expiration the expiration to apply; must not be {@literal null}.
     * @return never {@literal null}.
     * @see <a href="https://redis.io/commands/hsetex">Redis Documentation: HSETEX</a>
     */
    default Mono<Boolean> hSetEx(ByteBuffer key, Map<ByteBuffer, ByteBuffer> fieldValueMap,
                                 RedisHashCommands.HashFieldSetOption condition, Expiration expiration) {

        Assert.notNull(key, "Key must not be null");
        Assert.notNull(fieldValueMap, "Field-value map must not be null");
        Assert.notNull(condition, "Condition must not be null");
        Assert.notNull(expiration, "Expiration must not be null");

        return hSetEx(Mono.just(HSetExCommand.setWithConditionAndExpiration(fieldValueMap, condition, expiration).from(key)))
                .next().map(CommandResponse::getOutput);
    }

    /**
     * Set field-value pairs in hash at {@literal key} with condition and expiration.
     *
     * @param commands must not be {@literal null}.
     * @return never {@literal null}.
     * @see <a href="https://redis.io/commands/hsetex">Redis Documentation: HSETEX</a>
     */
    Flux<BooleanResponse<HSetExCommand>> hSetEx(Publisher<HSetExCommand> commands);
}
