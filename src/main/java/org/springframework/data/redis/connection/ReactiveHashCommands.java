/*
 * Copyright 2016-2019 the original author or authors.
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
import java.util.Map;

import org.reactivestreams.Publisher;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.connection.ReactiveRedisConnection.BooleanResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.Command;
import org.springframework.data.redis.connection.ReactiveRedisConnection.CommandResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.KeyCommand;
import org.springframework.data.redis.connection.ReactiveRedisConnection.MultiValueResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.NumericResponse;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Redis Hash commands executed using reactive infrastructure.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.0
 */
public interface ReactiveHashCommands {

	/**
	 * {@literal HSET} {@link Command}.
	 *
	 * @author Christoph Strobl
	 * @see <a href="http://redis.io/commands/hset">Redis Documentation: HSET</a>
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

			Assert.notNull(value, "Value must not be null!");

			return new HSetCommand(null, Collections.singletonMap(SINGLE_VALUE_KEY, value), Boolean.TRUE);
		}

		/**
		 * Creates a new {@link HSetCommand} given a {@link Map} of field values.
		 *
		 * @param fieldValueMap must not be {@literal null}.
		 * @return a new {@link HSetCommand} for a {@link Map} of field values.
		 */
		public static HSetCommand fieldValues(Map<ByteBuffer, ByteBuffer> fieldValueMap) {

			Assert.notNull(fieldValueMap, "Field values map must not be null!");

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

			Assert.notNull(field, "Field not be null!");

			return new HSetCommand(getKey(), Collections.singletonMap(field, fieldValueMap.get(SINGLE_VALUE_KEY)), upsert);
		}

		/**
		 * Applies the {@literal key}. Constructs a new command instance with all previously configured properties.
		 *
		 * @param key must not be {@literal null}.
		 * @return a new {@link HSetCommand} with {@literal key} applied.
		 */
		public HSetCommand forKey(ByteBuffer key) {

			Assert.notNull(key, "Key not be null!");

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
	 * @see <a href="http://redis.io/commands/hset">Redis Documentation: HSET</a>
	 */
	default Mono<Boolean> hSet(ByteBuffer key, ByteBuffer field, ByteBuffer value) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(field, "Field must not be null!");
		Assert.notNull(value, "Value must not be null!");

		return hSet(Mono.just(HSetCommand.value(value).ofField(field).forKey(key))).next().map(BooleanResponse::getOutput);
	}

	/**
	 * Set the {@literal value} of a hash {@literal field}.
	 *
	 * @param key must not be {@literal null}.
	 * @param field must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/hsetnx">Redis Documentation: HSETNX</a>
	 */
	default Mono<Boolean> hSetNX(ByteBuffer key, ByteBuffer field, ByteBuffer value) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(field, "Field must not be null!");
		Assert.notNull(value, "Value must not be null!");

		return hSet(Mono.just(HSetCommand.value(value).ofField(field).forKey(key).ifValueNotExists())).next()
				.map(BooleanResponse::getOutput);
	}

	/**
	 * Set multiple hash fields to multiple values using data provided in {@literal fieldValueMap}.
	 *
	 * @param key must not be {@literal null}.
	 * @param fieldValueMap must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/hmset">Redis Documentation: HMSET</a>
	 */
	default Mono<Boolean> hMSet(ByteBuffer key, Map<ByteBuffer, ByteBuffer> fieldValueMap) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(fieldValueMap, "Field must not be null!");

		return hSet(Mono.just(HSetCommand.fieldValues(fieldValueMap).forKey(key))).next().map(it -> true);
	}

	/**
	 * Set the {@literal value} of a hash {@literal field}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/hset">Redis Documentation: HSET</a>
	 */
	Flux<BooleanResponse<HSetCommand>> hSet(Publisher<HSetCommand> commands);

	/**
	 * {@literal HGET} {@link Command}.
	 *
	 * @author Christoph Strobl
	 * @see <a href="http://redis.io/commands/hget">Redis Documentation: HGET</a>
	 */
	class HGetCommand extends KeyCommand {

		private List<ByteBuffer> fields;

		private HGetCommand(@Nullable ByteBuffer key, List<ByteBuffer> fields) {

			super(key);

			this.fields = fields;
		}

		/**
		 * Creates a new {@link HGetCommand} given a {@link ByteBuffer field name}.
		 *
		 * @param field must not be {@literal null}.
		 * @return a new {@link HGetCommand} for a {@link ByteBuffer field name}.
		 */
		public static HGetCommand field(ByteBuffer field) {

			Assert.notNull(field, "Field must not be null!");

			return new HGetCommand(null, Collections.singletonList(field));
		}

		/**
		 * Creates a new {@link HGetCommand} given a {@link Collection} of field names.
		 *
		 * @param fields must not be {@literal null}.
		 * @return a new {@link HGetCommand} for a {@link Collection} of field names.
		 */
		public static HGetCommand fields(Collection<ByteBuffer> fields) {

			Assert.notNull(fields, "Fields must not be null!");

			return new HGetCommand(null, new ArrayList<>(fields));
		}

		/**
		 * Applies the hash {@literal key}. Constructs a new command instance with all previously configured properties.
		 *
		 * @param key must not be {@literal null}.
		 * @return a new {@link HGetCommand} with {@literal key} applied.
		 */
		public HGetCommand from(ByteBuffer key) {

			Assert.notNull(key, "Key must not be null!");

			return new HGetCommand(key, fields);
		}

		/**
		 * @return never {@literal null}.
		 */
		public List<ByteBuffer> getFields() {
			return fields;
		}
	}

	/**
	 * Get value for given {@literal field} from hash at {@literal key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param field must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/hget">Redis Documentation: HGET</a>
	 */
	default Mono<ByteBuffer> hGet(ByteBuffer key, ByteBuffer field) {
		return hMGet(key, Collections.singletonList(field)).map(val -> val.isEmpty() ? null : val.iterator().next());
	}

	/**
	 * Get values for given {@literal fields} from hash at {@literal key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param fields must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/hmget">Redis Documentation: HMGET</a>
	 */
	default Mono<List<ByteBuffer>> hMGet(ByteBuffer key, Collection<ByteBuffer> fields) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(fields, "Fields must not be null!");

		return hMGet(Mono.just(HGetCommand.fields(fields).from(key))).next().map(MultiValueResponse::getOutput);
	}

	/**
	 * Get values for given {@literal fields} from hash at {@literal key}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/hmget">Redis Documentation: HMGET</a>
	 */
	Flux<MultiValueResponse<HGetCommand, ByteBuffer>> hMGet(Publisher<HGetCommand> commands);

	/**
	 * {@literal HEXISTS} {@link Command}.
	 *
	 * @author Christoph Strobl
	 * @see <a href="http://redis.io/commands/hexists">Redis Documentation: HEXISTS</a>
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

			Assert.notNull(field, "Field must not be null!");

			return new HExistsCommand(null, field);
		}

		/**
		 * Applies the hash {@literal key}. Constructs a new command instance with all previously configured properties.
		 *
		 * @param key must not be {@literal null}.
		 * @return a new {@link HExistsCommand} with {@literal key} applied.
		 */
		public HExistsCommand in(ByteBuffer key) {

			Assert.notNull(key, "Key must not be null!");

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
	 * @see <a href="http://redis.io/commands/hexists">Redis Documentation: HEXISTS</a>
	 */
	default Mono<Boolean> hExists(ByteBuffer key, ByteBuffer field) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(field, "Field must not be null!");

		return hExists(Mono.just(HExistsCommand.field(field).in(key))).next().map(BooleanResponse::getOutput);
	}

	/**
	 * Determine if given hash {@literal field} exists.
	 *
	 * @param commands
	 * @return
	 * @see <a href="http://redis.io/commands/hexists">Redis Documentation: HEXISTS</a>
	 */
	Flux<BooleanResponse<HExistsCommand>> hExists(Publisher<HExistsCommand> commands);

	/**
	 * @author Christoph Strobl
	 * @see <a href="http://redis.io/commands/hdel">Redis Documentation: HDEL</a>
	 */
	class HDelCommand extends KeyCommand {

		private final List<ByteBuffer> fields;

		private HDelCommand(@Nullable ByteBuffer key, List<ByteBuffer> fields) {

			super(key);

			this.fields = fields;
		}

		/**
		 * Creates a new {@link HDelCommand} given a {@link ByteBuffer field name}.
		 *
		 * @param field must not be {@literal null}.
		 * @return a new {@link HDelCommand} for a {@link ByteBuffer field name}.
		 */
		public static HDelCommand field(ByteBuffer field) {

			Assert.notNull(field, "Field must not be null!");

			return new HDelCommand(null, Collections.singletonList(field));
		}

		/**
		 * Creates a new {@link HDelCommand} given a {@link Collection} of field names.
		 *
		 * @param fields must not be {@literal null}.
		 * @return a new {@link HDelCommand} for a {@link Collection} of field names.
		 */
		public static HDelCommand fields(Collection<ByteBuffer> fields) {

			Assert.notNull(fields, "Fields must not be null!");

			return new HDelCommand(null, new ArrayList<>(fields));
		}

		/**
		 * Applies the hash {@literal key}. Constructs a new command instance with all previously configured properties.
		 *
		 * @param key must not be {@literal null}.
		 * @return a new {@link HDelCommand} with {@literal key} applied.
		 */
		public HDelCommand from(ByteBuffer key) {

			Assert.notNull(key, "Key must not be null!");

			return new HDelCommand(key, fields);
		}

		/**
		 * @return never {@literal null}.
		 */
		public List<ByteBuffer> getFields() {
			return fields;
		}
	}

	/**
	 * Delete given hash {@literal field}.
	 *
	 * @param key must not be {@literal null}.
	 * @param field must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/hdel">Redis Documentation: HDEL</a>
	 */
	default Mono<Boolean> hDel(ByteBuffer key, ByteBuffer field) {

		Assert.notNull(field, "Field must not be null!");

		return hDel(key, Collections.singletonList(field)).map(val -> val > 0 ? Boolean.TRUE : Boolean.FALSE);
	}

	/**
	 * Delete given hash {@literal fields}.
	 *
	 * @param key must not be {@literal null}.
	 * @param fields must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/hdel">Redis Documentation: HDEL</a>
	 */
	default Mono<Long> hDel(ByteBuffer key, Collection<ByteBuffer> fields) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(fields, "Fields must not be null!");

		return hDel(Mono.just(HDelCommand.fields(fields).from(key))).next().map(NumericResponse::getOutput);
	}

	/**
	 * Delete given hash {@literal fields}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/hdel">Redis Documentation: HDEL</a>
	 */
	Flux<NumericResponse<HDelCommand, Long>> hDel(Publisher<HDelCommand> commands);

	/**
	 * Get size of hash at {@literal key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/hlen">Redis Documentation: HLEN</a>
	 */
	default Mono<Long> hLen(ByteBuffer key) {

		Assert.notNull(key, "Key must not be null!");

		return hLen(Mono.just(new KeyCommand(key))).next().map(NumericResponse::getOutput);
	}

	/**
	 * Get size of hash at {@literal key}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/hlen">Redis Documentation: HLEN</a>
	 */
	Flux<NumericResponse<KeyCommand, Long>> hLen(Publisher<KeyCommand> commands);

	/**
	 * Get key set (fields) of hash at {@literal key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/hkeys">Redis Documentation: HKEYS</a>
	 */
	default Flux<ByteBuffer> hKeys(ByteBuffer key) {

		Assert.notNull(key, "Key must not be null!");

		return hKeys(Mono.just(new KeyCommand(key))).flatMap(CommandResponse::getOutput);
	}

	/**
	 * Get key set (fields) of hash at {@literal key}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/hkeys">Redis Documentation: HKEYS</a>
	 */
	Flux<CommandResponse<KeyCommand, Flux<ByteBuffer>>> hKeys(Publisher<KeyCommand> commands);

	/**
	 * Get entry set (values) of hash at {@literal key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/hvals">Redis Documentation: HVALS</a>
	 */
	default Flux<ByteBuffer> hVals(ByteBuffer key) {

		Assert.notNull(key, "Key must not be null!");

		return hVals(Mono.just(new KeyCommand(key))).flatMap(CommandResponse::getOutput);
	}

	/**
	 * Get entry set (values) of hash at {@literal key}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/hvals">Redis Documentation: HVALS</a>
	 */
	Flux<CommandResponse<KeyCommand, Flux<ByteBuffer>>> hVals(Publisher<KeyCommand> commands);

	/**
	 * Get entire hash stored at {@literal key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/hgetall">Redis Documentation: HGETALL</a>
	 */
	default Flux<Map.Entry<ByteBuffer, ByteBuffer>> hGetAll(ByteBuffer key) {

		Assert.notNull(key, "Key must not be null!");

		return hGetAll(Mono.just(new KeyCommand(key))).flatMap(CommandResponse::getOutput);
	}

	/**
	 * Get entire hash stored at {@literal key}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/hgetall">Redis Documentation: HGETALL</a>
	 */
	Flux<CommandResponse<KeyCommand, Flux<Map.Entry<ByteBuffer, ByteBuffer>>>> hGetAll(Publisher<KeyCommand> commands);
}
