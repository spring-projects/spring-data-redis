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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.reactivestreams.Publisher;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.connection.ReactiveRedisConnection.BooleanResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.CommandResponse;
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
public interface ReactiveHashCommands {

	/**
	 * @author Christoph Strobl
	 */
	class HSetCommand extends KeyCommand {

		private static final ByteBuffer SINGLE_VALUE_KEY = ByteBuffer.allocate(0);
		private final Map<ByteBuffer, ByteBuffer> fieldValueMap;
		private final Boolean upsert;

		private HSetCommand(ByteBuffer key, Map<ByteBuffer, ByteBuffer> keyValueMap, Boolean upsert) {

			super(key);
			this.fieldValueMap = keyValueMap;
			this.upsert = upsert;
		}

		public static HSetCommand value(ByteBuffer value) {
			return new HSetCommand(null, Collections.singletonMap(SINGLE_VALUE_KEY, value), Boolean.TRUE);
		}

		public static HSetCommand fieldValues(Map<ByteBuffer, ByteBuffer> fieldValueMap) {
			return new HSetCommand(null, fieldValueMap, Boolean.TRUE);
		}

		public HSetCommand ofField(ByteBuffer field) {

			if (!fieldValueMap.containsKey(SINGLE_VALUE_KEY)) {
				throw new InvalidDataAccessApiUsageException("Value has not been set.");
			}

			return new HSetCommand(getKey(), Collections.singletonMap(field, fieldValueMap.get(SINGLE_VALUE_KEY)), upsert);
		}

		public HSetCommand forKey(ByteBuffer key) {
			return new HSetCommand(key, fieldValueMap, upsert);
		}

		public HSetCommand ifValueNotExists() {
			return new HSetCommand(getKey(), fieldValueMap, Boolean.FALSE);
		}

		public Boolean isUpsert() {
			return upsert;
		}

		public Map<ByteBuffer, ByteBuffer> getFieldValueMap() {
			return fieldValueMap;
		}
	}

	/**
	 * Set the {@code value} of a hash {@code field}.
	 *
	 * @param key must not be {@literal null}.
	 * @param field must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return
	 */
	default Mono<Boolean> hSet(ByteBuffer key, ByteBuffer field, ByteBuffer value) {

		Assert.notNull(key, "key must not be null");
		Assert.notNull(field, "field must not be null");
		Assert.notNull(value, "value must not be null");

		return hSet(Mono.just(HSetCommand.value(value).ofField(field).forKey(key))).next().map(BooleanResponse::getOutput);
	}

	/**
	 * Set the {@code value} of a hash {@code field}.
	 *
	 * @param key must not be {@literal null}.
	 * @param field must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return
	 */
	default Mono<Boolean> hSetNX(ByteBuffer key, ByteBuffer field, ByteBuffer value) {

		Assert.notNull(key, "key must not be null");
		Assert.notNull(field, "field must not be null");
		Assert.notNull(value, "value must not be null");

		return hSet(Mono.just(HSetCommand.value(value).ofField(field).forKey(key).ifValueNotExists())).next()
				.map(BooleanResponse::getOutput);
	}

	/**
	 * Set multiple hash fields to multiple values using data provided in {@code fieldValueMap}.
	 *
	 * @param key must not be {@literal null}.
	 * @param fieldValueMap must not be {@literal null}.
	 * @return
	 */
	default Mono<Boolean> hMSet(ByteBuffer key, Map<ByteBuffer, ByteBuffer> fieldValueMap) {

		Assert.notNull(key, "key must not be null");
		Assert.notNull(fieldValueMap, "field must not be null");

		return hSet(Mono.just(HSetCommand.fieldValues(fieldValueMap).forKey(key).ifValueNotExists())).next()
				.map(BooleanResponse::getOutput);
	}

	/**
	 * Set the {@code value} of a hash {@code field}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 */
	Flux<BooleanResponse<HSetCommand>> hSet(Publisher<HSetCommand> commands);

	/**
	 * @author Christoph Strobl
	 */
	class HGetCommand extends KeyCommand {

		private List<ByteBuffer> fields;

		private HGetCommand(ByteBuffer key, List<ByteBuffer> fields) {

			super(key);
			this.fields = fields;
		}

		public static HGetCommand field(ByteBuffer field) {
			return new HGetCommand(null, Collections.singletonList(field));
		}

		public static HGetCommand fields(List<ByteBuffer> fields) {
			return new HGetCommand(null, new ArrayList<>(fields));
		}

		public HGetCommand from(ByteBuffer key) {
			return new HGetCommand(key, fields);
		}

		public List<ByteBuffer> getFields() {
			return fields;
		}
	}

	/**
	 * Get value for given {@code field} from hash at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param field must not be {@literal null}.
	 * @return
	 */
	default Mono<ByteBuffer> hGet(ByteBuffer key, ByteBuffer field) {
		return hMGet(key, Collections.singletonList(field)).map(val -> val.isEmpty() ? null : val.iterator().next());
	}

	/**
	 * Get values for given {@code fields} from hash at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param fields must not be {@literal null}.
	 * @return
	 */
	default Mono<List<ByteBuffer>> hMGet(ByteBuffer key, List<ByteBuffer> fields) {

		Assert.notNull(key, "key must not be null");
		Assert.notNull(fields, "fields must not be null");

		return hMGet(Mono.just(HGetCommand.fields(fields).from(key))).next().map(MultiValueResponse::getOutput);
	}

	/**
	 * Get values for given {@code fields} from hash at {@code key}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 */
	Flux<MultiValueResponse<HGetCommand, ByteBuffer>> hMGet(Publisher<HGetCommand> commands);

	/**
	 * @author Christoph Strobl
	 */
	class HExistsCommand extends KeyCommand {

		private final ByteBuffer field;

		private HExistsCommand(ByteBuffer key, ByteBuffer field) {

			super(key);
			this.field = field;
		}

		public static HExistsCommand field(ByteBuffer field) {
			return new HExistsCommand(null, field);
		}

		public HExistsCommand in(ByteBuffer key) {
			return new HExistsCommand(key, field);
		}

		public ByteBuffer getField() {
			return field;
		}
	}

	/**
	 * Determine if given hash {@code field} exists.
	 *
	 * @param key must not be {@literal null}.
	 * @param field must not be {@literal null}.
	 * @return
	 */
	default Mono<Boolean> hExists(ByteBuffer key, ByteBuffer field) {

		Assert.notNull(key, "key must not be null");
		Assert.notNull(field, "field must not be null");

		return hExists(Mono.just(HExistsCommand.field(field).in(key))).next().map(BooleanResponse::getOutput);
	}

	/**
	 * Determine if given hash {@code field} exists.
	 *
	 * @param commands
	 * @return
	 */
	Flux<BooleanResponse<HExistsCommand>> hExists(Publisher<HExistsCommand> commands);

	/**
	 * @author Christoph Strobl
	 */
	class HDelCommand extends KeyCommand {

		private final List<ByteBuffer> fields;

		private HDelCommand(ByteBuffer key, List<ByteBuffer> fields) {
			super(key);
			this.fields = fields;
		}

		public static HDelCommand field(ByteBuffer field) {
			return new HDelCommand(null, Collections.singletonList(field));
		}

		public static HDelCommand fields(List<ByteBuffer> fields) {
			return new HDelCommand(null, new ArrayList<>(fields));
		}

		public HDelCommand from(ByteBuffer key) {
			return new HDelCommand(key, fields);
		}

		public List<ByteBuffer> getFields() {
			return fields;
		}
	}

	/**
	 * Delete given hash {@code field}.
	 *
	 * @param key must not be {@literal null}.
	 * @param field must not be {@literal null}.
	 * @return
	 */
	default Mono<Boolean> hDel(ByteBuffer key, ByteBuffer field) {

		Assert.notNull(field, "field must not be null");

		return hDel(key, Collections.singletonList(field)).map(val -> val > 0 ? Boolean.TRUE : Boolean.FALSE);
	}

	/**
	 * Delete given hash {@code fields}.
	 *
	 * @param key must not be {@literal null}.
	 * @param fields must not be {@literal null}.
	 * @return
	 */
	default Mono<Long> hDel(ByteBuffer key, List<ByteBuffer> fields) {

		Assert.notNull(key, "key must not be null");
		Assert.notNull(fields, "fields must not be null");

		return hDel(Mono.just(HDelCommand.fields(fields).from(key))).next().map(NumericResponse::getOutput);
	}

	/**
	 * Delete given hash {@code fields}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 */
	Flux<NumericResponse<HDelCommand, Long>> hDel(Publisher<HDelCommand> commands);

	/**
	 * Get size of hash at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 */
	default Mono<Long> hLen(ByteBuffer key) {

		Assert.notNull(key, "key must not be null");

		return hLen(Mono.just(new KeyCommand(key))).next().map(NumericResponse::getOutput);
	}

	/**
	 * Get size of hash at {@code key}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 */
	Flux<NumericResponse<KeyCommand, Long>> hLen(Publisher<KeyCommand> commands);

	/**
	 * Get key set (fields) of hash at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 */
	default Mono<List<ByteBuffer>> hKeys(ByteBuffer key) {

		Assert.notNull(key, "key must not be null");

		return hKeys(Mono.just(new KeyCommand(key))).next().map(MultiValueResponse::getOutput);
	}

	/**
	 * Get key set (fields) of hash at {@code key}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 */
	Flux<MultiValueResponse<KeyCommand, ByteBuffer>> hKeys(Publisher<KeyCommand> commands);

	/**
	 * Get entry set (values) of hash at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 */
	default Mono<List<ByteBuffer>> hVals(ByteBuffer key) {

		Assert.notNull(key, "key must not be null");

		return hVals(Mono.just(new KeyCommand(key))).next().map(MultiValueResponse::getOutput);
	}

	/**
	 * Get entry set (values) of hash at {@code key}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 */
	Flux<MultiValueResponse<KeyCommand, ByteBuffer>> hVals(Publisher<KeyCommand> commands);

	/**
	 * Get entire hash stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 */
	default Mono<Map<ByteBuffer, ByteBuffer>> hGetAll(ByteBuffer key) {

		Assert.notNull(key, "key must not be null");

		return hGetAll(Mono.just(new KeyCommand(key))).next().map(CommandResponse::getOutput);
	}

	/**
	 * Get entire hash stored at {@code key}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 */
	Flux<CommandResponse<KeyCommand, Map<ByteBuffer, ByteBuffer>>> hGetAll(Publisher<KeyCommand> commands);

}
