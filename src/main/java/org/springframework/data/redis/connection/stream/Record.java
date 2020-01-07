/*
 * Copyright 2018-2020 the original author or authors.
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
package org.springframework.data.redis.connection.stream;

import java.util.Map;

import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * A single entry in the stream consisting of the {@link RecordId entry-id} and the actual entry-value (typically a
 * collection of {@link MapRecord field/value pairs}).
 *
 * @param <V> the type backing the {@link Record}.
 * @author Christoph Strobl
 * @since 2.2
 * @see <a href="https://redis.io/topics/streams-intro#streams-basics">Redis Documentation - Stream Basics</a>
 */
public interface Record<S, V> {

	/**
	 * The id of the stream (aka the {@literal key} in Redis).
	 *
	 * @return can be {@literal null}.
	 */
	@Nullable
	S getStream();

	/**
	 * The id of the entry inside the stream.
	 *
	 * @return never {@literal null}.
	 */
	RecordId getId();

	/**
	 * @return the actual content. Never {@literal null}.
	 */
	V getValue();

	/**
	 * Create a new {@link MapRecord} instance backed by the given {@link Map} holding {@literal field/value} pairs.
	 * <br />
	 * You may want to use the builders available via {@link StreamRecords}.
	 *
	 * @param map the raw map.
	 * @param <K> the key type of the given {@link Map}.
	 * @param <V> the value type of the given {@link Map}.
	 * @return new instance of {@link MapRecord}.
	 */
	static <S, K, V> MapRecord<S, K, V> of(Map<K, V> map) {

		Assert.notNull(map, "Map must not be null!");
		return StreamRecords.mapBacked(map);
	}

	/**
	 * Create a new {@link ObjectRecord} instance backed by the given {@literal value}. The value may be a simple type,
	 * like {@link String} or a complex one. <br />
	 * You may want to use the builders available via {@link StreamRecords}.
	 *
	 * @param value the value to persist.
	 * @param <V> the type of the backing value.
	 * @return new instance of {@link MapRecord}.
	 */
	static <S, V> ObjectRecord<S, V> of(V value) {

		Assert.notNull(value, "Value must not be null!");
		return StreamRecords.objectBacked(value);
	}

	/**
	 * Create a new instance of {@link Record} with the given {@link RecordId}.
	 *
	 * @param id must not be {@literal null}.
	 * @return new instance of {@link Record}.
	 */
	Record<S, V> withId(RecordId id);

	/**
	 * Create a new instance of {@link Record} with the given {@literal key} to store the record at.
	 *
	 * @param key the Redis key identifying the stream.
	 * @param <SK>
	 * @return new instance of {@link Record}.
	 */
	<SK> Record<SK, V> withStreamKey(SK key);
}
