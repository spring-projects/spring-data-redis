/*
 * Copyright 2018-2019 the original author or authors.
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

import org.springframework.data.redis.connection.stream.StreamRecords.ObjectBackedRecord;
import org.springframework.data.redis.hash.HashMapper;
import org.springframework.util.Assert;

/**
 * A {@link Record} within the stream mapped to a single object. This may be a simple type, such as {@link String} or a
 * complex one.
 *
 * @param <V> the type of the backing Object.
 * @author Christoph Strobl
 * @author Mark Paluch
 * @see 2.2
 */
public interface ObjectRecord<S, V> extends Record<S, V> {

	/**
	 * Creates a new {@link ObjectRecord} associated with the {@code stream} key and {@code value}.
	 * 
	 * @param stream the stream key.
	 * @param value the value.
	 * @return the {@link ObjectRecord} holding the {@code stream} key and {@code value}.
	 */
	static <S, V> ObjectRecord<S, V> create(S stream, V value) {

		Assert.notNull(stream, "Stream must not be null");
		Assert.notNull(value, "Value must not be null");

		return new ObjectBackedRecord<>(stream, RecordId.autoGenerate(), value);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStreamCommands.Record#withId(org.springframework.data.redis.connection.RedisStreamCommands.RecordId)
	 */
	@Override
	ObjectRecord<S, V> withId(RecordId id);

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStreamCommands.Record#withStreamKey(java.lang.Object)
	 */
	<SK> ObjectRecord<SK, V> withStreamKey(SK key);

	/**
	 * Apply the given {@link HashMapper} to the backing value to create a new {@link MapRecord}. An already assigned
	 * {@link RecordId id} is carried over to the new instance.
	 *
	 * @param mapper must not be {@literal null}.
	 * @param <HK> the key type of the resulting {@link MapRecord}.
	 * @param <HV> the value type of the resulting {@link MapRecord}.
	 * @return new instance of {@link MapRecord}.
	 */
	default <HK, HV> MapRecord<S, HK, HV> toMapRecord(HashMapper<? super V, HK, HV> mapper) {
		return Record.<S, HK, HV> of(mapper.toHash(getValue())).withId(getId()).withStreamKey(getStream());
	}
}
