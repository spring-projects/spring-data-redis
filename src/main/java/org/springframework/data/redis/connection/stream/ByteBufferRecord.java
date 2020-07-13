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

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.data.redis.hash.HashMapper;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.util.ByteUtils;
import org.springframework.lang.Nullable;

/**
 * A {@link Record} within the stream backed by a collection of binary {@literal field/value} paris.
 *
 * @author Christoph Strobl
 * @see 2.2
 */
public interface ByteBufferRecord extends MapRecord<ByteBuffer, ByteBuffer, ByteBuffer> {

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStreamCommands.Record#withId(org.springframework.data.redis.connection.RedisStreamCommands.RecordId)
	 */
	@Override
	ByteBufferRecord withId(RecordId id);

	/**
	 * Create a new {@link ByteBufferRecord} with the associated stream {@literal key}.
	 * 
	 * @param key the binary stream key.
	 * @return a new {@link ByteBufferRecord}.
	 */
	ByteBufferRecord withStreamKey(ByteBuffer key);

	/**
	 * Deserialize {@link #getStream() key} and {@link #getValue() field/value pairs} with the given
	 * {@link RedisSerializer}. An already assigned {@link RecordId id} is carried over to the new instance.
	 *
	 * @param serializer can be {@literal null} if the {@link Record} only holds binary data.
	 * @return new {@link MapRecord} holding the deserialized values.
	 */
	default <T> MapRecord<T, T, T> deserialize(@Nullable RedisSerializer<T> serializer) {
		return deserialize(serializer, serializer, serializer);
	}

	/**
	 * Deserialize {@link #getStream() key} with the {@literal streamSerializer}, field names with the
	 * {@literal fieldSerializer} and values with the {@literal valueSerializer}. An already assigned {@link RecordId id}
	 * is carried over to the new instance.
	 *
	 * @param streamSerializer can be {@literal null} if the key suites already the target format.
	 * @param fieldSerializer can be {@literal null} if the fields suite already the target format.
	 * @param valueSerializer can be {@literal null} if the values suite already the target format.
	 * @return new {@link MapRecord} holding the deserialized values.
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	default <K, HK, HV> MapRecord<K, HK, HV> deserialize(@Nullable RedisSerializer<? extends K> streamSerializer,
			@Nullable RedisSerializer<? extends HK> fieldSerializer,
			@Nullable RedisSerializer<? extends HV> valueSerializer) {

		return mapEntries(it -> Collections.<HK, HV> singletonMap(
				fieldSerializer != null ? fieldSerializer.deserialize(ByteUtils.getBytes(it.getKey())) : (HK) it.getKey(),
				valueSerializer != null ? valueSerializer.deserialize(ByteUtils.getBytes(it.getValue())) : (HV) it.getValue())
				.entrySet().iterator().next()).withStreamKey(
						streamSerializer != null ? streamSerializer.deserialize(ByteUtils.getBytes(getStream())) : (K) getStream());
	}

	/**
	 * Convert a binary {@link MapRecord} into a {@link ByteRecord}.
	 *
	 * @param source must not be {@literal null}.
	 * @return new instance of {@link ByteRecord}.
	 */
	static ByteBufferRecord of(MapRecord<ByteBuffer, ByteBuffer, ByteBuffer> source) {
		return StreamRecords.newRecord().in(source.getStream()).withId(source.getId()).ofBuffer(source.getValue());
	}

	/**
	 * Convert a binary {@link MapRecord} into an {@link ObjectRecord}.
	 *
	 * @param source must not be {@literal null}.
	 * @return new instance of {@link ByteRecord}.
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	default <OV> ObjectRecord<ByteBuffer, OV> toObjectRecord(
			HashMapper<? super OV, ? super ByteBuffer, ? super ByteBuffer> mapper) {

		Map<byte[], byte[]> targetMap = getValue().entrySet().stream().collect(
				Collectors.toMap(entry -> ByteUtils.getBytes(entry.getKey()), entry -> ByteUtils.getBytes(entry.getValue())));

		return Record.<ByteBuffer, OV> of((OV) (mapper).fromHash((Map) targetMap)).withId(getId())
				.withStreamKey(getStream());
	}
}
