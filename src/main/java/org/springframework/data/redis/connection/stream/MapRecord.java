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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;

import org.springframework.data.redis.connection.stream.StreamRecords.MapBackedRecord;
import org.springframework.data.redis.hash.HashMapper;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * A {@link Record} within the stream backed by a collection of {@literal field/value} paris.
 *
 * @param <K> the field type of the backing map.
 * @param <V> the value type of the backing map.
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Romain Beghi
 * @since 2.2
 */
public interface MapRecord<S, K, V> extends Record<S, Map<K, V>>, Iterable<Map.Entry<K, V>> {

	/**
	 * Creates a new {@link MapRecord} associated with the {@code stream} key and {@link Map value}.
	 * 
	 * @param stream the stream key.
	 * @param map the value.
	 * @return the {@link ObjectRecord} holding the {@code stream} key and {@code value}.
	 */
	static <S, K, V> MapRecord<S, K, V> create(S stream, Map<K, V> map) {

		Assert.notNull(stream, "Stream must not be null");
		Assert.notNull(map, "Map must not be null");

		return new MapBackedRecord<>(stream, RecordId.autoGenerate(), map);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStreamCommands.Record#withId(org.springframework.data.redis.connection.RedisStreamCommands.RecordId)
	 */
	@Override
	MapRecord<S, K, V> withId(RecordId id);

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStreamCommands.Record#withStreamKey(java.lang.Object)
	 */
	@Override
	<SK> MapRecord<SK, K, V> withStreamKey(SK key);

	/**
	 * Apply the given {@link Function mapFunction} to each and every entry in the backing collection to create a new
	 * {@link MapRecord}.
	 *
	 * @param mapFunction must not be {@literal null}.
	 * @param <HK> the field type of the new backing collection.
	 * @param <HV> the value type of the new backing collection.
	 * @return new instance of {@link MapRecord}.
	 */
	default <HK, HV> MapRecord<S, HK, HV> mapEntries(Function<Entry<K, V>, Entry<HK, HV>> mapFunction) {

		Map<HK, HV> mapped = new LinkedHashMap<>();
		iterator().forEachRemaining(it -> {

			Entry<HK, HV> mappedPair = mapFunction.apply(it);
			mapped.put(mappedPair.getKey(), mappedPair.getValue());
		});

		return StreamRecords.newRecord().in(getStream()).withId(getId()).ofMap(mapped);
	}

	/**
	 * Map this {@link MapRecord} by applying the mapping {@link Function}.
	 * 
	 * @param mapFunction function to apply to this {@link MapRecord} element.
	 * @return the mapped {@link MapRecord}.
	 */
	default <SK, HK, HV> MapRecord<SK, HK, HV> map(Function<MapRecord<S, K, V>, MapRecord<SK, HK, HV>> mapFunction) {
		return mapFunction.apply(this);
	}

	/**
	 * Serialize {@link #getStream() key} and {@link #getValue() field/value pairs} with the given
	 * {@link RedisSerializer}. An already assigned {@link RecordId id} is carried over to the new instance.
	 *
	 * @param serializer can be {@literal null} if the {@link Record} only holds binary data.
	 * @return new {@link ByteRecord} holding the serialized values.
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	default ByteRecord serialize(@Nullable RedisSerializer<?> serializer) {
		return serialize((RedisSerializer) serializer, (RedisSerializer) serializer, (RedisSerializer) serializer);
	}

	/**
	 * Serialize {@link #getStream() key} with the {@literal streamSerializer}, field names with the
	 * {@literal fieldSerializer} and values with the {@literal valueSerializer}. An already assigned {@link RecordId id}
	 * is carried over to the new instance.
	 * 
	 * @param streamSerializer can be {@literal null} if the key is binary.
	 * @param fieldSerializer can be {@literal null} if the fields are binary.
	 * @param valueSerializer can be {@literal null} if the values are binary.
	 * @return new {@link ByteRecord} holding the serialized values.
	 */
	default ByteRecord serialize(@Nullable RedisSerializer<? super S> streamSerializer,
			@Nullable RedisSerializer<? super K> fieldSerializer, @Nullable RedisSerializer<? super V> valueSerializer) {

		MapRecord<S, byte[], byte[]> binaryMap = mapEntries(
				it -> Collections.singletonMap(StreamSerialization.serialize(fieldSerializer, it.getKey()),
						StreamSerialization.serialize(valueSerializer, it.getValue())).entrySet().iterator().next());

		return StreamRecords.newRecord() //
				.in(streamSerializer != null ? streamSerializer.serialize(getStream()) : (byte[]) getStream()) //
				.withId(getId()) //
				.ofBytes(binaryMap.getValue());
	}

	/**
	 * Apply the given {@link HashMapper} to the backing value to create a new {@link MapRecord}. An already assigned
	 * {@link RecordId id} is carried over to the new instance.
	 *
	 * @param mapper must not be {@literal null}.
	 * @param <OV> type of the value backing the {@link ObjectRecord}.
	 * @return new instance of {@link ObjectRecord}.
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	default <OV> ObjectRecord<S, OV> toObjectRecord(HashMapper<? super OV, ? super K, ? super V> mapper) {
		return Record.<S, OV> of((OV) mapper.fromHash((Map) getValue())).withId(getId()).withStreamKey(getStream());
	}
}
