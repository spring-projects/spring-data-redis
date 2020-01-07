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

import lombok.EqualsAndHashCode;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.springframework.data.redis.util.ByteUtils;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.ObjectUtils;

/**
 * {@link StreamRecords} provides utilities to create specific {@link Record} instances.
 *
 * @author Christoph Strobl
 * @since 2.2
 */
public class StreamRecords {

	/**
	 * Create a new {@link ByteRecord} for the given raw field/value pairs.
	 *
	 * @param raw must not be {@literal null}.
	 * @return new instance of {@link ByteRecord}.
	 */
	public static ByteRecord rawBytes(Map<byte[], byte[]> raw) {
		return new ByteMapBackedRecord(null, RecordId.autoGenerate(), raw);
	}

	/**
	 * Create a new {@link ByteBufferRecord} for the given raw field/value pairs.
	 *
	 * @param raw must not be {@literal null}.
	 * @return new instance of {@link ByteBufferRecord}.
	 */
	public static ByteBufferRecord rawBuffer(Map<ByteBuffer, ByteBuffer> raw) {
		return new ByteBufferMapBackedRecord(null, RecordId.autoGenerate(), raw);
	}

	/**
	 * Create a new {@link ByteBufferRecord} for the given raw field/value pairs.
	 *
	 * @param raw must not be {@literal null}.
	 * @return new instance of {@link ByteBufferRecord}.
	 */
	public static StringRecord string(Map<String, String> raw) {
		return new StringMapBackedRecord(null, RecordId.autoGenerate(), raw);
	}

	/**
	 * Create a new {@link MapRecord} backed by the field/value pairs of the given {@link Map}.
	 *
	 * @param map must not be {@literal null}.
	 * @param <S> type of the stream key.
	 * @param <K> type of the map key.
	 * @param <V> type of the map value.
	 * @return new instance of {@link MapRecord}.
	 */
	public static <S, K, V> MapRecord<S, K, V> mapBacked(Map<K, V> map) {
		return new MapBackedRecord<>(null, RecordId.autoGenerate(), map);
	}

	/**
	 * Create new {@link ObjectRecord} backed by the given value.
	 *
	 * @param value must not be {@literal null}.
	 * @param <S> the stream key type
	 * @param <V> the value type.
	 * @return new instance of {@link ObjectRecord}.
	 */
	public static <S, V> ObjectRecord<S, V> objectBacked(V value) {
		return new ObjectBackedRecord<>(null, RecordId.autoGenerate(), value);
	}

	/**
	 * Obtain new instance of {@link RecordBuilder} to fluently create {@link Record records}.
	 *
	 * @return new instance of {@link RecordBuilder}.
	 */
	public static RecordBuilder<?> newRecord() {
		return new RecordBuilder<>(null, RecordId.autoGenerate());
	}

	// Utility constructor
	private StreamRecords() {}

	/**
	 * Builder for {@link Record}.
	 * 
	 * @param <S> stream keyy type.
	 */
	public static class RecordBuilder<S> {

		private RecordId id;
		private S stream;

		RecordBuilder(@Nullable S stream, RecordId recordId) {

			this.stream = stream;
			this.id = recordId;
		}

		/**
		 * Configure a stream key.
		 * 
		 * @param stream the stream key, must not be null.
		 * @param <STREAM_KEY>
		 * @return {@literal this} {@link RecordBuilder}.
		 */
		public <STREAM_KEY> RecordBuilder<STREAM_KEY> in(STREAM_KEY stream) {

			Assert.notNull(stream, "Stream key must not be null");

			return new RecordBuilder<>(stream, id);
		}

		/**
		 * Configure a record Id given a {@link String}. Associates a user-supplied record id instead of using
		 * server-generated record Id's.
		 * 
		 * @param id the record id.
		 * @return {@literal this} {@link RecordBuilder}.
		 * @see RecordId
		 */
		public RecordBuilder<S> withId(String id) {
			return withId(RecordId.of(id));
		}

		/**
		 * Configure a {@link RecordId}. Associates a user-supplied record id instead of using server-generated record Id's.
		 * 
		 * @param id the record id.
		 * @return {@literal this} {@link RecordBuilder}.
		 */
		public RecordBuilder<S> withId(RecordId id) {

			Assert.notNull(id, "RecordId must not be null");

			this.id = id;
			return this;
		}

		/**
		 * Create a {@link MapRecord}.
		 * 
		 * @param map
		 * @param <K>
		 * @param <V>
		 * @return new instance of {@link MapRecord}.
		 */
		public <K, V> MapRecord<S, K, V> ofMap(Map<K, V> map) {
			return new MapBackedRecord<>(stream, id, map);
		}

		/**
		 * Create a {@link StringRecord}.
		 * 
		 * @param map
		 * @return new instance of {@link StringRecord}.
		 * @see MapRecord
		 */
		public StringRecord ofStrings(Map<String, String> map) {
			return new StringMapBackedRecord(ObjectUtils.nullSafeToString(stream), id, map);
		}

		/**
		 * Create an {@link ObjectRecord}.
		 * 
		 * @param value
		 * @param <V>
		 * @return new instance of {@link ObjectRecord}.
		 */
		public <V> ObjectRecord<S, V> ofObject(V value) {
			return new ObjectBackedRecord<>(stream, id, value);
		}

		/**
		 * @param value
		 * @return new instance of {@link ByteRecord}.
		 */
		public ByteRecord ofBytes(Map<byte[], byte[]> value) {

			// todo auto conversion of known values
			return new ByteMapBackedRecord((byte[]) stream, id, value);
		}

		/**
		 * @param value
		 * @return new instance of {@link ByteBufferRecord}.
		 */
		public ByteBufferRecord ofBuffer(Map<ByteBuffer, ByteBuffer> value) {

			ByteBuffer streamKey;

			if (stream instanceof ByteBuffer) {
				streamKey = (ByteBuffer) stream;
			} else if (stream instanceof String) {
				streamKey = ByteUtils.getByteBuffer((String) stream);
			} else if (stream instanceof byte[]) {
				streamKey = ByteBuffer.wrap((byte[]) stream);
			} else {
				throw new IllegalArgumentException(String.format("Stream key %s cannot be converted to byte buffer.", stream));
			}

			return new ByteBufferMapBackedRecord(streamKey, id, value);
		}
	}

	/**
	 * Default implementation of {@link MapRecord}.
	 *
	 * @param <S>
	 * @param <K>
	 * @param <V>
	 */
	static class MapBackedRecord<S, K, V> implements MapRecord<S, K, V> {

		private @Nullable S stream;
		private RecordId recordId;
		private final Map<K, V> kvMap;

		MapBackedRecord(@Nullable S stream, RecordId recordId, Map<K, V> kvMap) {

			this.stream = stream;
			this.recordId = recordId;
			this.kvMap = kvMap;
		}

		@Nullable
		@Override
		public S getStream() {
			return stream;
		}

		@Nullable
		@Override
		public RecordId getId() {
			return recordId;
		}

		@Override
		public Iterator<Entry<K, V>> iterator() {
			return kvMap.entrySet().iterator();
		}

		@Override
		public Map<K, V> getValue() {
			return kvMap;
		}

		@Override
		public MapRecord<S, K, V> withId(RecordId id) {
			return new MapBackedRecord<>(stream, id, this.kvMap);
		}

		@Override
		public <S1> MapRecord<S1, K, V> withStreamKey(S1 key) {
			return new MapBackedRecord<>(key, recordId, this.kvMap);
		}

		@Override
		public String toString() {
			return "MapBackedRecord{" + "recordId=" + recordId + ", kvMap=" + kvMap + '}';
		}

		@Override
		public boolean equals(Object o) {

			if (o == null) {
				return false;
			}

			if (this == o) {
				return true;
			}

			if (!ClassUtils.isAssignable(MapBackedRecord.class, o.getClass())) {
				return false;
			}

			MapBackedRecord<?, ?, ?> that = (MapBackedRecord<?, ?, ?>) o;

			if (!ObjectUtils.nullSafeEquals(this.stream, that.stream)) {
				return false;
			}

			if (!ObjectUtils.nullSafeEquals(this.recordId, that.recordId)) {
				return false;
			}

			return ObjectUtils.nullSafeEquals(this.kvMap, that.kvMap);
		}

		@Override
		public int hashCode() {
			int result = stream != null ? stream.hashCode() : 0;
			result = 31 * result + recordId.hashCode();
			result = 31 * result + kvMap.hashCode();
			return result;
		}
	}

	/**
	 * Default implementation of {@link ByteRecord}.
	 */
	static class ByteMapBackedRecord extends MapBackedRecord<byte[], byte[], byte[]> implements ByteRecord {

		ByteMapBackedRecord(byte[] stream, RecordId recordId, Map<byte[], byte[]> map) {
			super(stream, recordId, map);
		}

		@Override
		public ByteMapBackedRecord withStreamKey(byte[] key) {
			return new ByteMapBackedRecord(key, getId(), getValue());
		}

		@Override
		public ByteMapBackedRecord withId(RecordId id) {
			return new ByteMapBackedRecord(getStream(), id, getValue());
		}
	}

	/**
	 * Default implementation of {@link ByteBufferRecord}.
	 */
	static class ByteBufferMapBackedRecord extends MapBackedRecord<ByteBuffer, ByteBuffer, ByteBuffer>
			implements ByteBufferRecord {

		ByteBufferMapBackedRecord(ByteBuffer stream, RecordId recordId, Map<ByteBuffer, ByteBuffer> map) {
			super(stream, recordId, map);
		}

		@Override
		public ByteBufferMapBackedRecord withStreamKey(ByteBuffer key) {
			return new ByteBufferMapBackedRecord(key, getId(), getValue());
		}

		@Override
		public ByteBufferMapBackedRecord withId(RecordId id) {
			return new ByteBufferMapBackedRecord(getStream(), id, getValue());
		}
	}

	/**
	 * Default implementation of StringRecord.
	 */
	static class StringMapBackedRecord extends MapBackedRecord<String, String, String> implements StringRecord {

		StringMapBackedRecord(String stream, RecordId recordId, Map<String, String> stringStringMap) {
			super(stream, recordId, stringStringMap);
		}

		@Override
		public StringRecord withStreamKey(String key) {
			return new StringMapBackedRecord(key, getId(), getValue());
		}

		@Override
		public StringMapBackedRecord withId(RecordId id) {
			return new StringMapBackedRecord(getStream(), id, getValue());
		}
	}

	/**
	 * Default implementation of {@link ObjectRecord}.
	 *
	 * @param <S>
	 * @param <V>
	 */
	@EqualsAndHashCode
	static class ObjectBackedRecord<S, V> implements ObjectRecord<S, V> {

		private @Nullable S stream;
		private RecordId recordId;
		private final V value;

		ObjectBackedRecord(@Nullable S stream, RecordId recordId, V value) {

			this.stream = stream;
			this.recordId = recordId;
			this.value = value;
		}

		@Nullable
		@Override
		public S getStream() {
			return stream;
		}

		@Nullable
		@Override
		public RecordId getId() {
			return recordId;
		}

		@Override
		public V getValue() {
			return value;
		}

		@Override
		public ObjectRecord<S, V> withId(RecordId id) {
			return new ObjectBackedRecord<>(stream, id, value);
		}

		@Override
		public <SK> ObjectRecord<SK, V> withStreamKey(SK key) {
			return new ObjectBackedRecord<>(key, recordId, value);
		}

		@Override
		public String toString() {
			return "ObjectBackedRecord{" + "recordId=" + recordId + ", value=" + value + '}';
		}
	}
}
