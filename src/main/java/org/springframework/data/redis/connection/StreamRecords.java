package org.springframework.data.redis.connection;

import lombok.EqualsAndHashCode;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.springframework.data.redis.connection.RedisStreamCommands.ByteBufferRecord;
import org.springframework.data.redis.connection.RedisStreamCommands.ByteRecord;
import org.springframework.data.redis.connection.RedisStreamCommands.MapRecord;
import org.springframework.data.redis.connection.RedisStreamCommands.ObjectRecord;
import org.springframework.data.redis.connection.RedisStreamCommands.RecordId;
import org.springframework.data.redis.connection.RedisStreamCommands.StringRecord;
import org.springframework.data.redis.util.ByteUtils;
import org.springframework.lang.Nullable;
import org.springframework.util.ClassUtils;
import org.springframework.util.ObjectUtils;

/**
 * {@link StreamRecords} provides utilities to create specific
 * {@link org.springframework.data.redis.connection.RedisStreamCommands.Record} instances.
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
	 * Obtain new instance of {@link RecordBuilder} to fluently create
	 * {@link org.springframework.data.redis.connection.RedisStreamCommands.Record records}.
	 *
	 * @return
	 */
	public static RecordBuilder newRecord() {
		return new RecordBuilder(null, RecordId.autoGenerate());
	}

	public static class RecordBuilder<S> {

		private RecordId id;
		private S stream;

		RecordBuilder(@Nullable S stream, RecordId recordId) {

			this.stream = stream;
			this.id = recordId;
		}

		public <STREAM_KEY> RecordBuilder<STREAM_KEY> in(STREAM_KEY stream) {
			return new RecordBuilder<>(stream, id);
		}

		public RecordBuilder<S> withId(String id) {
			return withId(RecordId.of(id));
		}

		public RecordBuilder<S> withId(RecordId id) {

			this.id = id;
			return this;
		}

		/**
		 * @param map
		 * @param <K>
		 * @param <V>
		 * @return new instance of {@link MapRecord}.
		 */
		public <K, V> MapRecord<S, K, V> ofMap(Map<K, V> map) {
			return new MapBackedRecord<>(stream, id, map);
		}

		/**
		 * @param map
		 * @return new instance of {@link StringRecord}.
		 */
		public StringRecord ofStrings(Map<String, String> map) {
			return new StringMapBackedRecord(ObjectUtils.nullSafeToString(stream), id, map);
		}

		/**
		 * @param value
		 * @param <V>
		 * @return ni instance of {@link ObjectRecord}.
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

			ByteBuffer streamKey = null;

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

	static class MapBackedRecord<S, K, V> implements MapRecord<S, K, V> {

		private @Nullable S stream;
		private RecordId recordId;
		private final Map<K, V> kvMap;

		MapBackedRecord(S stream, RecordId recordId, Map<K, V> kvMap) {

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

	static class ByteMapBackedRecord extends MapBackedRecord<byte[], byte[], byte[]> implements ByteRecord {

		ByteMapBackedRecord(byte[] stream, RecordId recordId, Map<byte[], byte[]> map) {
			super(stream, recordId, map);
		}

		@Override
		public ByteMapBackedRecord withStreamKey(byte[] key) {
			return new ByteMapBackedRecord(key, getId(), getValue());
		}

		public ByteMapBackedRecord withId(RecordId id) {
			return new ByteMapBackedRecord(getStream(), id, getValue());
		}
	}

	static class ByteBufferMapBackedRecord extends MapBackedRecord<ByteBuffer, ByteBuffer, ByteBuffer>
			implements ByteBufferRecord {

		ByteBufferMapBackedRecord(ByteBuffer stream, RecordId recordId, Map<ByteBuffer, ByteBuffer> map) {
			super(stream, recordId, map);
		}

		@Override
		public ByteBufferMapBackedRecord withStreamKey(ByteBuffer key) {
			return new ByteBufferMapBackedRecord(key, getId(), getValue());
		}

		public ByteBufferMapBackedRecord withId(RecordId id) {
			return new ByteBufferMapBackedRecord(getStream(), id, getValue());
		}
	}

	static class StringMapBackedRecord extends MapBackedRecord<String, String, String> implements StringRecord {

		StringMapBackedRecord(String stream, RecordId recordId, Map<String, String> stringStringMap) {
			super(stream, recordId, stringStringMap);
		}

		@Override
		public StringRecord withStreamKey(String key) {
			return new StringMapBackedRecord(key, getId(), getValue());
		}

		public StringMapBackedRecord withId(RecordId id) {
			return new StringMapBackedRecord(getStream(), id, getValue());
		}
	}

	@EqualsAndHashCode
	static class ObjectBackedRecord<S, V> implements ObjectRecord<S, V> {

		private @Nullable S stream;
		private RecordId recordId;
		private final V value;

		public ObjectBackedRecord(@Nullable S stream, RecordId recordId, V value) {

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
		public <S1> ObjectRecord<S1, V> withStreamKey(S1 key) {
			return new ObjectBackedRecord<>(key, recordId, value);
		}

		@Override
		public String toString() {
			return "ObjectBackedRecord{" + "recordId=" + recordId + ", value=" + value + '}';
		}
	}
}
