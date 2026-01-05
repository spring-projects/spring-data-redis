/*
 * Copyright 2018-present the original author or authors.
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

import static org.assertj.core.api.Assertions.*;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import org.springframework.data.redis.connection.stream.ByteRecord;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.connection.stream.Record;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.hash.HashMapper;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializer;

/**
 * Unit tests for {@link StreamRecords}.
 *
 * @author Christoph Strobl
 * @author Romain Beghi
 * @author Seo Bo Gyeong
 */
class StreamRecordsUnitTests {

	private static final String STRING_STREAM_KEY = "stream-key";
	private static final RecordId RECORD_ID = RecordId.of("1-0");
	private static final String STRING_MAP_KEY = "string-key";
	private static final String STRING_VAL = "string-val";
	private static final DummyObject OBJECT_VAL = new DummyObject();

	private static final Jackson2JsonRedisSerializer<DummyObject> JSON_REDIS_SERIALIZER = new Jackson2JsonRedisSerializer<>(
			DummyObject.class);

	private static final byte[] SERIALIZED_STRING_VAL = RedisSerializer.string().serialize(STRING_VAL);
	private static final byte[] SERIALIZED_STRING_MAP_KEY = RedisSerializer.string().serialize(STRING_MAP_KEY);
	private static final byte[] SERIALIZED_STRING_STREAM_KEY = RedisSerializer.string().serialize(STRING_STREAM_KEY);
	private static final byte[] SERIALIZED_JSON_OBJECT_VAL = JSON_REDIS_SERIALIZER.serialize(OBJECT_VAL);

	private static class DummyObject implements Serializable {
		private final Integer dummyId = 1;

		public Integer getDummyId() {
			return this.dummyId;
		}
	}

	@Test // DATAREDIS-864
	void objectRecordToMapRecordViaHashMapper() {

		ObjectRecord<String, String> source = Record.of("some-string").withId(RECORD_ID).withStreamKey(STRING_STREAM_KEY);

		MapRecord<String, String, String> target = source
				.toMapRecord(StubValueReturningHashMapper.simpleString(STRING_VAL));

		assertThat(target.getId()).isEqualTo(RECORD_ID);
		assertThat(target.getStream()).isEqualTo(STRING_STREAM_KEY);
		assertThat(target.getValue()).hasSize(1).containsEntry(STRING_MAP_KEY, STRING_VAL);
	}

	@Test // DATAREDIS-864
	void mapRecordToObjectRecordViaHashMapper() {

		MapRecord<String, String, String> source = Record.of(Collections.singletonMap(STRING_MAP_KEY, "some-string"))
				.withId(RECORD_ID).withStreamKey(STRING_STREAM_KEY);

		ObjectRecord<String, String> target = source.toObjectRecord(StubValueReturningHashMapper.simpleString(STRING_VAL));

		assertThat(target.getId()).isEqualTo(RECORD_ID);
		assertThat(target.getStream()).isEqualTo(STRING_STREAM_KEY);
		assertThat(target.getValue()).isEqualTo(STRING_VAL);
	}

	@Test // DATAREDIS-864
	void serializeMapRecordStringAsHashValue() {

		MapRecord<String, String, String> source = Record.of(Collections.singletonMap(STRING_MAP_KEY, STRING_VAL))
				.withId(RECORD_ID).withStreamKey(STRING_STREAM_KEY);

		ByteRecord target = source.serialize(RedisSerializer.string());

		assertThat(target.getId()).isEqualTo(RECORD_ID);
		assertByteRecord(target);
	}

	@Test // DATAREDIS-993
	void serializeMapRecordObjectAsHashValue() {

		MapRecord<String, String, DummyObject> source = Record.of(Collections.singletonMap(STRING_MAP_KEY, OBJECT_VAL))
				.withId(RECORD_ID).withStreamKey(STRING_STREAM_KEY);

		ByteRecord target = source.serialize(RedisSerializer.string(), RedisSerializer.string(), JSON_REDIS_SERIALIZER);

		assertThat(target.getId()).isEqualTo(RECORD_ID);
		assertThat(target.getStream()).isEqualTo(SERIALIZED_STRING_STREAM_KEY);
		assertThat(target.getValue().keySet().iterator().next()).isEqualTo(SERIALIZED_STRING_MAP_KEY);
		assertThat(target.getValue().values().iterator().next()).isEqualTo(SERIALIZED_JSON_OBJECT_VAL);
	}

	@Test // DATAREDIS-864
	void deserializeByteMapRecord() {

		ByteRecord source = StreamRecords.newRecord().in(SERIALIZED_STRING_STREAM_KEY).withId(RECORD_ID)
				.ofBytes(Collections.singletonMap(SERIALIZED_STRING_MAP_KEY, SERIALIZED_STRING_VAL));

		MapRecord<String, String, String> target = source.deserialize(RedisSerializer.string());

		assertThat(target.getId()).isEqualTo(RECORD_ID);
		assertThat(target.getStream()).isEqualTo(STRING_STREAM_KEY);
		assertThat(target.getValue().keySet().iterator().next()).isEqualTo(STRING_MAP_KEY);
		assertThat(target.getValue().values().iterator().next()).isEqualTo(STRING_VAL);
	}

	static class StubValueReturningHashMapper<T, K, V> implements HashMapper<T, K, V> {

		final Map<K, V> to;
		final T from;

		StubValueReturningHashMapper(Map<K, V> to, T from) {
			this.to = to;
			this.from = from;
		}

		@Override
		public Map<K, V> toHash(T object) {
			return to;
		}

		@Override
		public T fromHash(Map<K, V> hash) {
			return from;
		}

		static HashMapper<Object, String, String> simpleString(String value) {
			return new StubValueReturningHashMapper<>(Collections.singletonMap(STRING_MAP_KEY, value), value);
		}
	}

	@Test // GH-3204
	void ofBytesWithNullStreamKey() {

		ByteRecord record = StreamRecords.newRecord().withId(RECORD_ID)
				.ofBytes(Collections.singletonMap(SERIALIZED_STRING_MAP_KEY, SERIALIZED_STRING_VAL));

		assertThat(record.getId()).isEqualTo(RECORD_ID);
		assertThat(record.getStream()).isNull();
	}

	@Test // GH-3204
	void ofBytesWithUnsupportedStreamKeyType() {

		assertThatIllegalArgumentException().isThrownBy(() -> StreamRecords.newRecord().in(123L) // Unsupported type
				.withId(RECORD_ID).ofBytes(Collections.singletonMap(SERIALIZED_STRING_MAP_KEY, SERIALIZED_STRING_VAL)))
				.withMessageContaining("Stream key '123' cannot be converted to byte array");
	}

	@ParameterizedTest // GH-3204
	@MethodSource("ofBytesInStreamArgs")
	void ofBytes(Object streamKey) {

		ByteRecord record = StreamRecords.newRecord().in(streamKey).withId(RECORD_ID)
				.ofBytes(Collections.singletonMap(SERIALIZED_STRING_MAP_KEY, SERIALIZED_STRING_VAL));

		assertThat(record.getId()).isEqualTo(RECORD_ID);
		assertByteRecord(record);
	}

	static Stream<Arguments> ofBytesInStreamArgs() {
		return Stream.of(Arguments.argumentSet("ByteBuffer", ByteBuffer.wrap(SERIALIZED_STRING_STREAM_KEY)), //
				Arguments.argumentSet("byte[]", new Object[] { SERIALIZED_STRING_STREAM_KEY }), //
				Arguments.argumentSet("String", STRING_STREAM_KEY));
	}

	private void assertByteRecord(ByteRecord target) {

		assertThat(target.getStream()).isEqualTo(SERIALIZED_STRING_STREAM_KEY);
		assertThat(target.getValue()).hasSize(1);

		target.getValue().forEach((k, v) -> {
			assertThat(k).isEqualTo(SERIALIZED_STRING_MAP_KEY);
			assertThat(v).isEqualTo(SERIALIZED_STRING_VAL);
		});
	}

}
