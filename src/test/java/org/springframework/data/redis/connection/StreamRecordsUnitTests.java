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
package org.springframework.data.redis.connection;

import static org.assertj.core.api.Assertions.*;

import java.util.Collections;
import java.util.Map;

import org.junit.Test;
import org.springframework.data.redis.connection.stream.ByteRecord;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.connection.stream.Record;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.hash.HashMapper;
import org.springframework.data.redis.serializer.RedisSerializer;

/**
 * @author Christoph Strobl
 */
public class StreamRecordsUnitTests {

	static final String STRING_STREAM_KEY = "stream-key";
	static final RecordId RECORD_ID = RecordId.of("1-0");
	static final String STRING_MAP_KEY = "string-key";
	static final String STRING_VAL = "string-val";

	static final byte[] SERIALIZED_STRING_VAL = RedisSerializer.string().serialize(STRING_VAL);
	static final byte[] SERIALIZED_STRING_MAP_KEY = RedisSerializer.string().serialize(STRING_MAP_KEY);
	static final byte[] SERIALIZED_STRING_STREAM_KEY = RedisSerializer.string().serialize(STRING_STREAM_KEY);

	@Test // DATAREDIS-864
	public void objectRecordToMapRecordViaHashMapper() {

		ObjectRecord<String, String> source = Record.of("some-string").withId(RECORD_ID).withStreamKey(STRING_STREAM_KEY);

		MapRecord<String, String, String> target = source
				.toMapRecord(StubValueReturningHashMapper.simpleString(STRING_VAL));

		assertThat(target.getId()).isEqualTo(RECORD_ID);
		assertThat(target.getStream()).isEqualTo(STRING_STREAM_KEY);
		assertThat(target.getValue()).hasSize(1).containsEntry(STRING_MAP_KEY, STRING_VAL);
	}

	@Test // DATAREDIS-864
	public void mapRecordToObjectRecordViaHashMapper() {

		MapRecord<String, String, String> source = Record.of(Collections.singletonMap(STRING_MAP_KEY, "some-string"))
				.withId(RECORD_ID).withStreamKey(STRING_STREAM_KEY);

		ObjectRecord<String, String> target = source.toObjectRecord(StubValueReturningHashMapper.simpleString(STRING_VAL));

		assertThat(target.getId()).isEqualTo(RECORD_ID);
		assertThat(target.getStream()).isEqualTo(STRING_STREAM_KEY);
		assertThat(target.getValue()).isEqualTo(STRING_VAL);
	}

	@Test // DATAREDIS-864
	public void serializeMapRecord() {

		MapRecord<String, String, String> source = Record.of(Collections.singletonMap(STRING_MAP_KEY, STRING_VAL))
				.withId(RECORD_ID).withStreamKey(STRING_STREAM_KEY);

		ByteRecord target = source.serialize(RedisSerializer.string());

		assertThat(target.getId()).isEqualTo(RECORD_ID);
		assertThat(target.getStream()).isEqualTo(SERIALIZED_STRING_STREAM_KEY);
		assertThat(target.getValue().keySet().iterator().next()).isEqualTo(SERIALIZED_STRING_MAP_KEY);
		assertThat(target.getValue().values().iterator().next()).isEqualTo(SERIALIZED_STRING_VAL);
	}

	@Test // DATAREDIS-864
	public void deserializeByteMapRecord() {

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

		public StubValueReturningHashMapper(Map<K, V> to) {
			this(to, (T) new Object());
		}

		public StubValueReturningHashMapper(T from) {
			this(Collections.emptyMap(), from);
		}

		public StubValueReturningHashMapper(Map<K, V> to, T from) {
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

}
