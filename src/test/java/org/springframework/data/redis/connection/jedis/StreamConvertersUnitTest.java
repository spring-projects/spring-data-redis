/*
 * Copyright 2025-present the original author or authors.
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
package org.springframework.data.redis.connection.jedis;

import static org.assertj.core.api.Assertions.*;

import redis.clients.jedis.params.XAddParams;
import redis.clients.jedis.params.XPendingParams;
import redis.clients.jedis.params.XTrimParams;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

import org.junit.jupiter.api.Test;

import org.springframework.data.redis.connection.RedisStreamCommands.StreamDeletionPolicy;
import org.springframework.data.redis.connection.RedisStreamCommands.XAddOptions;
import org.springframework.data.redis.connection.RedisStreamCommands.XPendingOptions;
import org.springframework.data.redis.connection.RedisStreamCommands.XTrimOptions;
import org.springframework.data.redis.connection.stream.RecordId;

/**
 * @author Jeonggyu Choi
 * @author Christoph Strobl
 * @author Viktoriya Kutsarova
 */
class StreamConvertersUnitTest {

	@Test // GH-2046
	void shouldConvertIdle() {

		XPendingOptions options = XPendingOptions.unbounded(5L).minIdleTime(Duration.of(1, ChronoUnit.HOURS));

		XPendingParams xPendingParams = StreamConverters.toXPendingParams(options);

		assertThat(xPendingParams).hasFieldOrPropertyWithValue("idle", Duration.of(1, ChronoUnit.HOURS).toMillis());
	}

	@Test // GH-3232
	void shouldConvertXAddOptionsWithMaxlen() {

		RecordId recordId = RecordId.autoGenerate();
		XAddOptions options = XAddOptions.maxlen(100);

		XAddParams params = StreamConverters.toXAddParams(recordId, options);

		assertThat(params).hasFieldOrPropertyWithValue("maxLen", 100L);
	}

	@Test // GH-3232
	void shouldConvertXAddOptionsWithMinId() {

		RecordId recordId = RecordId.autoGenerate();
		XAddOptions options = XAddOptions.minId(RecordId.of("1234567890-0"));

		XAddParams params = StreamConverters.toXAddParams(recordId, options);

		assertThat(params).hasFieldOrPropertyWithValue("minId", "1234567890-0");
	}

	@Test // GH-3232
	void shouldConvertXAddOptionsWithApproximateTrimming() {

		RecordId recordId = RecordId.autoGenerate();
		XAddOptions options = XAddOptions.maxlen(100).approximateTrimming(true);

		XAddParams params = StreamConverters.toXAddParams(recordId, options);

		assertThat(params).hasFieldOrPropertyWithValue("approximateTrimming", true);
	}

	@Test // GH-3232
	void shouldConvertXAddOptionsWithExactTrimming() {

		RecordId recordId = RecordId.autoGenerate();
		XAddOptions options = XAddOptions.maxlen(100).withExactTrimming(true);

		XAddParams params = StreamConverters.toXAddParams(recordId, options);

		assertThat(params).hasFieldOrPropertyWithValue("exactTrimming", true);
	}

	@Test // GH-3232
	void shouldConvertXAddOptionsWithLimit() {

		RecordId recordId = RecordId.autoGenerate();
		XAddOptions options = XAddOptions.maxlen(100).approximateTrimming(true).withLimit(50);

		XAddParams params = StreamConverters.toXAddParams(recordId, options);

		assertThat(params).hasFieldOrPropertyWithValue("limit", 50L);
	}

	@Test // GH-3232
	void shouldConvertXAddOptionsWithDeletionPolicy() {

		RecordId recordId = RecordId.autoGenerate();
		XAddOptions options = XAddOptions.maxlen(100).withDeletionPolicy(StreamDeletionPolicy.KEEP_REFERENCES);

		XAddParams params = StreamConverters.toXAddParams(recordId, options);

		assertThat(params).hasFieldOrPropertyWithValue("trimMode",
				redis.clients.jedis.args.StreamDeletionPolicy.KEEP_REFERENCES);
	}

	@Test // GH-3232
	void shouldConvertXAddOptionsWithRecordId() {

		RecordId recordId = RecordId.of("1234567890-0");
		XAddOptions options = XAddOptions.none();

		XAddParams params = StreamConverters.toXAddParams(recordId, options);

		assertThat(params).hasFieldOrPropertyWithValue("maxLen", null);
		assertThat(params).hasFieldOrPropertyWithValue("minId", null);
		assertThat(params).hasFieldOrPropertyWithValue("limit", null);
		assertThat(params).hasFieldOrPropertyWithValue("trimMode", null);
		assertThat(params).hasFieldOrPropertyWithValue("nomkstream", false);
		assertThat(params).hasFieldOrPropertyWithValue("exactTrimming", true);
		assertThat(params).hasFieldOrPropertyWithValue("approximateTrimming", false);
	}

	@Test // GH-3232
	void shouldConvertXTrimOptionsWithMaxlen() {

		XTrimOptions options = XTrimOptions.maxlen(100);

		XTrimParams params = StreamConverters.toXTrimParams(options);

		assertThat(params).hasFieldOrPropertyWithValue("maxLen", 100L);
	}

	@Test // GH-3232
	void shouldConvertXTrimOptionsWithMinId() {

		XTrimOptions options = XTrimOptions.minId(RecordId.of("1234567890-0"));

		XTrimParams params = StreamConverters.toXTrimParams(options);

		assertThat(params).hasFieldOrPropertyWithValue("minId", "1234567890-0");
	}

	@Test // GH-3232
	void shouldConvertXTrimOptionsWithApproximateTrimming() {

		XTrimOptions options = XTrimOptions.maxlen(100).approximateTrimming(true);

		XTrimParams params = StreamConverters.toXTrimParams(options);

		assertThat(params).hasFieldOrPropertyWithValue("approximateTrimming", true);
	}

	@Test // GH-3232
	void shouldConvertXTrimOptionsWithExactTrimming() {

		XTrimOptions options = XTrimOptions.maxlen(100).exactTrimming(true);

		XTrimParams params = StreamConverters.toXTrimParams(options);

		assertThat(params).hasFieldOrPropertyWithValue("exactTrimming", true);
	}

	@Test // GH-3232
	void shouldConvertXTrimOptionsWithLimit() {

		XTrimOptions options = XTrimOptions.maxlen(100).approximateTrimming(true).limit(50);

		XTrimParams params = StreamConverters.toXTrimParams(options);

		assertThat(params).hasFieldOrPropertyWithValue("limit", 50L);
	}

	@Test // GH-3232
	void shouldConvertXTrimOptionsWithDeletionPolicy() {

		XTrimOptions options = XTrimOptions.maxlen(100).deletionPolicy(StreamDeletionPolicy.KEEP_REFERENCES);

		XTrimParams params = StreamConverters.toXTrimParams(options);

		assertThat(params).hasFieldOrPropertyWithValue("trimMode",
				redis.clients.jedis.args.StreamDeletionPolicy.KEEP_REFERENCES);
	}
}
