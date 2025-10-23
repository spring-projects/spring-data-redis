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

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import org.springframework.data.redis.connection.RedisStreamCommands.StreamDeletionPolicy;
import org.springframework.data.redis.connection.RedisStreamCommands.XAddOptions;
import org.springframework.data.redis.connection.RedisStreamCommands.XDelOptions;
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

	@Nested // GH-3232
	class ToXAddParamsShould {

		@Test
		void convertXAddOptionsWithMaxlen() {

			RecordId recordId = RecordId.autoGenerate();
			XAddOptions options = XAddOptions.maxlen(100);

			XAddParams params = StreamConverters.toXAddParams(recordId, options);

			assertThat(params).hasFieldOrPropertyWithValue("maxLen", 100L);
		}

		@Test
		void convertXAddOptionsWithMinId() {

			RecordId recordId = RecordId.autoGenerate();
			XAddOptions options = XAddOptions.minId(RecordId.of("1234567890-0"));

			XAddParams params = StreamConverters.toXAddParams(recordId, options);

			assertThat(params).hasFieldOrPropertyWithValue("minId", "1234567890-0");
		}

		@Test
		void convertXAddOptionsWithApproximateTrimming() {

			RecordId recordId = RecordId.autoGenerate();
			XAddOptions options = XAddOptions.maxlen(100).approximateTrimming(true);

			XAddParams params = StreamConverters.toXAddParams(recordId, options);

			assertThat(params).hasFieldOrPropertyWithValue("approximateTrimming", true);
		}

		@Test
		void convertXAddOptionsWithExactTrimming() {

			RecordId recordId = RecordId.autoGenerate();
			XAddOptions options = XAddOptions.maxlen(100).withExactTrimming(true);

			XAddParams params = StreamConverters.toXAddParams(recordId, options);

			assertThat(params).hasFieldOrPropertyWithValue("exactTrimming", true);
		}

		@Test
		void convertXAddOptionsWithLimit() {

			RecordId recordId = RecordId.autoGenerate();
			XAddOptions options = XAddOptions.maxlen(100).approximateTrimming(true).withLimit(50);

			XAddParams params = StreamConverters.toXAddParams(recordId, options);

			assertThat(params).hasFieldOrPropertyWithValue("limit", 50L);
		}

		@Test
		void convertXAddOptionsWithDeletionPolicy() {

			RecordId recordId = RecordId.autoGenerate();
			XAddOptions options = XAddOptions.maxlen(100).withDeletionPolicy(StreamDeletionPolicy.KEEP_REFERENCES);

			XAddParams params = StreamConverters.toXAddParams(recordId, options);

			assertThat(params).hasFieldOrPropertyWithValue("trimMode",
					redis.clients.jedis.args.StreamDeletionPolicy.KEEP_REFERENCES);
		}

		@Test
		void convertXAddOptionsWithRecordId() {

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
	}

	@Nested // GH-3232
	class ToXTrimParamsShould {

		@Test
		void convertXTrimOptionsWithMaxlen() {

			XTrimOptions options = XTrimOptions.maxlen(100);

			XTrimParams params = StreamConverters.toXTrimParams(options);

			assertThat(params).hasFieldOrPropertyWithValue("maxLen", 100L);
		}

		@Test
		void convertXTrimOptionsWithMinId() {

			XTrimOptions options = XTrimOptions.minId(RecordId.of("1234567890-0"));

			XTrimParams params = StreamConverters.toXTrimParams(options);

			assertThat(params).hasFieldOrPropertyWithValue("minId", "1234567890-0");
		}

		@Test
		void convertXTrimOptionsWithApproximateTrimming() {

			XTrimOptions options = XTrimOptions.maxlen(100).approximateTrimming(true);

			XTrimParams params = StreamConverters.toXTrimParams(options);

			assertThat(params).hasFieldOrPropertyWithValue("approximateTrimming", true);
		}

		@Test
		void convertXTrimOptionsWithExactTrimming() {

			XTrimOptions options = XTrimOptions.maxlen(100).exactTrimming(true);

			XTrimParams params = StreamConverters.toXTrimParams(options);

			assertThat(params).hasFieldOrPropertyWithValue("exactTrimming", true);
		}

		@Test
		void convertXTrimOptionsWithLimit() {

			XTrimOptions options = XTrimOptions.maxlen(100).approximateTrimming(true).limit(50);

			XTrimParams params = StreamConverters.toXTrimParams(options);

			assertThat(params).hasFieldOrPropertyWithValue("limit", 50L);
		}

		@Test
		void convertXTrimOptionsWithDeletionPolicy() {

			XTrimOptions options = XTrimOptions.maxlen(100).deletionPolicy(StreamDeletionPolicy.KEEP_REFERENCES);

			XTrimParams params = StreamConverters.toXTrimParams(options);

			assertThat(params).hasFieldOrPropertyWithValue("trimMode",
					redis.clients.jedis.args.StreamDeletionPolicy.KEEP_REFERENCES);
		}
	}

	@Nested // GH-3232
	class ToStreamDeletionPolicyShould {

		@Test
		void convertDefaultOptions() {

			XDelOptions options = XDelOptions.defaultOptions();

			redis.clients.jedis.args.StreamDeletionPolicy policy = StreamConverters.toStreamDeletionPolicy(options);

			assertThat(policy).isEqualTo(redis.clients.jedis.args.StreamDeletionPolicy.KEEP_REFERENCES);
		}

		@Test
		void convertKeepReferencesPolicy() {

			XDelOptions options = XDelOptions.deletionPolicy(StreamDeletionPolicy.KEEP_REFERENCES);

			redis.clients.jedis.args.StreamDeletionPolicy policy = StreamConverters.toStreamDeletionPolicy(options);

			assertThat(policy).isEqualTo(redis.clients.jedis.args.StreamDeletionPolicy.KEEP_REFERENCES);
		}

		@Test
		void convertDeleteReferencesPolicy() {

			XDelOptions options = XDelOptions.deletionPolicy(StreamDeletionPolicy.DELETE_REFERENCES);

			redis.clients.jedis.args.StreamDeletionPolicy policy = StreamConverters.toStreamDeletionPolicy(options);

			assertThat(policy).isEqualTo(redis.clients.jedis.args.StreamDeletionPolicy.DELETE_REFERENCES);
		}

		@Test
		void convertAcknowledgedPolicy() {

			XDelOptions options = XDelOptions.deletionPolicy(StreamDeletionPolicy.ACKNOWLEDGED);

			redis.clients.jedis.args.StreamDeletionPolicy policy = StreamConverters.toStreamDeletionPolicy(options);

			assertThat(policy).isEqualTo(redis.clients.jedis.args.StreamDeletionPolicy.ACKNOWLEDGED);
		}
	}
}
