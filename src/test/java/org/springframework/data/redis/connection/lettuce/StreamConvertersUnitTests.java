/*
 * Copyright 2025 the original author or authors.
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
package org.springframework.data.redis.connection.lettuce;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.util.ReflectionTestUtils.getField;

import io.lettuce.core.XAddArgs;
import io.lettuce.core.XTrimArgs;

import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.RedisStreamCommands.StreamDeletionPolicy;
import org.springframework.data.redis.connection.RedisStreamCommands.XAddOptions;
import org.springframework.data.redis.connection.RedisStreamCommands.XTrimOptions;
import org.springframework.data.redis.connection.stream.RecordId;

/**
 * Unit tests for {@link StreamConverters}.
 *
 * @author Viktoriya Kutsarova
 */
class StreamConvertersUnitTests {

	@Test // GH-3232
	void shouldConvertXAddOptionsWithMaxlen() {

		RecordId recordId = RecordId.autoGenerate();
		XAddOptions options = XAddOptions.maxlen(100);

		XAddArgs args = StreamConverters.toXAddArgs(recordId, options);

		assertThat(args).extracting("maxlen").isEqualTo(100L);
	}

	@Test // GH-3232
	void shouldConvertXAddOptionsWithMinId() {

		RecordId recordId = RecordId.autoGenerate();
		XAddOptions options = XAddOptions.minId(RecordId.of("1234567890-0"));

		XAddArgs args = StreamConverters.toXAddArgs(recordId, options);

		assertThat(getField(args, "minid")).isEqualTo("1234567890-0");
	}

	@Test // GH-3232
	void shouldConvertXAddOptionsWithApproximateTrimming() {

		RecordId recordId = RecordId.autoGenerate();
		XAddOptions options = XAddOptions.maxlen(100).approximateTrimming(true);

		XAddArgs args = StreamConverters.toXAddArgs(recordId, options);

		assertThat(args).extracting("approximateTrimming").isEqualTo(true);
	}

	@Test // GH-3232
	void shouldConvertXAddOptionsWithExactTrimming() {

		RecordId recordId = RecordId.autoGenerate();
		XAddOptions options = XAddOptions.maxlen(100).exactTrimming(true);

		XAddArgs args = StreamConverters.toXAddArgs(recordId, options);

		assertThat(args).extracting("exactTrimming").isEqualTo(true);
	}

	@Test // GH-3232
	void shouldConvertXAddOptionsWithLimit() {

		RecordId recordId = RecordId.autoGenerate();
		XAddOptions options = XAddOptions.maxlen(100).approximateTrimming(true).withLimit(50);

		XAddArgs args = StreamConverters.toXAddArgs(recordId, options);

		assertThat(args).extracting("limit").isEqualTo(50L);
	}

	@Test // GH-3232
	void shouldConvertXAddOptionsWithDeletionPolicy() {

		RecordId recordId = RecordId.autoGenerate();
		XAddOptions options = XAddOptions.maxlen(100).withDeletionPolicy(StreamDeletionPolicy.KEEP_REFERENCES);

		XAddArgs args = StreamConverters.toXAddArgs(recordId, options);

		assertThat(args).extracting("trimmingMode").isEqualTo(io.lettuce.core.StreamDeletionPolicy.KEEP_REFERENCES);
	}

	@Test // GH-3232
	void shouldConvertXAddOptionsWithRecordId() {

		RecordId recordId = RecordId.of("1234567890-0");
		XAddOptions options = XAddOptions.none();

		XAddArgs args = StreamConverters.toXAddArgs(recordId, options);

		assertThat(getField(args, "id")).isEqualTo("1234567890-0");
	}

	@Test // GH-3232
	void shouldConvertXTrimOptionsWithMaxlen() {

		XTrimOptions options = XTrimOptions.maxlen(100);

		XTrimArgs args = StreamConverters.toXTrimArgs(options);

		assertThat(args).extracting("maxlen").isEqualTo(100L);
	}

	@Test // GH-3232
	void shouldConvertXTrimOptionsWithMinId() {

		XTrimOptions options = XTrimOptions.minId(RecordId.of("1234567890-0"));

		XTrimArgs args = StreamConverters.toXTrimArgs(options);

		assertThat(getField(args, "minId")).isEqualTo("1234567890-0");
	}

	@Test // GH-3232
	void shouldConvertXTrimOptionsWithApproximateTrimming() {

		XTrimOptions options = XTrimOptions.maxlen(100).approximateTrimming(true);

		XTrimArgs args = StreamConverters.toXTrimArgs(options);

		assertThat(args).extracting("approximateTrimming").isEqualTo(true);
	}

	@Test // GH-3232
	void shouldConvertXTrimOptionsWithExactTrimming() {

		XTrimOptions options = XTrimOptions.maxlen(100).exactTrimming(true);

		XTrimArgs args = StreamConverters.toXTrimArgs(options);

		assertThat(args).extracting("exactTrimming").isEqualTo(true);
	}

	@Test // GH-3232
	void shouldConvertXTrimOptionsWithLimit() {

		XTrimOptions options = XTrimOptions.maxlen(100).approximateTrimming(true).limit(50);

		XTrimArgs args = StreamConverters.toXTrimArgs(options);

		assertThat(args).extracting("limit").isEqualTo(50L);
	}

	@Test // GH-3232
	void shouldConvertXTrimOptionsWithDeletionPolicy() {

		XTrimOptions options = XTrimOptions.maxlen(100).deletionPolicy(StreamDeletionPolicy.KEEP_REFERENCES);

		XTrimArgs args = StreamConverters.toXTrimArgs(options);

		assertThat(args).extracting("trimmingMode").isEqualTo(io.lettuce.core.StreamDeletionPolicy.KEEP_REFERENCES);
	}
}

