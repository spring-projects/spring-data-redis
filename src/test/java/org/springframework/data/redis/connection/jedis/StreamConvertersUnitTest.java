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
import static org.junit.jupiter.params.provider.Arguments.*;

import redis.clients.jedis.params.XAddParams;
import redis.clients.jedis.params.XPendingParams;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.stream.Stream;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import org.springframework.data.redis.connection.RedisStreamCommands.StreamDeletionPolicy;
import org.springframework.data.redis.connection.RedisStreamCommands.TrimOptions;
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

		@ParameterizedTest
		@DisplayName("ConvertXAddOptions")
		@MethodSource
		void convertXAddOptions(XAddOptions options, String paramsFieldToCheck, Object paramsFieldExpectedValue) {
			assertThat(StreamConverters.toXAddParams(RecordId.autoGenerate(), options)).hasFieldOrPropertyWithValue(paramsFieldToCheck, paramsFieldExpectedValue);
		}

		static Stream<Arguments> convertXAddOptions() {
			return Stream.of(
					argumentSet("withMaxLen", XAddOptions.trim(TrimOptions.maxLen(100)), "maxLen", 100L),
					argumentSet("withMinId", XAddOptions.trim(TrimOptions.minId(RecordId.of("1234567890-0"))), "minId", "1234567890-0"),
					argumentSet("withApproximateTrimming", XAddOptions.trim(TrimOptions.maxLen(100).approximate()), "approximateTrimming", true),
					argumentSet("withExactTrimming", XAddOptions.trim(TrimOptions.maxLen(100).exact()), "exactTrimming", true),
					argumentSet("withLimit", XAddOptions.trim(TrimOptions.maxLen(100).approximate().limit(50)), "limit", 50L),
					argumentSet("withDeletionPolicy", XAddOptions.trim(TrimOptions.maxLen(100).deletionPolicy(StreamDeletionPolicy.keep())), "trimMode", redis.clients.jedis.args.StreamDeletionPolicy.KEEP_REFERENCES)
			);
		}

		@Test
		void convertXAddOptionsWithRecordId() {

			String testId = "1234567890-0";
			RecordId recordId = RecordId.of(testId);
			XAddOptions options = XAddOptions.none();

			XAddParams params = StreamConverters.toXAddParams(recordId, options);

			assertThat(params).extracting("id.raw").asInstanceOf(InstanceOfAssertFactories.BYTE_ARRAY)
					.satisfies(idRawBytes -> assertThat(new String(idRawBytes)).isEqualTo(testId));
		}
	}

	@ParameterizedTest // GH-3232
	@DisplayName("ToXTrimParamsShouldConvertXTrimOptions")
	@MethodSource
	void toXTrimParamsShouldConvertXTrimOptions(XTrimOptions options, String paramsFieldToCheck, Object paramsFieldExpectedValue) {
		assertThat(StreamConverters.toXTrimParams(options)).hasFieldOrPropertyWithValue(paramsFieldToCheck, paramsFieldExpectedValue);
	}

	static Stream<Arguments> toXTrimParamsShouldConvertXTrimOptions() {
		return Stream.of(
				argumentSet("withMaxLen", XTrimOptions.trim(TrimOptions.maxLen(100)), "maxLen", 100L),
				argumentSet("withMinId", XTrimOptions.trim(TrimOptions.minId(RecordId.of("1234567890-0"))), "minId", "1234567890-0"),
				argumentSet("withApproximateTrimming", XTrimOptions.trim(TrimOptions.maxLen(100).approximate()), "approximateTrimming", true),
				argumentSet("withExactTrimming", XTrimOptions.trim(TrimOptions.maxLen(100).exact()), "exactTrimming", true),
				argumentSet("withLimit", XTrimOptions.trim(TrimOptions.maxLen(100).approximate().limit(50)), "limit", 50L),
				argumentSet("withDeletionPolicy", XTrimOptions.trim(TrimOptions.maxLen(100).deletionPolicy(StreamDeletionPolicy.keep())), "trimMode", redis.clients.jedis.args.StreamDeletionPolicy.KEEP_REFERENCES)
		);
	}

	@ParameterizedTest // GH-3232
	@DisplayName("ToStreamDeletionPolicyShouldConvertXDelOptions")
	@MethodSource
	void toStreamDeletionPolicyShouldConvertXDelOptions(XDelOptions options, redis.clients.jedis.args.StreamDeletionPolicy expectedDeletionPolicy) {
		assertThat(StreamConverters.toStreamDeletionPolicy(options)).isEqualTo(expectedDeletionPolicy);
	}

	static Stream<Arguments> toStreamDeletionPolicyShouldConvertXDelOptions() {
		return Stream.of(
				argumentSet("withDefaultOptions", XDelOptions.defaults(), redis.clients.jedis.args.StreamDeletionPolicy.KEEP_REFERENCES),
				argumentSet("withKeepReferencesPolicy", XDelOptions.deletionPolicy(StreamDeletionPolicy.keep()), redis.clients.jedis.args.StreamDeletionPolicy.KEEP_REFERENCES),
				argumentSet("withDeleteReferencesPolicy", XDelOptions.deletionPolicy(StreamDeletionPolicy.delete()), redis.clients.jedis.args.StreamDeletionPolicy.DELETE_REFERENCES),
				argumentSet("withRemoveAcknowledgedPolicy", XDelOptions.deletionPolicy(StreamDeletionPolicy.removeAcknowledged()), redis.clients.jedis.args.StreamDeletionPolicy.ACKNOWLEDGED)
		);
	}

}
