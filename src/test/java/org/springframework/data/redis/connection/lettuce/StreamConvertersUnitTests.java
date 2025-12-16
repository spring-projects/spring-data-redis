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
package org.springframework.data.redis.connection.lettuce;

import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.*;

import io.lettuce.core.XAddArgs;
import java.util.stream.Stream;

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
import org.springframework.data.redis.connection.RedisStreamCommands.XTrimOptions;
import org.springframework.data.redis.connection.stream.RecordId;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Jimin Sin
 */
class StreamConvertersUnitTests {

	@Nested // GH-3232
	class ToXAddArgsShould {

		@ParameterizedTest
		@DisplayName("ConvertXAddOptions")
		@MethodSource
		void convertXAddOptions(XAddOptions options, String paramsFieldToCheck, Object paramsFieldExpectedValue) {
			assertThat(StreamConverters.toXAddArgs(RecordId.autoGenerate(), options)).hasFieldOrPropertyWithValue(paramsFieldToCheck, paramsFieldExpectedValue);
		}

		static Stream<Arguments> convertXAddOptions() {
			return Stream.of(
					argumentSet("withMaxLen", XAddOptions.trim(TrimOptions.maxLen(100)), "maxlen", 100L),
					argumentSet("withMinId", XAddOptions.trim(TrimOptions.minId(RecordId.of("1234567890-0"))), "minid", "1234567890-0"),
					argumentSet("withApproximateTrimming", XAddOptions.trim(TrimOptions.maxLen(100).approximate()), "approximateTrimming", true),
					argumentSet("withExactTrimming", XAddOptions.trim(TrimOptions.maxLen(100).exact()), "exactTrimming", true),
					argumentSet("withLimit", XAddOptions.trim(TrimOptions.maxLen(100).approximate().limit(50)), "limit", 50L),
					argumentSet("withDeletionPolicy", XAddOptions.trim(TrimOptions.maxLen(100).deletionPolicy(StreamDeletionPolicy.keep())), "trimmingMode", io.lettuce.core.StreamDeletionPolicy.KEEP_REFERENCES)
			);
		}

		@Test // GH-2047
		void convertXAddOptionsWithNoMkStream() {

			XAddOptions options = XAddOptions.makeNoStream();

			XAddArgs args = StreamConverters.toXAddArgs(RecordId.autoGenerate(), options);

			assertThat(args).hasFieldOrPropertyWithValue("nomkstream", true);
		}
	}

	@Nested // GH-3232
	class ToXTrimArgsShould {

		@ParameterizedTest
		@DisplayName("ToXTrimArgsShouldConvertXTrimOptions")
		@MethodSource
		void toXTrimArgsShouldConvertXTrimOptions(XTrimOptions options, String paramsFieldToCheck, Object paramsFieldExpectedValue) {
			assertThat(StreamConverters.toXTrimArgs(options)).hasFieldOrPropertyWithValue(paramsFieldToCheck, paramsFieldExpectedValue);
		}

		static Stream<Arguments> toXTrimArgsShouldConvertXTrimOptions() {
			return Stream.of(
				argumentSet("withMaxLen", XTrimOptions.trim(TrimOptions.maxLen(100)), "maxlen", 100L),
				argumentSet("withMinId", XTrimOptions.trim(TrimOptions.minId(RecordId.of("1234567890-0"))), "minId", "1234567890-0"),
				argumentSet("withApproximateTrimming", XTrimOptions.trim(TrimOptions.maxLen(100).approximate()), "approximateTrimming", true),
				argumentSet("withExactTrimming", XTrimOptions.trim(TrimOptions.maxLen(100).exact()), "exactTrimming", true),
				argumentSet("withLimit", XTrimOptions.trim(TrimOptions.maxLen(100).approximate().limit(50)), "limit", 50L),
				argumentSet("withDeletionPolicy", XTrimOptions.trim(TrimOptions.maxLen(100).deletionPolicy(StreamDeletionPolicy.keep())), "trimmingMode", io.lettuce.core.StreamDeletionPolicy.KEEP_REFERENCES)
			);
		}
	}

	@ParameterizedTest // GH-3232
	@DisplayName("ToStreamDeletionPolicyShouldConvertXDelOptions")
	@MethodSource
	void toStreamDeletionPolicyShouldConvertXDelOptions(XDelOptions options, io.lettuce.core.StreamDeletionPolicy expectedDeletionPolicy) {
		assertThat(StreamConverters.toXDelArgs(options)).isEqualTo(expectedDeletionPolicy);
	}

	static Stream<Arguments> toStreamDeletionPolicyShouldConvertXDelOptions() {
		return Stream.of(
				argumentSet("withDefaultOptions", XDelOptions.defaults(), io.lettuce.core.StreamDeletionPolicy.KEEP_REFERENCES),
				argumentSet("withKeepReferencesPolicy", XDelOptions.deletionPolicy(StreamDeletionPolicy.keep()), io.lettuce.core.StreamDeletionPolicy.KEEP_REFERENCES),
				argumentSet("withDeleteReferencesPolicy", XDelOptions.deletionPolicy(StreamDeletionPolicy.delete()), io.lettuce.core.StreamDeletionPolicy.DELETE_REFERENCES),
				argumentSet("withRemoveAcknowledgedPolicy", XDelOptions.deletionPolicy(StreamDeletionPolicy.removeAcknowledged()), io.lettuce.core.StreamDeletionPolicy.ACKNOWLEDGED)
		);
	}

}