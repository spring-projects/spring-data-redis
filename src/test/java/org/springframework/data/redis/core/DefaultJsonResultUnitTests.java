/*
 * Copyright 2026-present the original author or authors.
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
package org.springframework.data.redis.core;

import static org.assertj.core.api.Assertions.*;

import java.util.List;

import org.junit.jupiter.api.Test;

import org.springframework.core.ParameterizedTypeReference;
import org.springframework.data.redis.core.JsonOperations.JsonResult;
import org.springframework.data.redis.core.RedisJsonTemplate.DefaultJsonResult;
import org.springframework.data.redis.serializer.GenericJacksonJsonRedisSerializer;
import org.springframework.data.redis.serializer.RedisJsonSerializer;
import org.springframework.data.redis.serializer.SerializationException;
import org.springframework.data.redis.util.ByteUtils;

/**
 * Unit tests for {@link DefaultJsonResult}.
 *
 * @author Moritz Halbritter
 * @author Mark Paluch
 */
class DefaultJsonResultUnitTests {

	RedisJsonSerializer serializer = GenericJacksonJsonRedisSerializer.builder().build();

	@Test // GH-3433
	void isNullAndExistsAcrossTheFiveStates() {

		JsonResult missingKey = DefaultJsonResult.ofMatchArray(serializer, null);
		JsonResult noMatch = DefaultJsonResult.ofMatchArray(serializer, "[]".getBytes());
		JsonResult jsonNullMatch = DefaultJsonResult.ofMatchArray(serializer, "[null]".getBytes());
		JsonResult oneMatch = DefaultJsonResult.ofMatchArray(serializer, "[\"John\"]".getBytes());
		JsonResult severalMatches = DefaultJsonResult.ofMatchArray(serializer, "[\"Berlin\",\"Hamburg\"]".getBytes());

		assertThat(missingKey.exists()).isFalse();
		assertThat(missingKey.isNull()).isFalse();

		assertThat(noMatch.exists()).isTrue();
		assertThat(noMatch.isNull()).isFalse();

		assertThat(jsonNullMatch.exists()).isTrue();
		assertThat(jsonNullMatch.isNull()).isTrue();

		assertThat(oneMatch.exists()).isTrue();
		assertThat(oneMatch.isNull()).isFalse();

		assertThat(severalMatches.exists()).isTrue();
		assertThat(severalMatches.isNull()).isFalse();
	}

	@Test // GH-3433
	void isNullWithIndividualNullMatch() {

		JsonResult result = DefaultJsonResult.ofMatchArray(serializer, ByteUtils.toUtf8Bytes("[null]"));
		JsonResult match = result.matches().iterator().next();

		assertThat(match.isNull()).isTrue();
	}

	@Test // GH-3433
	void asWithLeadingWhitespace() {

		JsonResult result = DefaultJsonResult.ofMatchArray(serializer, ByteUtils.toUtf8Bytes("  [1]"));

		assertThat(result.as(Long.class)).isEqualTo(1L);
	}

	@Test // GH-3433
	void matchesWithNonArrayPayload() {

		JsonResult result = DefaultJsonResult.ofMatchArray(serializer, ByteUtils.toUtf8Bytes("{\"a\":1}"));

		assertThatExceptionOfType(SerializationException.class).isThrownBy(result::matches);
	}

	@Test // GH-3433
	void isNullWithNonArrayPayload() {

		JsonResult result = DefaultJsonResult.ofMatchArray(serializer, ByteUtils.toUtf8Bytes("{\"a\":1}"));

		assertThatExceptionOfType(SerializationException.class).isThrownBy(result::isNull);
	}

	@Test // GH-3433
	void asBytesWithIndividualValue() {

		byte[] json = ByteUtils.toUtf8Bytes("\"John\"");
		JsonResult result = DefaultJsonResult.ofValue(serializer, json);

		assertThat(result.asBytes()).isEqualTo(json);
	}

	@Test // GH-3433
	void asWithArrayValue() {

		JsonResult result = DefaultJsonResult.ofMatchArray(serializer, ByteUtils.toUtf8Bytes("[[1,2,3]]"));

		assertThat(result.as(new ParameterizedTypeReference<List<Long>>() {})).containsExactly(1L, 2L, 3L);
	}

	@Test // GH-3433
	void asWithNoMatches() {

		JsonResult result = DefaultJsonResult.ofMatchArray(serializer, ByteUtils.toUtf8Bytes("[]"));

		assertThat(result.as(String.class)).isNull();
	}

	@Test // GH-3433
	void asWithMultipleMatches() {

		JsonResult result = DefaultJsonResult.ofMatchArray(serializer, ByteUtils.toUtf8Bytes("[\"a\",\"b\"]"));

		assertThatExceptionOfType(SerializationException.class).isThrownBy(() -> result.as(String.class));
	}

	@Test // GH-3433
	void asWithIndividualValue() {

		JsonResult result = DefaultJsonResult.ofValue(serializer, ByteUtils.toUtf8Bytes("\"John\""));

		assertThat(result.as(String.class)).isEqualTo("John");
	}

	@Test // GH-3433
	void matchesWithAbsentKey() {

		JsonResult result = DefaultJsonResult.ofMatchArray(serializer, null);

		assertThat(result.matches()).isEmpty();
	}

	@Test // GH-3433
	void matchesWithArrayValues() {

		JsonResult result = DefaultJsonResult.ofMatchArray(serializer,
				ByteUtils.toUtf8Bytes("[[\"admin\",\"dev\"],[\"ops\"]]"));

		assertThat(result.matches().as(new ParameterizedTypeReference<List<String>>() {}))
				.containsExactly(List.of("admin", "dev"), List.of("ops"));
	}

	@Test // GH-3433
	void matchesWithIndividualArrayValue() {

		JsonResult result = DefaultJsonResult.ofValue(serializer, ByteUtils.toUtf8Bytes("[\"a\",\"b\"]"));

		assertThat(result.matches()).hasSize(1);
		assertThat(result.matches().as(new ParameterizedTypeReference<List<String>>() {}))
				.containsExactly(List.of("a", "b"));
	}

	@Test // GH-3433
	void matchesPreserveNumberRepresentation() {

		JsonResult result = DefaultJsonResult.ofMatchArray(serializer, ByteUtils.toUtf8Bytes("[1.10,2]"));

		assertThat(result.matches().asBytes()).extracting(ByteUtils::toUtf8String)
				.containsExactly("1.10", "2");
	}

	@Test // GH-3433
	void absentKey() {

		JsonResult result = DefaultJsonResult.ofMatchArray(serializer, null);

		assertThat(result.asBytes()).isNull();
		assertThat(result.asString()).isNull();

		Object mapped = result.map(bytes -> {
			throw new AssertionError("Mapper must not be invoked for an absent key");
		});
		assertThat(mapped).isNull();
	}

	@Test // GH-3433
	void mapWithNoMatches() {

		JsonResult result = DefaultJsonResult.ofMatchArray(serializer, ByteUtils.toUtf8Bytes("[]"));

		String mapped = result.map(ByteUtils::toUtf8String);

		assertThat(mapped).isEqualTo("[]");
	}

	@Test // GH-3433
	void mapWithJsonNullMatch() {

		JsonResult result = DefaultJsonResult.ofMatchArray(serializer, ByteUtils.toUtf8Bytes("[null]"));

		String mapped = result.map(ByteUtils::toUtf8String);

		assertThat(mapped).isEqualTo("[null]");
	}

	@Test // GH-3433
	void asStringWithMatchArray() {

		JsonResult result = DefaultJsonResult.ofMatchArray(serializer, ByteUtils.toUtf8Bytes("[\"John\"]"));

		assertThat(result.asString()).isEqualTo("[\"John\"]");
	}

}
