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

import java.nio.charset.StandardCharsets;
import java.util.List;

import org.junit.jupiter.api.Test;

import org.springframework.core.ParameterizedTypeReference;
import org.springframework.data.redis.core.RedisJsonTemplate.DefaultJsonResult;
import org.springframework.data.redis.serializer.GenericJacksonJsonRedisSerializer;
import org.springframework.data.redis.serializer.RedisJsonSerializer;
import org.springframework.data.redis.serializer.SerializationException;

/**
 * Unit tests for {@link DefaultJsonResult}.
 *
 * @author Moritz Halbritter
 */
class DefaultJsonResultUnitTests {

	private final RedisJsonSerializer serializer = GenericJacksonJsonRedisSerializer.builder().build();

	@Test
	void isNullAndExistsAcrossTheFiveStates() {

		DefaultJsonResult missingKey = DefaultJsonResult.ofMatchArray(serializer, null);
		DefaultJsonResult noMatch = DefaultJsonResult.ofMatchArray(serializer, "[]".getBytes());
		DefaultJsonResult jsonNullMatch = DefaultJsonResult.ofMatchArray(serializer, "[null]".getBytes());
		DefaultJsonResult oneMatch = DefaultJsonResult.ofMatchArray(serializer, "[\"John\"]".getBytes());
		DefaultJsonResult severalMatches = DefaultJsonResult.ofMatchArray(serializer, "[\"Berlin\",\"Hamburg\"]".getBytes());

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

	@Test
	void matchesChildOverJsonNullReportsIsNull() {

		DefaultJsonResult result = DefaultJsonResult.ofMatchArray(serializer, "[null]".getBytes());
		JsonOperations.JsonResult child = result.matches().iterator().next();

		assertThat(child.isNull()).isTrue();
	}

	@Test
	void ofMatchArrayAcceptsLeadingWhitespace() {
		assertThat(DefaultJsonResult.ofMatchArray(serializer, "  [1]".getBytes()).as(Long.class)).isEqualTo(1L);
	}

	@Test // a non-array payload is rejected when the elements are needed, not at construction time
	void ofMatchArrayRejectsNonArrayPayloadOnUse() {

		DefaultJsonResult result = DefaultJsonResult.ofMatchArray(serializer, "{\"a\":1}".getBytes());

		assertThatExceptionOfType(SerializationException.class).isThrownBy(result::matches);
		assertThatExceptionOfType(SerializationException.class).isThrownBy(result::isNull);
	}

	@Test // the payload is not validated at construction time, only when it is read
	void ofValueAcceptsAnyPayload() {
		assertThat(DefaultJsonResult.ofValue(serializer, "foo".getBytes()).asBytes()).isEqualTo("foo".getBytes());
	}

	@Test
	void matchArrayWithArrayValueDecodesAsList() {

		DefaultJsonResult result = DefaultJsonResult.ofMatchArray(serializer, "[[1,2,3]]".getBytes());

		assertThat(result.as(new ParameterizedTypeReference<List<Long>>() {})).containsExactly(1L, 2L, 3L);
	}

	@Test
	void matchArrayWithZeroMatchesDecodesToNull() {
		assertThat(DefaultJsonResult.ofMatchArray(serializer, "[]".getBytes()).as(String.class)).isNull();
	}

	@Test
	void matchArrayWithMultipleMatchesThrowsOnAs() {

		DefaultJsonResult result = DefaultJsonResult.ofMatchArray(serializer, "[\"a\",\"b\"]".getBytes());

		assertThatExceptionOfType(SerializationException.class).isThrownBy(() -> result.as(String.class));
	}

	@Test
	void nonMatchArrayDecodesDirectlyWithoutUnwrap() {
		assertThat(DefaultJsonResult.ofValue(serializer, "\"John\"".getBytes()).as(String.class)).isEqualTo("John");
	}

	@Test
	void matchesOnAbsentKeyIsEmpty() {
		assertThat(DefaultJsonResult.ofMatchArray(serializer, null).matches()).isEmpty();
	}

	@Test
	void matchesOnMatchArraySplitsIntoOneResultPerMatch() {

		DefaultJsonResult result = DefaultJsonResult.ofMatchArray(serializer, "[[\"admin\",\"dev\"],[\"ops\"]]".getBytes());

		assertThat(result.matches().as(new ParameterizedTypeReference<List<String>>() {}))
				.containsExactly(List.of("admin", "dev"), List.of("ops"));
	}

	@Test
	void matchesOnNonMatchArrayYieldsExactlyOneElement() {

		DefaultJsonResult result = DefaultJsonResult.ofValue(serializer, "[\"a\",\"b\"]".getBytes());

		assertThat(result.matches()).hasSize(1);
		assertThat(result.matches().as(new ParameterizedTypeReference<List<String>>() {}))
				.containsExactly(List.of("a", "b"));
	}

	@Test
	void matchesChildReturnsOriginalWireBytes() {

		DefaultJsonResult result = DefaultJsonResult.ofMatchArray(serializer, "[1.10,2]".getBytes());

		assertThat(result.matches().asBytes()).extracting(bytes -> new String(bytes, StandardCharsets.UTF_8))
				.containsExactly("1.10", "2");
	}

	@Test
	void absentKeyHasNullBytesAndStringAndSkipsMap() {

		DefaultJsonResult result = DefaultJsonResult.ofMatchArray(serializer, null);

		assertThat(result.asBytes()).isNull();
		assertThat(result.asString()).isNull();
		assertThat((Object) result.map(bytes -> new Object())).isNull();
	}

	@Test
	void mapIsInvokedForZeroMatchesAndForJsonNull() {

		DefaultJsonResult noMatch = DefaultJsonResult.ofMatchArray(serializer, "[]".getBytes());
		DefaultJsonResult jsonNull = DefaultJsonResult.ofMatchArray(serializer, "[null]".getBytes());

		assertThat(noMatch.map((byte[] bytes) -> new String(bytes, StandardCharsets.UTF_8))).isEqualTo("[]");
		assertThat(jsonNull.map((byte[] bytes) -> new String(bytes, StandardCharsets.UTF_8))).isEqualTo("[null]");
	}

	@Test
	void asStringIsRawWireTextNotDecodedValue() {
		assertThat(DefaultJsonResult.ofMatchArray(serializer, "[\"John\"]".getBytes()).asString())
				.isEqualTo("[\"John\"]");
	}

}
