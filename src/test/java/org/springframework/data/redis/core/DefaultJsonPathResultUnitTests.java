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
import java.util.Map;

import org.junit.jupiter.api.Test;

import org.springframework.core.ParameterizedTypeReference;
import org.springframework.data.redis.core.RedisJsonTemplate.DefaultJsonPathResult;
import org.springframework.data.redis.core.RedisJsonTemplate.RequestedPath;
import org.springframework.data.redis.serializer.GenericJacksonJsonRedisSerializer;
import org.springframework.data.redis.serializer.RedisJsonSerializer;
import org.springframework.data.redis.serializer.SerializationException;

/**
 * Unit tests for {@link DefaultJsonPathResult}.
 *
 * @author Moritz Halbritter
 * @author Mark Paluch
 */
class DefaultJsonPathResultUnitTests {

	private final RedisJsonSerializer serializer = GenericJacksonJsonRedisSerializer.builder().build();

	@Test // GH-3433
	void pathWithEscapedMemberNamePreservesNumberPrecision() {

		String reply = "{\"$['a\\\"b']\":[\"value\"],\"$.c\":[3.14159265358979323846]}";

		DefaultJsonPathResult result = newResult(reply, new RequestedPath("a\"b", "$['a\"b']"),
				new RequestedPath("c", "$.c"));

		// Resolving the escaped name must preserve the other member's JSON representation.
		assertThat(result.path("a\"b").asString()).isEqualTo("[\"value\"]");
		assertThat(result.path("c").asString()).isEqualTo("[3.14159265358979323846]");
	}

	@Test // GH-3433
	void absentKey() {

		DefaultJsonPathResult result = new DefaultJsonPathResult(serializer,
				List.of(new RequestedPath("name", "$['name']")), null);

		assertThat(result.exists()).isFalse();
		assertThat(result.asBytes()).isNull();
		assertThat(result.asString()).isNull();

		Map<?, ?> value = result.as(Map.class);
		assertThat(value).isNull();

		Object mapped = result.map(bytes -> {
			throw new AssertionError("Mapper must not be invoked for an absent key");
		});
		assertThat(mapped).isNull();
		assertThat(result.path("name").exists()).isFalse();
	}

	@Test // GH-3433
	void pathWithUnrequestedPathAndAbsentKey() {

		DefaultJsonPathResult result = new DefaultJsonPathResult(serializer,
				List.of(new RequestedPath("name", "$['name']")), null);

		assertThatIllegalArgumentException().isThrownBy(() -> result.path("age"))
				.withMessage("Path 'age' was not requested");
	}

	@Test // GH-3433
	void pathWithUnrequestedPath() {

		DefaultJsonPathResult result = newResult("[1]", new RequestedPath("name", "$['name']"));

		assertThatIllegalArgumentException().isThrownBy(() -> result.path("age"))
				.withMessage("Path 'age' was not requested");
	}

	@Test // GH-3433
	void asWithMultipleMatches() {

		String reply = "{\"$.name\":[\"Rand\"],\"$..city\":[\"Emond's Field\",\"Caemlyn\"]}";

		DefaultJsonPathResult result = newResult(reply, new RequestedPath("$.name", "$.name"),
				new RequestedPath("$..city", "$..city"));

		assertThatExceptionOfType(SerializationException.class).isThrownBy(() -> result.as(Map.class))
				.withMessageContaining("'$..city' matched more than once");
	}

	@Test // GH-3433
	void pathWithMultipleMatches() {

		String reply = "{\"$.name\":[\"Rand\"],\"$..city\":[\"Emond's Field\",\"Caemlyn\"]}";

		DefaultJsonPathResult result = newResult(reply, new RequestedPath("$.name", "$.name"),
				new RequestedPath("$..city", "$..city"));

		assertThat(result.path("$..city").matches().as(String.class)).containsExactly("Emond's Field", "Caemlyn");
	}

	@Test // GH-3433
	void asWithUnmatchedPath() {

		String reply = "{\"$.name\":[\"Rand\"],\"$.nope\":[]}";

		DefaultJsonPathResult result = newResult(reply, new RequestedPath("$.name", "$.name"),
				new RequestedPath("$.nope", "$.nope"));

		Map<String, Object> values = result.as(new ParameterizedTypeReference<>() {});

		assertThat(values).containsOnly(entry("$.name", "Rand"), entry("$.nope", null));
	}

	@Test
	void createWithMissingResponsePath() {

		String reply = "{\"$.name\":[\"Rand\"]}";

		assertThatIllegalArgumentException()
				.isThrownBy(() -> newResult(reply, new RequestedPath("$.name", "$.name"), new RequestedPath("$[[[", "$[[[")))
				.withMessageContaining("Redis did not return path '$[[['");
	}

	private DefaultJsonPathResult newResult(String reply, RequestedPath... paths) {
		return new DefaultJsonPathResult(serializer, List.of(paths), reply.getBytes(StandardCharsets.UTF_8));
	}

}
