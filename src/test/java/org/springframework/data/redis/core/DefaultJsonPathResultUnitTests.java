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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import org.springframework.data.redis.core.RedisJsonTemplate.DefaultJsonPathResult;
import org.springframework.data.redis.core.RedisJsonTemplate.RequestedPath;
import org.springframework.data.redis.serializer.GenericJacksonJsonRedisSerializer;
import org.springframework.data.redis.serializer.RedisJsonSerializer;
import org.springframework.data.redis.serializer.SerializationException;

/**
 * Unit tests for {@link DefaultJsonPathResult}.
 *
 * @author Moritz Halbritter
 */
class DefaultJsonPathResultUnitTests {

	private final RedisJsonSerializer serializer = GenericJacksonJsonRedisSerializer.builder().build();

	private static DefaultJsonPathResult newResult(RedisJsonSerializer serializer, String reply,
			String... requestedAndSent) {

		List<RequestedPath> requestedPaths = new ArrayList<>();
		for (int i = 0; i < requestedAndSent.length; i += 2) {
			requestedPaths.add(new RequestedPath(requestedAndSent[i], requestedAndSent[i + 1]));
		}

		return new DefaultJsonPathResult(serializer, requestedPaths, reply.getBytes(StandardCharsets.UTF_8));
	}

	/**
	 * A member name requiring full JSON unescaping (e.g. {@code $['a"b']}) has to resolve, and must not force a
	 * deserialize/re-serialize round-trip of the whole reply, which would lose precision on any other path's numbers
	 * in that same reply.
	 */
	@Test
	void resolvesAnEscapedMemberNameWithoutLosingNumberPrecision() {

		String reply = "{\"$['a\\\"b']\":[\"value\"],\"$.c\":[3.14159265358979323846]}";

		DefaultJsonPathResult result = newResult(serializer, reply, "a\"b", "$['a\"b']", "c", "$.c");

		assertThat(result.path("a\"b").asString()).isEqualTo("[\"value\"]");
		assertThat(result.path("c").asString()).isEqualTo("[3.14159265358979323846]");
	}

	@Test
	@SuppressWarnings("unchecked")
	void absentKeyYieldsNoValueForEveryAccessor() {

		DefaultJsonPathResult result = new DefaultJsonPathResult(serializer,
				List.of(new RequestedPath("name", "$['name']")), null);

		assertThat(result.exists()).isFalse();
		assertThat(result.asBytes()).isNull();
		assertThat(result.asString()).isNull();
		assertThat(result.as(Map.class)).isNull();
		assertThat((Object) result.map(bytes -> new Object())).isNull();
		assertThat(result.path("name").exists()).isFalse();

		// an absent key has no members to validate against, so path(...) still has to reject from the requested paths
		assertThatIllegalArgumentException().isThrownBy(() -> result.path("age"))
				.withMessage("Path 'age' was not requested");
	}

	@Test
	void pathRejectsAnUnrequestedPath() {

		DefaultJsonPathResult result = newResult(serializer, "[1]", "name", "$['name']");

		assertThatIllegalArgumentException().isThrownBy(() -> result.path("age"))
				.withMessage("Path 'age' was not requested");
	}

	@Test
	void asRejectsAPathThatMatchedMoreThanOnce() {

		String reply = "{\"$.name\":[\"Rand\"],\"$..city\":[\"Emond's Field\",\"Caemlyn\"]}";

		DefaultJsonPathResult result = newResult(serializer, reply, "$.name", "$.name", "$..city", "$..city");

		assertThatExceptionOfType(SerializationException.class).isThrownBy(() -> result.as(Map.class))
				.withMessageContaining("'$..city' matched more than once");

		// the multi-match path is still readable through path(...)
		assertThat(result.path("$..city").matches().as(String.class)).containsExactly("Emond's Field", "Caemlyn");
	}

	@Test
	@SuppressWarnings({"rawtype", "unchecked"})
	void flattensAPathThatMatchedNothingToNull() {

		String reply = "{\"$.name\":[\"Rand\"],\"$.nope\":[]}";

		DefaultJsonPathResult result = newResult(serializer, reply, "$.name", "$.name", "$.nope", "$.nope");

		assertThat(result.as(Map.class)).containsEntry("$.name", "Rand").containsEntry("$.nope", null);
	}

	@Test
	void rejectsAPathRedisDidNotReturn() {

		String reply = "{\"$.name\":[\"Rand\"]}";

		assertThatIllegalArgumentException()
				.isThrownBy(() -> newResult(serializer, reply, "$.name", "$.name", "$[[[", "$[[["))
				.withMessageContaining("Redis did not return path '$[[['");
	}

}
