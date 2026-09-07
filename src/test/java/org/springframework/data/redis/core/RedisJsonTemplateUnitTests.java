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
import static org.mockito.Mockito.*;

import java.util.List;

import org.junit.jupiter.api.Test;

import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.serializer.GenericJacksonJsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

/**
 * Unit tests for {@link RedisJsonTemplate} argument validation, i.e. the checks that reject a call before it reaches
 * Redis and therefore need no server.
 *
 * @author Moritz Halbritter
 */
class RedisJsonTemplateUnitTests {

	private final RedisJsonTemplate<String> template = new RedisJsonTemplate<>(mock(RedisConnectionFactory.class),
			StringRedisSerializer.UTF_8, GenericJacksonJsonRedisSerializer.builder().build());

	@Test
	void pathsRejectsEmptyPaths() {

		assertThatIllegalArgumentException().isThrownBy(() -> template.paths("key", List.of()))
				.withMessageStartingWith("Paths must not be empty");
	}

	@Test
	void pathsRejectsDuplicatePaths() {

		assertThatIllegalArgumentException().isThrownBy(() -> template.paths("key", "name", "name"))
				.withMessageStartingWith("Duplicate paths are not supported");
		assertThatIllegalArgumentException().isThrownBy(() -> template.paths("key", "$.name", "$.name"))
				.withMessageStartingWith("Duplicate paths are not supported");
	}

	@Test
	void pathsRejectsMixingPropertyNamesAndJsonPathExpressions() {

		assertThatIllegalArgumentException().isThrownBy(() -> template.paths("key", "name", "$.age"))
				.withMessage("Mixing bare property names and JSONPath expressions is not supported");
	}

	@Test
	void pathsNeverTreatsQuoteOrBracketBearingNamesAsBareProperties() {

		// A bare property name is spliced into $['...'] unescaped, so it must never be able to close the quoting and
		// turn the caller's string into JSONPath syntax. Pairing each candidate with a real bare name asserts it was
		// classified as a JSONPath expression rather than a property.
		for (String candidate : List.of("a'] ['b", "a'b", "a\\b", "a[0]", "a b", "$.a")) {
			assertThatIllegalArgumentException().isThrownBy(() -> template.paths("key", "plain", candidate))
					.withMessage("Mixing bare property names and JSONPath expressions is not supported");
		}
	}

	@Test
	void pathsTreatsUnderscoresAndLeadingDigitsAsBareProperties() {

		// BARE_PROPERTY_PATH admits these. Pairing each with a JSONPath expression inverts the assertion above: the
		// mixing error is raised only if the candidate was classified as a bare property name.
		for (String candidate : List.of("_name", "a_b", "1st", "42", "a1.b2", "foo-bar")) {
			assertThatIllegalArgumentException().isThrownBy(() -> template.paths("key", "$.plain", candidate))
					.withMessage("Mixing bare property names and JSONPath expressions is not supported");
		}
	}

}
