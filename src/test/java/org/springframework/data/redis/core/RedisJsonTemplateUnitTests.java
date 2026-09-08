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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.serializer.GenericJacksonJsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

/**
 * Unit tests for {@link RedisJsonTemplate}.
 *
 * @author Moritz Halbritter
 */
class RedisJsonTemplateUnitTests {

	RedisJsonTemplate<String> template = new RedisJsonTemplate<>(mock(RedisConnectionFactory.class),
			StringRedisSerializer.UTF_8, GenericJacksonJsonRedisSerializer.builder().build());

	@Test // GH-3433
	void pathsWithEmptyPaths() {

		assertThatIllegalArgumentException().isThrownBy(() -> template.paths("key", List.of()))
				.withMessageStartingWith("Paths must not be empty");
	}

	@ParameterizedTest // GH-3433
	@ValueSource(strings = { "name", "$.name" })
	void pathsWithDuplicatePaths(String path) {

		assertThatIllegalArgumentException().isThrownBy(() -> template.paths("key", path, path))
				.withMessageStartingWith("Duplicate paths are not supported");
	}

	@Test // GH-3433
	void pathsWithMixedPathForms() {

		assertThatIllegalArgumentException().isThrownBy(() -> template.paths("key", "name", "$.age"))
				.withMessage("Mixing bare property names and JSONPath expressions is not supported");
	}

	@ParameterizedTest // GH-3433
	@ValueSource(strings = { "a'] ['b", "a'b", "a\\b", "a[0]", "a b", "$.a" })
	void pathsWithNonPropertySyntax(String path) {

		// Pair with a property path to verify that this input is classified differently.
		assertThatIllegalArgumentException().isThrownBy(() -> template.paths("key", "plain", path))
				.withMessage("Mixing bare property names and JSONPath expressions is not supported");
	}

	@ParameterizedTest // GH-3433
	@ValueSource(strings = { "_name", "a_b", "1st", "42", "a1.b2", "foo-bar" })
	void pathsWithPropertyPathSyntax(String propertyPath) {

		// Pair with a JSONPath expression to verify that this input is classified as a property path.
		assertThatIllegalArgumentException().isThrownBy(() -> template.paths("key", "$.plain", propertyPath))
				.withMessage("Mixing bare property names and JSONPath expressions is not supported");
	}

}
