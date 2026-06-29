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
package org.springframework.data.redis.serializer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.util.List;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import org.springframework.core.ParameterizedTypeReference;
import org.springframework.data.redis.Person;
import org.springframework.data.redis.PersonObjectFactory;

/**
 * Unit tests for {@link JacksonRedisJsonSerializer}.
 *
 * @author Yordan Tsintsov
 * @since 4.2
 */
class JacksonRedisJsonSerializerUnitTests {

	private final JacksonRedisJsonSerializer serializer = new JacksonRedisJsonSerializer(JsonMapper.shared());

	@Test
	void testToJsonReturnsJsonNullLiteralForNull() {
		assertThat(serializer.serialize(null)).isEqualTo("null");
	}

	@Test
	void testToJsonConvertsToJsonFromObject() {

		Person person = new PersonObjectFactory().instance();

		String json = serializer.serialize(person);

		assertThat(serializer.deserialize(json, Person.class)).isEqualTo(person);
	}

	@Test
	void testToJsonWrapsJacksonExceptions() {

		assertThatExceptionOfType(SerializationException.class)
				.isThrownBy(() -> serializer.serialize(new ThrowingPojo()))
				.withMessageStartingWith("Could not write JSON:");
	}

	@Test
	void testFromJsonClassDeserializesPojo() {

		Person person = new PersonObjectFactory().instance();

		assertThat(serializer.deserialize(serializer.serialize(person), Person.class)).isEqualTo(person);
	}

	@Test
	void testFromJsonClassWrapsInvalidJson() {

		assertThatExceptionOfType(SerializationException.class)
				.isThrownBy(() -> serializer.deserialize("{not json", Person.class))
				.withMessageStartingWith("Could not read JSON:");
	}

	@Test
	void testFromJsonTypeRefDeserializesGenericList() {

		List<Long> result = serializer.deserialize("[1,2,3]", new ParameterizedTypeReference<>() {});

		assertThat(result).containsExactly(1L, 2L, 3L);
	}

	@Test
	void testFromJsonTypeRefDeserializesNestedGenericList() {

		List<List<Long>> result = serializer.deserialize("[[1,2,3,4,5,6]]",
				new ParameterizedTypeReference<>() {});

		assertThat(result).containsExactly(List.of(1L, 2L, 3L, 4L, 5L, 6L));
	}

	@Test
	void testFromJsonTypeRefWrapsInvalidJson() {

		assertThatExceptionOfType(SerializationException.class)
				.isThrownBy(() -> serializer.deserialize("{not json", new ParameterizedTypeReference<List<Long>>() {}))
				.withMessageStartingWith("Could not read JSON:");
	}

	static class ThrowingPojo {

		public String getValue() {
			throw new IllegalStateException("boom");
		}
	}

}
