/*
 * Copyright 2022-2022 the original author or authors.
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

import com.fasterxml.jackson.core.type.TypeReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.Person;
import org.springframework.data.redis.PersonObjectFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.*;

/**
 * Unit tests for {@link JacksonTypeReference2JsonRedisSerializer}
 *
 * @author Jos Roseboom
 */
class JacksonTypeReference2JsonRedisSerializerTests {

	private JacksonTypeReference2JsonRedisSerializer<Person> personSerializer;

	@BeforeEach public void setup() {
		personSerializer = new JacksonTypeReference2JsonRedisSerializer<>(new TypeReference<>() {
		});
	}

	@Test // GH-2374: should behave like required for Jackson2JsonRedisSerializer in DATAREDIS-241
	void testJackson2JsonSerializer() {
		Person person = new PersonObjectFactory().instance();
		assertThat(personSerializer.deserialize(personSerializer.serialize(person))).isEqualTo(person);
	}

	@Test // GH-2374: should behave like required for Jackson2JsonRedisSerializer in DATAREDIS-241
	void testJackson2JsonSerializerShouldReturnEmptyByteArrayWhenSerializingNull() {
		assertThat(personSerializer.serialize(null)).isEqualTo(new byte[0]);
	}

	@Test // GH-2374: should behave like required for Jackson2JsonRedisSerializer in DATAREDIS-241
	void testJackson2JsonSerializerShouldReturnNullWhenDerserializingEmtyByteArray() {
		assertThat(personSerializer.deserialize(new byte[0])).isNull();
	}

	@Test // GH-2374: should behave like required for Jackson2JsonRedisSerializer in DATAREDIS-241
	void testJackson2JsonSerilizerShouldThrowExceptionWhenDeserializingInvalidByteArray() {

		Person person = new PersonObjectFactory().instance();
		byte[] serializedValue = personSerializer.serialize(person);
		Arrays.sort(serializedValue); // corrupt serialization result

		assertThatExceptionOfType(SerializationException.class).isThrownBy(
				() -> personSerializer.deserialize(serializedValue));
	}

	@Test // GH-2374
	void testSetOfPersons() {
		PersonObjectFactory personFactory = new PersonObjectFactory();
		Set<Person> personSet = Set.of(personFactory.instance(), personFactory.instance());
		JacksonTypeReference2JsonRedisSerializer<Set<Person>> personSetSerializer = new JacksonTypeReference2JsonRedisSerializer<>(
				new TypeReference<>() {
				});

		assertThat(personSetSerializer.deserialize(personSetSerializer.serialize(personSet))).isEqualTo(personSet);
	}

	@Test // GH-2374
	void testMultipleLevelNesting() {
		final Map<Integer, Set<List<String>>> nestedGenerics = Map.of(3, Set.of(List.of("MyString")));
		JacksonTypeReference2JsonRedisSerializer<Map<Integer, Set<List<String>>>> personSetSerializer = new JacksonTypeReference2JsonRedisSerializer<>(
				new TypeReference<>() {
				});

		assertThat(personSetSerializer.deserialize(personSetSerializer.serialize(nestedGenerics))).isEqualTo(
				nestedGenerics);
	}

	@Test // GH-2374
	void testNotSerializableGeneric() {
		final Set<NotSerializableClass> notSerializableClasses = Set.of(new NotSerializableClass("John"),
				new NotSerializableClass("Jane"));
		JacksonTypeReference2JsonRedisSerializer<Set<NotSerializableClass>> personSetSerializer = new JacksonTypeReference2JsonRedisSerializer<>(
				new TypeReference<>() {
				});

		assertThat(personSetSerializer.deserialize(personSetSerializer.serialize(notSerializableClasses))).isEqualTo(
				notSerializableClasses);
	}

}
