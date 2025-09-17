/*
 * Copyright 2014-2025 the original author or authors.
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

import static org.assertj.core.api.Assertions.*;

import java.util.Arrays;

import org.junit.jupiter.api.Test;

import org.springframework.data.redis.Person;
import org.springframework.data.redis.PersonObjectFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;

/**
 * Unit tests for {@link Jackson2JsonRedisSerializer}.
 *
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @author Mark Paluch
 */
class Jackson2JsonRedisSerializerTests {

	private Jackson2JsonRedisSerializer<Person> serializer = new Jackson2JsonRedisSerializer<>(Person.class);

	@Test // DATAREDIS-241
	void testJackson2JsonSerializer() throws Exception {

		Person person = new PersonObjectFactory().instance();
		assertThat(serializer.deserialize(serializer.serialize(person))).isEqualTo(person);
	}

	@Test // DATAREDIS-241
	void testJackson2JsonSerializerShouldReturnEmptyByteArrayWhenSerializingNull() {
		assertThat(serializer.serialize(null)).isEqualTo(new byte[0]);
	}

	@Test // DTATREDIS-241
	void testJackson2JsonSerializerShouldReturnNullWhenDerserializingEmtyByteArray() {
		assertThat(serializer.deserialize(new byte[0])).isNull();
	}

	@Test // DTATREDIS-241
	void testJackson2JsonSerilizerShouldThrowExceptionWhenDeserializingInvalidByteArray() {

		Person person = new PersonObjectFactory().instance();
		byte[] serializedValue = serializer.serialize(person);
		Arrays.sort(serializedValue); // corrupt serialization result

		assertThatExceptionOfType(SerializationException.class).isThrownBy(() -> serializer.deserialize(serializedValue));
	}

	@Test // DTATREDIS-241
	void testJackson2JsonSerilizerThrowsExceptionWhenSettingNullObjectMapper() {
		assertThatIllegalArgumentException().isThrownBy(() -> serializer.setObjectMapper(null));
	}

	@Test // GH-2322
	void shouldConsiderWriter() {

		serializer = new Jackson2JsonRedisSerializer<>(new ObjectMapper(),
				TypeFactory.defaultInstance().constructType(Person.class), Jackson2ObjectReader.create(),
				(mapper, source) -> "foo".getBytes());
		Person person = new PersonObjectFactory().instance();
		assertThat(serializer.serialize(person)).isEqualTo("foo".getBytes());
	}

}
