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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.type.TypeFactory;

import java.io.Serializable;
import java.util.Arrays;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.springframework.data.redis.Person;
import org.springframework.data.redis.PersonObjectFactory;

/**
 * Unit tests for {@link JacksonJsonRedisSerializer}.
 *
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Chris Bono
 */
class JacksonJsonRedisSerializerTests {

	private JacksonJsonRedisSerializer<Person> serializer = new JacksonJsonRedisSerializer<>(Person.class);

	@Test // DATAREDIS-241
	void testJacksonJsonSerializerShouldReturnEmptyByteArrayWhenSerializingNull() {
		assertThat(serializer.serialize(null)).isEqualTo(new byte[0]);
	}

	@Test // DTATREDIS-241
	void testJacksonJsonSerializerShouldReturnNullWhenDerserializingEmptyByteArray() {
		assertThat(serializer.deserialize(new byte[0])).isNull();
	}

	@Test // DTATREDIS-241
	void testJacksonJsonSerializerShouldThrowExceptionWhenDeserializingInvalidByteArray() {

		Person person = new PersonObjectFactory().instance();
		byte[] serializedValue = serializer.serialize(person);
		Arrays.sort(serializedValue); // corrupt serialization result

		assertThatExceptionOfType(SerializationException.class).isThrownBy(() -> serializer.deserialize(serializedValue));
	}

	@Test // GH-2322
	void shouldConsiderWriter() {

		serializer = new JacksonJsonRedisSerializer<>(new JsonMapper(),
				TypeFactory.createDefaultInstance().constructType(Person.class), JacksonObjectReader.create(),
				(mapper, source) -> "foo".getBytes());
		Person person = new PersonObjectFactory().instance();
		assertThat(serializer.serialize(person)).isEqualTo("foo".getBytes());
	}

	@Test // GH-3271
	void canDeserializeSerializedStandardPojo() {

		// Where "Standard" is POJO w/ private fields w/ getters/setters

		Person person = new PersonObjectFactory().instance();
		byte[] serializedPerson = JsonMapper.shared().writeValueAsString(person).getBytes();

		Person deserializedPerson = serializer.deserialize(serializedPerson);

		// The bug was that the fields would be null upon deserialization
		assertThat(deserializedPerson).isEqualTo(person);
	}

	@Test // GH-3271
	void canDeserializeSerializedPropertyPojo() {

		// Where "Property" is POJO w/ public fields and no getters/setters

		JacksonJsonRedisSerializer<SessionTokenPropertyPojo> redisSerializer = new JacksonJsonRedisSerializer<>(JsonMapper.shared(), SessionTokenPropertyPojo.class);

		SessionTokenPropertyPojo sessionToken = new SessionTokenPropertyPojo(UUID.randomUUID().toString());
		byte[] serializedSessionToken = redisSerializer.serialize(sessionToken);//JsonMapper.shared().writeValueAsString(sessionToken).getBytes();

		SessionTokenPropertyPojo deserializedSessionToken = redisSerializer.deserialize(serializedSessionToken);

		// The bug was that the fields would be null upon deserialization
		assertThat(deserializedSessionToken.userUuid).isEqualTo(sessionToken.userUuid);
	}

	static class SessionTokenPropertyPojo implements Serializable {

		public String userUuid;

		private SessionTokenPropertyPojo() {
		}

		SessionTokenPropertyPojo(String userUuid) {
			this();
			this.userUuid = userUuid;
		}
	}

}
