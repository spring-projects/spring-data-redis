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
import java.util.Objects;
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
 */
class JacksonJsonRedisSerializerTests {

	private JacksonJsonRedisSerializer<Person> serializer = new JacksonJsonRedisSerializer<>(Person.class);

	@Test // GH-3271
	void canDeserializeSerialized() {

		JacksonJsonRedisSerializer<SessionToken> redisSerializer = new JacksonJsonRedisSerializer<>(SessionToken.class);

		SessionToken source = new SessionToken(UUID.randomUUID().toString());

		byte[] serialized = redisSerializer.serialize(source);
		assertThat(redisSerializer.deserialize(serialized)).isEqualTo(source);
	}

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

	static class SessionToken implements Serializable {

		private String userUuid;

		private SessionToken() {
			// why jackson?
		}

		public SessionToken(String userUuid) {
			this.userUuid = userUuid;
		}

		public String getUserUuid() {
			return userUuid;
		}

		public void setUserUuid(String userUuid) {
			this.userUuid = userUuid;
		}

		@Override
		public boolean equals(Object o) {
			if (o == this) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			SessionToken token = (SessionToken) o;
			return Objects.equals(userUuid, token.userUuid);
		}

		@Override
		public int hashCode() {
			return Objects.hash(userUuid);
		}

		@Override
		public String toString() {
			return "SessionToken{" + "userUuid='" + userUuid + '\'' + '}';
		}

	}

}
