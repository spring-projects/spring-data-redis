/*
 * Copyright 2014-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.redis.serializer;

import static org.junit.Assert.*;

import java.util.Arrays;

import org.hamcrest.core.Is;
import org.hamcrest.core.IsNull;
import org.junit.Before;
import org.junit.Test;
import org.springframework.data.redis.Person;
import org.springframework.data.redis.PersonObjectFactory;

/**
 * @author Thomas Darimont
 * @author Christoph Strobl
 */
public class Jackson2JsonRedisSerializerTests {

	private Jackson2JsonRedisSerializer<Person> serializer;

	@Before
	public void setUp() {
		this.serializer = new Jackson2JsonRedisSerializer<>(Person.class);
	}

	@Test // DATAREDIS-241
	public void testJackson2JsonSerializer() throws Exception {

		Person person = new PersonObjectFactory().instance();
		assertEquals(person, serializer.deserialize(serializer.serialize(person)));
	}

	@Test // DATAREDIS-241
	public void testJackson2JsonSerializerShouldReturnEmptyByteArrayWhenSerializingNull() {
		assertThat(serializer.serialize(null), Is.is(new byte[0]));
	}

	@Test // DTATREDIS-241
	public void testJackson2JsonSerializerShouldReturnNullWhenDerserializingEmtyByteArray() {
		assertThat(serializer.deserialize(new byte[0]), IsNull.nullValue());
	}

	@Test(expected = SerializationException.class) // DTATREDIS-241
	public void testJackson2JsonSerilizerShouldThrowExceptionWhenDeserializingInvalidByteArray() {

		Person person = new PersonObjectFactory().instance();
		byte[] serializedValue = serializer.serialize(person);
		Arrays.sort(serializedValue); // corrupt serialization result

		serializer.deserialize(serializedValue);
	}

	@Test(expected = IllegalArgumentException.class) // DTATREDIS-241
	public void testJackson2JsonSerilizerThrowsExceptionWhenSettingNullObjectMapper() {
		serializer.setObjectMapper(null);
	}

}
