/*
 * Copyright 2015 the original author or authors.
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
import java.util.List;

import org.hamcrest.core.Is;
import org.hamcrest.core.IsNull;
import org.junit.Before;
import org.junit.Test;
import org.springframework.data.redis.AddressObjectFactory;
import org.springframework.data.redis.ObjectFactory;
import org.springframework.data.redis.Person;
import org.springframework.data.redis.PersonObjectFactory;

/**
 * Tests for {@link GenericJackson2JsonRedisSerializer}.
 * 
 * @author Thomas Darimont
 */
public class GenericJackson2JsonRedisSerializerTests {

	private GenericJackson2JsonRedisSerializer serializer;

	private List<ObjectFactory<? extends Object>> objectFactories;

	@Before
	@SuppressWarnings("unchecked")
	public void setUp() {

		this.serializer = new GenericJackson2JsonRedisSerializer();
		this.objectFactories = Arrays.<ObjectFactory<? extends Object>> asList(new PersonObjectFactory(),
				new AddressObjectFactory());
	}

	/**
	 * @see DATAREDIS-390
	 */
	@Test
	public void beAbleToSerializeMultipleTypes() throws Exception {

		for (ObjectFactory<? extends Object> factory : objectFactories) {
			Object instance = factory.instance();
			assertEquals(instance, serializer.deserialize(serializer.serialize(instance)));
		}
	}

	/**
	 * @see DATAREDIS-390
	 */
	@Test
	public void testJackson2JsonSerializerShouldReturnEmptyByteArrayWhenSerializingNull() {
		assertThat(serializer.serialize(null), Is.is(new byte[0]));
	}

	/**
	 * @see DATAREDIS-390
	 */
	@Test
	public void testJackson2JsonSerializerShouldReturnNullWhenDerserializingEmtyByteArray() {
		assertThat(serializer.deserialize(new byte[0]), IsNull.nullValue());
	}

	/**
	 * @see DATAREDIS-390
	 */
	@Test(expected = SerializationException.class)
	public void testJackson2JsonSerilizerShouldThrowExceptionWhenDeserializingInvalidByteArray() {

		Person person = new PersonObjectFactory().instance();
		byte[] serializedValue = serializer.serialize(person);
		Arrays.sort(serializedValue); // corrupt serialization result

		serializer.deserialize(serializedValue);
	}

	/**
	 * @see DATAREDIS-390
	 */
	@Test(expected = IllegalArgumentException.class)
	public void testJackson2JsonSerilizerThrowsExceptionWhenSettingNullObjectMapper() {
		serializer.setObjectMapper(null);
	}
}
