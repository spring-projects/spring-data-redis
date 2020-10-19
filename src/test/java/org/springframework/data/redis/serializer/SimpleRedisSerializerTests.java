/*
 * Copyright 2011-2020 the original author or authors.
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

import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import org.springframework.data.redis.Address;
import org.springframework.data.redis.Person;
import org.springframework.instrument.classloading.ShadowingClassLoader;
import org.springframework.oxm.xstream.XStreamMarshaller;

/**
 * @author Jennifer Hickey
 * @author Mark Paluch
 * @author Christoph Strobl
 */
class SimpleRedisSerializerTests {

	@EqualsAndHashCode
	private static class A implements Serializable {

		private Integer value = Integer.valueOf(30);
	}

	@EqualsAndHashCode
	private static class B implements Serializable {

		private String name = getClass().getName();
		private A a = new A();
	}

	private RedisSerializer serializer = new JdkSerializationRedisSerializer();

	@Test
	void testBasicSerializationRoundtrip() throws Exception {
		verifySerializedObjects(new Integer(300), new Double(200), new B());
	}

	private void verifySerializedObjects(Object... objects) {
		for (Object object : objects) {
			assertThat(serializer.deserialize(serializer.serialize(object))).as("Incorrectly (de)serialized object " + object)
					.isEqualTo(object);
		}
	}

	@Test // DATAREDIS-427
	void jdkSerializerShouldUseCustomClassLoader() throws ClassNotFoundException {

		ClassLoader customClassLoader = new ShadowingClassLoader(ClassLoader.getSystemClassLoader());

		JdkSerializationRedisSerializer serializer = new JdkSerializationRedisSerializer(customClassLoader);
		SerializableDomainClass domainClass = new SerializableDomainClass();

		byte[] serialized = serializer.serialize(domainClass);
		Object deserialized = serializer.deserialize(serialized);

		assertThat(deserialized.getClass().getName()).isEqualTo(SerializableDomainClass.class.getName());
		assertThat(deserialized).isNotInstanceOf(SerializableDomainClass.class);
		assertThat(deserialized.getClass().getClassLoader()).isEqualTo(customClassLoader);
	}

	@Test
	void testStringEncodedSerialization() {
		String value = UUID.randomUUID().toString();
		assertThat(serializer.deserialize(serializer.serialize(value))).isEqualTo(value);
		assertThat(serializer.deserialize(serializer.serialize(value))).isEqualTo(value);
		assertThat(serializer.deserialize(serializer.serialize(value))).isEqualTo(value);
	}

	@Test
	void testPersonSerialization() throws Exception {
		String value = UUID.randomUUID().toString();
		Person p1 = new Person(value, value, 1, new Address(value, 2));
		assertThat(serializer.deserialize(serializer.serialize(p1))).isEqualTo(p1);
		assertThat(serializer.deserialize(serializer.serialize(p1))).isEqualTo(p1);
	}

	@Test
	void testOxmSerializer() throws Exception {
		XStreamMarshaller xstream = new XStreamMarshaller();
		xstream.afterPropertiesSet();

		OxmSerializer serializer = new OxmSerializer(xstream, xstream);

		String value = UUID.randomUUID().toString();
		Person p1 = new Person(value, value, 1, new Address(value, 2));
		assertThat(serializer.deserialize(serializer.serialize(p1))).isEqualTo(p1);
		assertThat(serializer.deserialize(serializer.serialize(p1))).isEqualTo(p1);
	}

	@Test
	void testJsonSerializer() throws Exception {
		Jackson2JsonRedisSerializer<Person> serializer = new Jackson2JsonRedisSerializer<>(Person.class);
		String value = UUID.randomUUID().toString();
		Person p1 = new Person(value, value, 1, new Address(value, 2));
		assertThat(serializer.deserialize(serializer.serialize(p1))).isEqualTo(p1);
		assertThat(serializer.deserialize(serializer.serialize(p1))).isEqualTo(p1);
	}
}
