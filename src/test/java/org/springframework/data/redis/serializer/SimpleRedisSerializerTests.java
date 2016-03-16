/*
 * Copyright 2011-2016 the original author or authors.
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

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.URL;
import java.util.UUID;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.data.redis.Address;
import org.springframework.data.redis.Person;
import org.springframework.oxm.xstream.XStreamMarshaller;
import org.springframework.util.StreamUtils;

/**
 * @author Jennifer Hickey
 * @author Mark Paluch
 */
public class SimpleRedisSerializerTests {

	private static class A implements Serializable {

		private Integer value = Integer.valueOf(30);

		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((value == null) ? 0 : value.hashCode());
			return result;
		}

		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			A other = (A) obj;
			if (value == null) {
				if (other.value != null)
					return false;
			} else if (!value.equals(other.value))
				return false;
			return true;
		}
	}

	private static class B implements Serializable {

		private String name = getClass().getName();
		private A a = new A();

		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((a == null) ? 0 : a.hashCode());
			result = prime * result + ((name == null) ? 0 : name.hashCode());
			return result;
		}

		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			B other = (B) obj;
			if (a == null) {
				if (other.a != null)
					return false;
			} else if (!a.equals(other.a))
				return false;
			if (name == null) {
				if (other.name != null)
					return false;
			} else if (!name.equals(other.name))
				return false;
			return true;
		}
	}

	private RedisSerializer serializer;

	@Before
	public void setUp() {
		serializer = new JdkSerializationRedisSerializer();
	}

	@After
	public void tearDown() {
		serializer = null;
	}

	@Test
	public void testBasicSerializationRoundtrip() throws Exception {
		verifySerializedObjects(new Integer(300), new Double(200), new B());
	}

	private void verifySerializedObjects(Object... objects) {
		for (Object object : objects) {
			assertEquals("Incorrectly (de)serialized object " + object, object,
					serializer.deserialize(serializer.serialize(object)));
		}
	}

	@Test
	public void jdkSerializerShouldUseCustomClassLoader() throws ClassNotFoundException {

		ClassLoader customClassLoader = new CustomClassLoader();

		JdkSerializationRedisSerializer serializer = new JdkSerializationRedisSerializer(customClassLoader);
		SerializableDomainClass domainClass = new SerializableDomainClass();

		byte[] serialized = serializer.serialize(domainClass);
		Object deserialized = serializer.deserialize(serialized);

		assertThat(deserialized.getClass().getName(), is(equalTo(SerializableDomainClass.class.getName())));
		assertThat(deserialized, is(not(instanceOf(SerializableDomainClass.class))));
		assertThat(deserialized.getClass().getClassLoader(), is(equalTo(customClassLoader)));
	}

	@Test
	public void testStringEncodedSerialization() {
		String value = UUID.randomUUID().toString();
		assertEquals(value, serializer.deserialize(serializer.serialize(value)));
		assertEquals(value, serializer.deserialize(serializer.serialize(value)));
		assertEquals(value, serializer.deserialize(serializer.serialize(value)));
	}

	@Test
	public void testPersonSerialization() throws Exception {
		String value = UUID.randomUUID().toString();
		Person p1 = new Person(value, value, 1, new Address(value, 2));
		assertEquals(p1, serializer.deserialize(serializer.serialize(p1)));
		assertEquals(p1, serializer.deserialize(serializer.serialize(p1)));
	}

	@Test
	public void testOxmSerializer() throws Exception {
		XStreamMarshaller xstream = new XStreamMarshaller();
		xstream.afterPropertiesSet();

		OxmSerializer serializer = new OxmSerializer(xstream, xstream);

		String value = UUID.randomUUID().toString();
		Person p1 = new Person(value, value, 1, new Address(value, 2));
		assertEquals(p1, serializer.deserialize(serializer.serialize(p1)));
		assertEquals(p1, serializer.deserialize(serializer.serialize(p1)));
	}

	@Test
	public void testJsonSerializer() throws Exception {
		JacksonJsonRedisSerializer<Person> serializer = new JacksonJsonRedisSerializer<Person>(Person.class);
		String value = UUID.randomUUID().toString();
		Person p1 = new Person(value, value, 1, new Address(value, 2));
		assertEquals(p1, serializer.deserialize(serializer.serialize(p1)));
		assertEquals(p1, serializer.deserialize(serializer.serialize(p1)));
	}

	/**
	 * Custom class loader that loads class files from the test's class path. This {@link ClassLoader} does not delegate
	 * to a parent class loader to truly load classes that are defined by this class loader and not interfere with any
	 * parent class loader. The class loader uses simple class definition which is fine for the test but do not use this
	 * as sample for production class loaders.
	 */
	private static class CustomClassLoader extends ClassLoader {

		public CustomClassLoader() {
			super(null);
		}

		@Override
		protected Class<?> findClass(String name) throws ClassNotFoundException {

			URL resource = SimpleRedisSerializerTests.class.getResource("/" + name.replace('.', '/') + ".class");

			InputStream is = null;
			try {

				is = resource.openStream();
				byte[] bytes = StreamUtils.copyToByteArray(is);
				return defineClass(name, bytes, 0, bytes.length);
			} catch (IOException o_O) {
				throw new ClassNotFoundException("Cannot read class file", o_O);
			} finally {

				if (is != null) {
					try {
						is.close();
					} catch (IOException e) {
						// ignore
					}
				}
			}

		}

	}
}
