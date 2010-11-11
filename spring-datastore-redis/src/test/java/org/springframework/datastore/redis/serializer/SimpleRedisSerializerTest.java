/*
 * Copyright 2010 the original author or authors.
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
package org.springframework.datastore.redis.serializer;

import static org.junit.Assert.*;

import java.io.Serializable;
import java.util.UUID;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.datastore.redis.Address;
import org.springframework.datastore.redis.Person;


public class SimpleRedisSerializerTest {

	private static class A implements Serializable {
		private Integer value = Integer.valueOf(30);

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((value == null) ? 0 : value.hashCode());
			return result;
		}

		@Override
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
			}
			else if (!value.equals(other.value))
				return false;
			return true;
		}
	}

	private static class B implements Serializable {
		private String name = getClass().getName();
		private A a = new A();

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((a == null) ? 0 : a.hashCode());
			result = prime * result + ((name == null) ? 0 : name.hashCode());
			return result;
		}

		@Override
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
			}
			else if (!a.equals(other.a))
				return false;
			if (name == null) {
				if (other.name != null)
					return false;
			}
			else if (!name.equals(other.name))
				return false;
			return true;
		}
	}

	private RedisSerializer serializer;

	@Before
	public void setUp() {
		serializer = new SimpleRedisSerializer();
	}

	@After
	public void tearDown() {
		serializer = null;
	}

	@Test
	public void testBasicSerializationRoundtrip() throws Exception {
		Integer integer = new Integer(300);
		verifySerializedObjects(new Integer(300), new Double(200), new B());
	}

	private void verifySerializedObjects(Object... objects) {
		for (Object object : objects) {
			assertEquals("Incorrectly (de)serialized object " + object, object,
					serializer.deserialize(serializer.serialize(object)));
		}
	}

	@Test
	public void testStringEncodedSerialization() {
		String value = UUID.randomUUID().toString();
		assertEquals(value, serializer.deserialize(serializer.serializeAsString(value)));
		assertEquals(value, serializer.deserialize(serializer.serializeAsString(value)));
		assertEquals(value, serializer.deserialize(serializer.serializeAsString(value)));
	}

	@Test
	public void testPersonSerialization() throws Exception {
		String value = UUID.randomUUID().toString();
		Person p1 = new Person(value, value, 1, new Address(value, 2));
		assertEquals(p1, serializer.deserialize(serializer.serialize(p1)));
		assertEquals(p1, serializer.deserialize(serializer.serializeAsString(p1)));
	}
}