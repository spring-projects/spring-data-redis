/*
 * Copyright 2011 the original author or authors.
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
package org.springframework.data.keyvalue.redis.mapping;

import static org.junit.Assert.*;

import java.util.Map;

import org.junit.Test;
import org.springframework.data.keyvalue.redis.Address;
import org.springframework.data.keyvalue.redis.Person;
import org.springframework.data.keyvalue.redis.mapper.HashMapper;

/**
 * @author Costin Leau
 */
public abstract class AbstractHashMapperTest {
	protected abstract HashMapper mapperFor(Class t);

	private void test(Object o) {
		HashMapper<Object> mapper = mapperFor(o.getClass());
		Map hash = mapper.toHash(o);
		System.out.println("object hash " + hash.size() + " is " + hash);
		assertEquals(o, mapper.fromHash(hash));
	}

	@Test(expected = Exception.class)
	public void testBasicValues() throws Exception {
		test("SomeStrangeString*&#@");
		test(123);
		test(Integer.MAX_VALUE);
		test(Long.MAX_VALUE);
		test(Double.MIN_VALUE);
		test(Float.MIN_VALUE);
		test(Boolean.FALSE);
		test(Thread.State.BLOCKED);
	}

	@Test
	public void testSimpleBean() throws Exception {
		test(new Address("Broadway", 1));
	}

	@Test
	public void testNestedBean() throws Exception {
		test(new Person("George", "Enescu", 74, new Address("liveni", 19)));
	}
}