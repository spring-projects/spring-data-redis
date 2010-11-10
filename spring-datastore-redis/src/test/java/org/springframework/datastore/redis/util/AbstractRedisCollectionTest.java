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
package org.springframework.datastore.redis.util;


import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import static org.junit.matchers.JUnitMatchers.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;


/**
 * Base test for Redis collections.
 *  
 * @author Costin Leau
 */
public abstract class AbstractRedisCollectionTest<T> {

	private AbstractRedisCollection<T> collection;

	@Before
	public void setUp() throws Exception {
		collection = createCollection();
	}

	abstract AbstractRedisCollection<T> createCollection();

	abstract void destroyCollection();

	abstract RedisStore copyStore(RedisStore store);


	/**
	 * Return a new instance of T
	 * @return
	 */
	abstract T getT();

	@After
	public void tearDown() throws Exception {
		// remove the collection entirely since clear() doesn't always work
		collection.getCommands().del(collection.getKey());
		//collection.clear();
		destroyCollection();
	}

	@Test
	public void testAdd() {
		T t1 = getT();
		assertThat(collection.add(t1), is(Boolean.TRUE));
		assertThat(collection, hasItem(t1));
		assertEquals(1, collection.size());
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testAddAll() {
		T t1 = getT();
		T t2 = getT();
		T t3 = getT();

		List<T> list = Arrays.asList(t1, t2, t3);

		assertThat(collection.addAll(list), is(Boolean.TRUE));
		assertThat(collection, hasItem(t1));
		assertThat(collection, hasItem(t2));
		assertThat(collection, hasItem(t3));
		assertEquals(collection.size(), 3);
	}

	@Test
	public void testClear() {
		T t1 = getT();
		assertEquals(0, collection.size());
		collection.add(t1);
		assertEquals(1, collection.size());
		collection.clear();
		assertEquals(0, collection.size());
	}

	@Test
	public void containsObject() {
		T t1 = getT();
		assertThat(collection, not(hasItem(t1)));
		assertThat(collection.add(t1), is(Boolean.TRUE));
		assertThat(collection, hasItem(t1));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void containsAll() {
		T t1 = getT();
		T t2 = getT();
		T t3 = getT();

		List<T> list = Arrays.asList(t1, t2, t3);

		assertThat(collection.addAll(list), is(Boolean.TRUE));
		assertThat(collection.containsAll(list), is(Boolean.TRUE));
		assertThat(collection, hasItems(t1, t2, t3));
	}

	@Test
	public void testEquals() {
		assertEquals(collection, copyStore(collection));
	}

	@Test
	public void testHashCode() {
		assertThat(collection.hashCode(), not(equalTo(collection.getKey().hashCode())));
	}

	@Test
	public void testIsEmpty() {
		assertEquals(0, collection.size());
		assertTrue(collection.isEmpty());
		collection.add(getT());
		assertEquals(1, collection.size());
		assertFalse(collection.isEmpty());
		collection.clear();
		assertTrue(collection.isEmpty());
	}

	public Iterator<T> iterator() {
		return collection.iterator();
	}

	public boolean remove(Object o) {
		return collection.remove(o);
	}

	public boolean removeAll(Collection<?> c) {
		return collection.removeAll(c);
	}

	public boolean retainAll(Collection<?> c) {
		return collection.retainAll(c);
	}

	@Test
	public void testSize() {
		assertEquals(0, collection.size());
		assertTrue(collection.isEmpty());
		collection.add(getT());
		assertEquals(1, collection.size());
		collection.add(getT());
		collection.add(getT());
		assertEquals(2, collection.size());
	}

	public Object[] toArray() {
		return collection.toArray();
	}

	public <T> T[] toArray(T[] a) {
		return collection.toArray(a);
	}

	public String toString() {
		return collection.toString();
	}
}