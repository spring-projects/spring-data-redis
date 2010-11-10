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
		collection = getCollection();
	}

	abstract AbstractRedisCollection<T> getCollection();

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
		collection.add(t1);
		assertEquals(1, collection.size());
		collection.clear();
		assertEquals(0, collection.size());
	}

	public boolean contains(Object o) {
		return collection.contains(o);
	}

	public boolean containsAll(Collection<?> c) {
		return collection.containsAll(c);
	}

	public boolean equals(Object obj) {
		return collection.equals(obj);
	}

	public String getKey() {
		return collection.getKey();
	}

	public int hashCode() {
		return collection.hashCode();
	}

	public boolean isEmpty() {
		return collection.isEmpty();
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

	public int size() {
		return collection.size();
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