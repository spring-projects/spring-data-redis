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


import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.matchers.JUnitMatchers.hasItem;
import static org.junit.matchers.JUnitMatchers.hasItems;

import java.util.Arrays;
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

	protected AbstractRedisCollection<T> collection;

	@Before
	public void setUp() throws Exception {
		collection = createCollection();
	}

	abstract AbstractRedisCollection<T> createCollection();

	abstract void destroyCollection();

	abstract RedisStore<T> copyStore(RedisStore<T> store);


	/**
	 * Return a new instance of T
	 * @return
	 */
	abstract T getT();

	@After
	public void tearDown() throws Exception {
		// remove the collection entirely since clear() doesn't always work
		collection.getOperations().delete(collection.getKey());
		destroyCollection();
	}

	@Test
	public void testAdd() {
		T t1 = getT();
		assertThat(collection.add(t1), is(true));
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

		assertThat(collection.addAll(list), is(true));
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
		assertThat(collection.add(t1), is(true));
		assertThat(collection, hasItem(t1));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void containsAll() {
		T t1 = getT();
		T t2 = getT();
		T t3 = getT();

		List<T> list = Arrays.asList(t1, t2, t3);

		assertThat(collection.addAll(list), is(true));
		assertThat(collection.containsAll(list), is(true));
		assertThat(collection, hasItems(t1, t2, t3));
	}

	@Test
	public void testEquals() {
		//assertEquals(collection, copyStore(collection));
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

	@Test
	public void testIterator() {
		T t1 = getT();
		T t2 = getT();
		T t3 = getT();

		List<T> list = Arrays.asList(t1, t2, t3);

		assertThat(collection.addAll(list), is(true));
		Iterator<T> iterator = collection.iterator();

		assertEquals(t1, iterator.next());
		assertEquals(t2, iterator.next());
		assertEquals(t3, iterator.next());
		assertFalse(iterator.hasNext());
	}

	@Test
	public void testRemoveObject() {
		T t1 = getT();
		T t2 = getT();
		T t3 = getT();

		assertEquals(0, collection.size());
		assertThat(collection.add(t1), is(true));
		assertThat(collection.add(t2), is(true));
		assertEquals(2, collection.size());
		assertThat(collection.remove(t3), is(false));
		assertThat(collection.remove(t2), is(true));
		assertThat(collection.remove(t2), is(false));
		assertEquals(1, collection.size());
		assertThat(collection.remove(t1), is(true));
		assertEquals(0, collection.size());
	}

	@Test
	public void removeAll() {
		T t1 = getT();
		T t2 = getT();
		T t3 = getT();

		List<T> list = Arrays.asList(t1, t2, t3);

		assertThat(collection.addAll(list), is(true));
		assertThat(collection.containsAll(list), is(true));
		assertThat(collection, hasItems(t1, t2, t3));

		List<T> newList = Arrays.asList(getT(), getT());
		List<T> partialList = Arrays.asList(getT(), t1, getT());

		assertThat(collection.removeAll(newList), is(false));
		assertThat(collection.removeAll(partialList), is(true));
		assertThat(collection, not(hasItem(t1)));
		assertThat(collection, hasItems(t2, t3));
		assertThat(collection.removeAll(list), is(true));
		assertThat(collection, not(hasItems(t2, t3)));
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testRetainAll() {
		T t1 = getT();
		T t2 = getT();
		T t3 = getT();

		List<T> list = Arrays.asList(t1, t2);
		List<T> newList = Arrays.asList(t2, t3);

		assertThat(collection.addAll(list), is(true));
		assertThat(collection, hasItems(t1, t2));
		assertThat(collection.retainAll(newList), is(true));
		assertThat(collection, not(hasItem(t1)));
		assertThat(collection, hasItem(t2));
	}

	@Test
	public void testSize() {
		assertEquals(0, collection.size());
		assertTrue(collection.isEmpty());
		collection.add(getT());
		assertEquals(1, collection.size());
		collection.add(getT());
		collection.add(getT());
		assertEquals(3, collection.size());
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testToArray() {
		Object[] expectedArray = new Object[] { getT(), getT(), getT() };
		List<T> list = (List<T>) Arrays.asList(expectedArray);

		assertThat(collection.addAll(list), is(true));
		
		Object[] array = collection.toArray();
		assertArrayEquals(expectedArray, array);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testToArrayWithGenerics() {
		Object[] expectedArray = new Object[] { getT(), getT(), getT() };
		List<T> list = (List<T>) Arrays.asList(expectedArray);

		assertThat(collection.addAll(list), is(true));

		Object[] array = collection.toArray(new Object[expectedArray.length]);
		assertArrayEquals(expectedArray, array);
	}

	@Test
	public void testToString() {
		String name = collection.toString();
		collection.add(getT());
		assertEquals(name, collection.toString());
	}
}