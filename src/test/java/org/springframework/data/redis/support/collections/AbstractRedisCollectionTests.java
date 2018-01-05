/*
 * Copyright 2011-2018 the original author or authors.
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
package org.springframework.data.redis.support.collections;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import static org.springframework.data.redis.matcher.RedisTestMatchers.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.springframework.data.redis.ConnectionFactoryTracker;
import org.springframework.data.redis.ObjectFactory;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;

/**
 * Base test for Redis collections.
 *
 * @author Costin Leau
 * @author Mark Paluch
 */
@RunWith(Parameterized.class)
public abstract class AbstractRedisCollectionTests<T> {

	protected AbstractRedisCollection<T> collection;
	protected ObjectFactory<T> factory;
	@SuppressWarnings("rawtypes") protected RedisTemplate template;

	@Before
	public void setUp() throws Exception {
		collection = createCollection();
	}

	abstract AbstractRedisCollection<T> createCollection();

	abstract RedisStore copyStore(RedisStore store);

	@SuppressWarnings("rawtypes")
	public AbstractRedisCollectionTests(ObjectFactory<T> factory, RedisTemplate template) {
		this.factory = factory;
		this.template = template;
		ConnectionFactoryTracker.add(template.getConnectionFactory());
	}

	@AfterClass
	public static void cleanUp() {
		ConnectionFactoryTracker.cleanUp();
	}

	@Parameters
	public static Collection<Object[]> testParams() {
		return CollectionTestParams.testParams();
	}

	/**
	 * Return a new instance of T
	 *
	 * @return
	 */
	protected T getT() {
		return factory.instance();
	}

	@SuppressWarnings("unchecked")
	@After
	public void tearDown() throws Exception {
		// remove the collection entirely since clear() doesn't always work
		collection.getOperations().delete(Collections.singleton(collection.getKey()));
		template.execute((RedisCallback<Object>) connection -> {
			connection.flushDb();
			return null;
		});
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
	public void testContainsObject() {
		T t1 = getT();
		assertThat(collection, not(hasItem(t1)));
		assertThat(collection.add(t1), is(true));
		assertThat(collection, hasItem(t1));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testContainsAll() {
		T t1 = getT();
		T t2 = getT();
		T t3 = getT();

		List<T> list = Arrays.asList(t1, t2, t3);

		assertThat(collection.addAll(list), is(true));
		assertThat(collection, hasItems((T[]) list.toArray()));
		assertThat(collection, hasItems(t1, t2, t3));
	}

	@Test
	public void testEquals() {
		// assertEquals(collection, copyStore(collection));
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

	@SuppressWarnings("unchecked")
	@Test
	public void testIterator() {
		T t1 = getT();
		T t2 = getT();
		T t3 = getT();
		T t4 = getT();

		List<T> list = Arrays.asList(t1, t2, t3, t4);

		assertThat(collection.addAll(list), is(true));
		Iterator<T> iterator = collection.iterator();

		assertThat(iterator.next(), isEqual(t1));
		assertThat(iterator.next(), isEqual(t2));
		assertThat(iterator.next(), isEqual(t3));
		assertThat(iterator.next(), isEqual(t4));
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

	@SuppressWarnings("unchecked")
	@Test
	public void removeAll() {
		T t1 = getT();
		T t2 = getT();
		T t3 = getT();

		List<T> list = Arrays.asList(t1, t2, t3);

		assertThat(collection.addAll(list), is(true));
		assertThat(collection, hasItems((T[]) list.toArray()));
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

	// @Test(expected = UnsupportedOperationException.class)
	@SuppressWarnings("unchecked")
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

	@Test
	public void testGetKey() throws Exception {
		assertNotNull(collection.getKey());
	}
}
