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
package org.springframework.data.redis.support.collections;

import static org.assertj.core.api.Assertions.*;

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
		assertThat(collection.add(t1)).isTrue();
		assertThat(collection).contains(t1);
		assertThat(collection.size()).isEqualTo(1);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testAddAll() {
		T t1 = getT();
		T t2 = getT();
		T t3 = getT();

		List<T> list = Arrays.asList(t1, t2, t3);

		assertThat(collection.addAll(list)).isTrue();
		assertThat(collection).contains(t1);
		assertThat(collection).contains(t2);
		assertThat(collection).contains(t3);
		assertThat(3).isEqualTo(collection.size());
	}

	@Test
	public void testClear() {
		T t1 = getT();
		assertThat(collection.size()).isEqualTo(0);
		collection.add(t1);
		assertThat(collection.size()).isEqualTo(1);
		collection.clear();
		assertThat(collection.size()).isEqualTo(0);
	}

	@Test
	public void testContainsObject() {
		T t1 = getT();
		assertThat(collection).doesNotContain(t1);
		assertThat(collection.add(t1)).isTrue();
		assertThat(collection).contains(t1);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testContainsAll() {
		T t1 = getT();
		T t2 = getT();
		T t3 = getT();

		List<T> list = Arrays.asList(t1, t2, t3);

		assertThat(collection.addAll(list)).isTrue();
		assertThat(collection).contains((T[]) list.toArray());
		assertThat(collection).contains(t1, t2, t3);
	}

	@Test
	public void testEquals() {
		// assertEquals(collection, copyStore(collection));
	}

	@Test
	public void testHashCode() {
		assertThat(collection.hashCode()).isNotEqualTo(collection.getKey().hashCode());
	}

	@Test
	public void testIsEmpty() {
		assertThat(collection.size()).isEqualTo(0);
		assertThat(collection.isEmpty()).isTrue();
		collection.add(getT());
		assertThat(collection.size()).isEqualTo(1);
		assertThat(collection.isEmpty()).isFalse();
		collection.clear();
		assertThat(collection.isEmpty()).isTrue();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testIterator() {
		T t1 = getT();
		T t2 = getT();
		T t3 = getT();
		T t4 = getT();

		List<T> list = Arrays.asList(t1, t2, t3, t4);

		assertThat(collection.addAll(list)).isTrue();
		Iterator<T> iterator = collection.iterator();

		assertThat(iterator.next()).isEqualTo(t1);
		assertThat(iterator.next()).isEqualTo(t2);
		assertThat(iterator.next()).isEqualTo(t3);
		assertThat(iterator.next()).isEqualTo(t4);
		assertThat(iterator.hasNext()).isFalse();
	}

	@Test
	public void testRemoveObject() {
		T t1 = getT();
		T t2 = getT();
		T t3 = getT();

		assertThat(collection.size()).isEqualTo(0);
		assertThat(collection.add(t1)).isTrue();
		assertThat(collection.add(t2)).isTrue();
		assertThat(collection.size()).isEqualTo(2);
		assertThat(collection.remove(t3)).isFalse();
		assertThat(collection.remove(t2)).isTrue();
		assertThat(collection.remove(t2)).isFalse();
		assertThat(collection.size()).isEqualTo(1);
		assertThat(collection.remove(t1)).isTrue();
		assertThat(collection.size()).isEqualTo(0);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void removeAll() {
		T t1 = getT();
		T t2 = getT();
		T t3 = getT();

		List<T> list = Arrays.asList(t1, t2, t3);

		assertThat(collection.addAll(list)).isTrue();
		assertThat(collection).contains((T[]) list.toArray());
		assertThat(collection).contains(t1, t2, t3);

		List<T> newList = Arrays.asList(getT(), getT());
		List<T> partialList = Arrays.asList(getT(), t1, getT());

		assertThat(collection.removeAll(newList)).isFalse();
		assertThat(collection.removeAll(partialList)).isTrue();
		assertThat(collection).doesNotContain(t1);
		assertThat(collection).contains(t2, t3);
		assertThat(collection.removeAll(list)).isTrue();
		assertThat(collection).doesNotContain(t2, t3);
	}

	// @Test(expected = UnsupportedOperationException.class)
	@SuppressWarnings("unchecked")
	public void testRetainAll() {
		T t1 = getT();
		T t2 = getT();
		T t3 = getT();

		List<T> list = Arrays.asList(t1, t2);
		List<T> newList = Arrays.asList(t2, t3);

		assertThat(collection.addAll(list)).isTrue();
		assertThat(collection).contains(t1, t2);
		assertThat(collection.retainAll(newList)).isTrue();
		assertThat(collection).doesNotContain(t1);
		assertThat(collection).contains(t2);
	}

	@Test
	public void testSize() {
		assertThat(collection.size()).isEqualTo(0);
		assertThat(collection.isEmpty()).isTrue();
		collection.add(getT());
		assertThat(collection.size()).isEqualTo(1);
		collection.add(getT());
		collection.add(getT());
		assertThat(collection.size()).isEqualTo(3);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testToArray() {
		Object[] expectedArray = new Object[] { getT(), getT(), getT() };
		List<T> list = (List<T>) Arrays.asList(expectedArray);

		assertThat(collection.addAll(list)).isTrue();

		Object[] array = collection.toArray();
		assertThat(array).isEqualTo(expectedArray);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testToArrayWithGenerics() {
		Object[] expectedArray = new Object[] { getT(), getT(), getT() };
		List<T> list = (List<T>) Arrays.asList(expectedArray);

		assertThat(collection.addAll(list)).isTrue();

		Object[] array = collection.toArray(new Object[expectedArray.length]);
		assertThat(array).isEqualTo(expectedArray);
	}

	@Test
	public void testToString() {
		String name = collection.toString();
		collection.add(getT());
		assertThat(collection.toString()).isEqualTo(name);
	}

	@Test
	public void testGetKey() throws Exception {
		assertThat(collection.getKey()).isNotNull();
	}
}
