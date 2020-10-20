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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import org.springframework.data.redis.ObjectFactory;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.test.extension.parametrized.MethodSource;
import org.springframework.data.redis.test.extension.parametrized.ParameterizedRedisTest;

/**
 * Base test for Redis collections.
 *
 * @author Costin Leau
 * @author Mark Paluch
 */
@MethodSource("testParams")
public abstract class AbstractRedisCollectionIntegrationTests<T> {

	protected AbstractRedisCollection<T> collection;
	protected ObjectFactory<T> factory;
	@SuppressWarnings("rawtypes") protected RedisTemplate template;

	@BeforeEach
	public void setUp() throws Exception {
		collection = createCollection();
	}

	abstract AbstractRedisCollection<T> createCollection();

	abstract RedisStore copyStore(RedisStore store);

	@SuppressWarnings("rawtypes")
	AbstractRedisCollectionIntegrationTests(ObjectFactory<T> factory, RedisTemplate template) {
		this.factory = factory;
		this.template = template;
	}

	public static Collection<Object[]> testParams() {
		return CollectionTestParams.testParams();
	}

	/**
	 * Return a new instance of T
	 *
	 * @return
	 */
	T getT() {
		return factory.instance();
	}

	@SuppressWarnings("unchecked")
	@AfterEach
	void tearDown() throws Exception {
		// remove the collection entirely since clear() doesn't always work
		collection.getOperations().delete(Collections.singleton(collection.getKey()));
		template.execute((RedisCallback<Object>) connection -> {
			connection.flushDb();
			return null;
		});
	}

	@ParameterizedRedisTest
	public void testAdd() {
		T t1 = getT();
		assertThat(collection.add(t1)).isTrue();
		assertThat(collection).contains(t1);
		assertThat(collection).hasSize(1);
	}

	@SuppressWarnings("unchecked")
	@ParameterizedRedisTest
	void testAddAll() {
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

	@ParameterizedRedisTest
	void testClear() {
		T t1 = getT();
		assertThat(collection).isEmpty();
		collection.add(t1);
		assertThat(collection).hasSize(1);
		collection.clear();
		assertThat(collection).isEmpty();
	}

	@ParameterizedRedisTest
	void testContainsObject() {
		T t1 = getT();
		assertThat(collection).doesNotContain(t1);
		assertThat(collection.add(t1)).isTrue();
		assertThat(collection).contains(t1);
	}

	@SuppressWarnings("unchecked")
	@ParameterizedRedisTest
	void testContainsAll() {
		T t1 = getT();
		T t2 = getT();
		T t3 = getT();

		List<T> list = Arrays.asList(t1, t2, t3);

		assertThat(collection.addAll(list)).isTrue();
		assertThat(collection).contains((T[]) list.toArray());
		assertThat(collection).contains(t1, t2, t3);
	}

	@ParameterizedRedisTest
	void testEquals() {
		// assertEquals(collection, copyStore(collection));
	}

	@ParameterizedRedisTest
	void testHashCode() {
		assertThat(collection.hashCode()).isNotEqualTo(collection.getKey().hashCode());
	}

	@ParameterizedRedisTest
	void testIsEmpty() {
		assertThat(collection).isEmpty();
		assertThat(collection.isEmpty()).isTrue();
		collection.add(getT());
		assertThat(collection).hasSize(1);
		assertThat(collection.isEmpty()).isFalse();
		collection.clear();
		assertThat(collection.isEmpty()).isTrue();
	}

	@SuppressWarnings("unchecked")
	@ParameterizedRedisTest
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

	@ParameterizedRedisTest
	void testRemoveObject() {
		T t1 = getT();
		T t2 = getT();
		T t3 = getT();

		assertThat(collection).isEmpty();
		assertThat(collection.add(t1)).isTrue();
		assertThat(collection.add(t2)).isTrue();
		assertThat(collection).hasSize(2);
		assertThat(collection.remove(t3)).isFalse();
		assertThat(collection.remove(t2)).isTrue();
		assertThat(collection.remove(t2)).isFalse();
		assertThat(collection).hasSize(1);
		assertThat(collection.remove(t1)).isTrue();
		assertThat(collection).isEmpty();
	}

	@SuppressWarnings("unchecked")
	@ParameterizedRedisTest
	void removeAll() {
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

	// @ParameterizedRedisTest(expected = UnsupportedOperationException.class)
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

	@ParameterizedRedisTest
	void testSize() {
		assertThat(collection).isEmpty();
		assertThat(collection.isEmpty()).isTrue();
		collection.add(getT());
		assertThat(collection).hasSize(1);
		collection.add(getT());
		collection.add(getT());
		assertThat(collection).hasSize(3);
	}

	@SuppressWarnings("unchecked")
	@ParameterizedRedisTest
	public void testToArray() {
		Object[] expectedArray = new Object[] { getT(), getT(), getT() };
		List<T> list = (List<T>) Arrays.asList(expectedArray);

		assertThat(collection.addAll(list)).isTrue();

		Object[] array = collection.toArray();
		assertThat(array).isEqualTo(expectedArray);
	}

	@SuppressWarnings("unchecked")
	@ParameterizedRedisTest
	public void testToArrayWithGenerics() {
		Object[] expectedArray = new Object[] { getT(), getT(), getT() };
		List<T> list = (List<T>) Arrays.asList(expectedArray);

		assertThat(collection.addAll(list)).isTrue();

		Object[] array = collection.toArray(new Object[expectedArray.length]);
		assertThat(array).isEqualTo(expectedArray);
	}

	@ParameterizedRedisTest
	void testToString() {
		String name = collection.toString();
		collection.add(getT());
		assertThat(collection.toString()).isEqualTo(name);
	}

	@ParameterizedRedisTest
	void testGetKey() throws Exception {
		assertThat(collection.getKey()).isNotNull();
	}
}
