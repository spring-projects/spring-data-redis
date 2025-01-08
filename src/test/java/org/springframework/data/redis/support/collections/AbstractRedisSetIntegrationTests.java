/*
 * Copyright 2011-2025 the original author or authors.
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;

import org.springframework.data.redis.ObjectFactory;
import org.springframework.data.redis.core.BoundSetOperations;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.test.condition.EnabledOnCommand;
import org.springframework.data.redis.test.extension.parametrized.ParameterizedRedisTest;
import org.springframework.util.ObjectUtils;

/**
 * Integration test for Redis set.
 *
 * @author Costin Leau
 * @author Christoph Strobl
 * @author Thomas Darimont
 */
public abstract class AbstractRedisSetIntegrationTests<T> extends AbstractRedisCollectionIntegrationTests<T> {

	protected RedisSet<T> set;

	/**
	 * Constructs a new <code>AbstractRedisSetTests</code> instance.
	 *
	 * @param factory
	 * @param template
	 */
	@SuppressWarnings("rawtypes")
	AbstractRedisSetIntegrationTests(ObjectFactory<T> factory, RedisTemplate template) {
		super(factory, template);
	}

	@SuppressWarnings("unchecked")
	@BeforeEach
	public void setUp() throws Exception {
		super.setUp();
		set = (RedisSet<T>) collection;
	}

	@SuppressWarnings("unchecked")
	private RedisSet<T> createSetFor(String key) {
		return new DefaultRedisSet<>((BoundSetOperations<String, T>) set.getOperations().boundSetOps(key));
	}

	@ParameterizedRedisTest // GH-2037
	@EnabledOnCommand("SMISMEMBER")
	void testContainsAll() {
		
		T t1 = getT();
		T t2 = getT();
		T t3 = getT();

		set.add(t1);
		set.add(t2);

		assertThat(set.containsAll(Arrays.asList(t1, t2, t3))).isFalse();
		assertThat(set.containsAll(Arrays.asList(t1, t2))).isTrue();
		assertThat(set.containsAll(Collections.emptyList())).isTrue();
	}

	@ParameterizedRedisTest
	void testDiff() {
		RedisSet<T> diffSet1 = createSetFor("test:set:diff1");
		RedisSet<T> diffSet2 = createSetFor("test:set:diff2");

		T t1 = getT();
		T t2 = getT();
		T t3 = getT();

		set.add(t1);
		set.add(t2);
		set.add(t3);

		diffSet1.add(t2);
		diffSet2.add(t3);

		Set<T> diff = set.diff(Arrays.asList(diffSet1, diffSet2));
		assertThat(diff.size()).isEqualTo(1);
		assertThat(diff).contains(t1);
	}

	@ParameterizedRedisTest
	void testDiffAndStore() {
		RedisSet<T> diffSet1 = createSetFor("test:set:diff1");
		RedisSet<T> diffSet2 = createSetFor("test:set:diff2");

		T t1 = getT();
		T t2 = getT();
		T t3 = getT();
		T t4 = getT();

		set.add(t1);
		set.add(t2);
		set.add(t3);

		diffSet1.add(t2);
		diffSet2.add(t3);
		diffSet2.add(t4);

		String resultName = "test:set:diff:result:1";
		RedisSet<T> diff = set.diffAndStore(Arrays.asList(diffSet1, diffSet2), resultName);

		assertThat(diff.size()).isEqualTo(1);
		assertThat(diff).contains(t1);
		assertThat(diff.getKey()).isEqualTo(resultName);
	}

	@ParameterizedRedisTest
	void testIntersect() {
		RedisSet<T> intSet1 = createSetFor("test:set:int1");
		RedisSet<T> intSet2 = createSetFor("test:set:int2");

		T t1 = getT();
		T t2 = getT();
		T t3 = getT();
		T t4 = getT();

		set.add(t1);
		set.add(t2);
		set.add(t3);

		intSet1.add(t2);
		intSet1.add(t4);
		intSet2.add(t2);
		intSet2.add(t3);

		Set<T> inter = set.intersect(Arrays.asList(intSet1, intSet2));
		assertThat(inter.size()).isEqualTo(1);
		assertThat(inter).contains(t2);
	}

	public void testIntersectAndStore() {
		RedisSet<T> intSet1 = createSetFor("test:set:int1");
		RedisSet<T> intSet2 = createSetFor("test:set:int2");

		T t1 = getT();
		T t2 = getT();
		T t3 = getT();
		T t4 = getT();

		set.add(t1);
		set.add(t2);
		set.add(t3);

		intSet1.add(t2);
		intSet1.add(t4);
		intSet2.add(t2);
		intSet2.add(t3);

		String resultName = "test:set:intersect:result:1";
		RedisSet<T> inter = set.intersectAndStore(Arrays.asList(intSet1, intSet2), resultName);
		assertThat(inter.size()).isEqualTo(1);
		assertThat(inter).contains(t2);
		assertThat(inter.getKey()).isEqualTo(resultName);
	}

	@SuppressWarnings("unchecked")
	@ParameterizedRedisTest
	void testUnion() {
		RedisSet<T> unionSet1 = createSetFor("test:set:union1");
		RedisSet<T> unionSet2 = createSetFor("test:set:union2");

		T t1 = getT();
		T t2 = getT();
		T t3 = getT();
		T t4 = getT();

		set.add(t1);
		set.add(t2);

		unionSet1.add(t2);
		unionSet1.add(t4);
		unionSet2.add(t3);

		Set<T> union = set.union(Arrays.asList(unionSet1, unionSet2));
		assertThat(union.size()).isEqualTo(4);
		assertThat(union).contains(t1, t2, t3, t4);
	}

	@SuppressWarnings("unchecked")
	@ParameterizedRedisTest
	void testUnionAndStore() {
		RedisSet<T> unionSet1 = createSetFor("test:set:union1");
		RedisSet<T> unionSet2 = createSetFor("test:set:union2");

		T t1 = getT();
		T t2 = getT();
		T t3 = getT();
		T t4 = getT();

		set.add(t1);
		set.add(t2);

		unionSet1.add(t2);
		unionSet1.add(t4);
		unionSet2.add(t3);

		String resultName = "test:set:union:result:1";
		RedisSet<T> union = set.unionAndStore(Arrays.asList(unionSet1, unionSet2), resultName);
		assertThat(union.size()).isEqualTo(4);
		assertThat(union).contains(t1, t2, t3, t4);
		assertThat(union.getKey()).isEqualTo(resultName);
	}

	@ParameterizedRedisTest
	public void testIterator() {
		T t1 = getT();
		T t2 = getT();
		T t3 = getT();
		T t4 = getT();

		List<T> list = Arrays.asList(t1, t2, t3, t4);

		assertThat(collection.addAll(list)).isTrue();
		Iterator<T> iterator = collection.iterator();

		List<T> result = new ArrayList<>(list);

		while (iterator.hasNext()) {
			T expected = iterator.next();
			Iterator<T> resultItr = result.iterator();
			while (resultItr.hasNext()) {
				T obj = resultItr.next();
				if (ObjectUtils.nullSafeEquals(expected, obj)) {
					resultItr.remove();
				}
			}
		}

		assertThat(result.size()).isEqualTo(0);
	}

	@SuppressWarnings("unchecked")
	@ParameterizedRedisTest
	public void testToArray() {
		Object[] expectedArray = new Object[] { getT(), getT(), getT() };
		List<T> list = (List<T>) Arrays.asList(expectedArray);

		assertThat(collection.addAll(list)).isTrue();

		Object[] array = collection.toArray();

		List<T> result = new ArrayList<>(list);

		for (int i = 0; i < array.length; i++) {
			Iterator<T> resultItr = result.iterator();
			while (resultItr.hasNext()) {
				T obj = resultItr.next();
				if (ObjectUtils.nullSafeEquals(array[i], obj)) {
					resultItr.remove();
				}
			}
		}

		assertThat(result).isEmpty();
	}

	@SuppressWarnings("unchecked")
	@ParameterizedRedisTest
	public void testToArrayWithGenerics() {
		Object[] expectedArray = new Object[] { getT(), getT(), getT() };
		List<T> list = (List<T>) Arrays.asList(expectedArray);

		assertThat(collection.addAll(list)).isTrue();

		Object[] array = collection.toArray(new Object[expectedArray.length]);
		List<T> result = new ArrayList<>(list);

		for (int i = 0; i < array.length; i++) {
			Iterator<T> resultItr = result.iterator();
			while (resultItr.hasNext()) {
				T obj = resultItr.next();
				if (ObjectUtils.nullSafeEquals(array[i], obj)) {
					resultItr.remove();
				}
			}
		}

		assertThat(result.size()).isEqualTo(0);
	}

	// DATAREDIS-314
	@SuppressWarnings("unchecked")
	@ParameterizedRedisTest
	void testScanWorksCorrectly() throws IOException {

		Object[] expectedArray = new Object[] { getT(), getT(), getT() };
		collection.addAll((List<T>) Arrays.asList(expectedArray));

		Cursor<T> cursor = (Cursor<T>) set.scan();
		while (cursor.hasNext()) {
			assertThat(cursor.next()).isIn(expectedArray[0], expectedArray[1], expectedArray[2]);
		}
		cursor.close();
	}

	@ParameterizedRedisTest // GH-2049
	void randMemberReturnsSomething() {

		Object[] valuesArray = new Object[]{getT(), getT(), getT()};

		collection.addAll((List<T>) Arrays.asList(valuesArray));

		assertThat(set.randomValue()).isIn(valuesArray);
	}
}
