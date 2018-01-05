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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.data.redis.ObjectFactory;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.BoundSetOperations;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.test.util.MinimumRedisVersionRule;
import org.springframework.data.redis.test.util.RedisClientRule;
import org.springframework.data.redis.test.util.RedisDriver;
import org.springframework.data.redis.test.util.WithRedisDriver;
import org.springframework.test.annotation.IfProfileValue;

/**
 * Integration test for Redis set.
 *
 * @author Costin Leau
 * @author Christoph Strobl
 * @author Thomas Darimont
 */
public abstract class AbstractRedisSetTests<T> extends AbstractRedisCollectionTests<T> {

	public @Rule RedisClientRule clientRule = new RedisClientRule() {
		public RedisConnectionFactory getConnectionFactory() {
			return template.getConnectionFactory();
		}
	};

	public @Rule MinimumRedisVersionRule versionRule = new MinimumRedisVersionRule();

	protected RedisSet<T> set;

	/**
	 * Constructs a new <code>AbstractRedisSetTests</code> instance.
	 *
	 * @param factory
	 * @param template
	 */
	@SuppressWarnings("rawtypes")
	public AbstractRedisSetTests(ObjectFactory<T> factory, RedisTemplate template) {
		super(factory, template);
	}

	@SuppressWarnings("unchecked")
	@Before
	public void setUp() throws Exception {
		super.setUp();
		set = (RedisSet<T>) collection;
	}

	@SuppressWarnings("unchecked")
	private RedisSet<T> createSetFor(String key) {
		return new DefaultRedisSet<>((BoundSetOperations<String, T>) set.getOperations().boundSetOps(key));
	}

	@Test
	public void testDiff() {
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
		assertEquals(1, diff.size());
		assertThat(diff, hasItem(t1));
	}

	@Test
	public void testDiffAndStore() {
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

		assertEquals(1, diff.size());
		assertThat(diff, hasItem(t1));
		assertEquals(resultName, diff.getKey());
	}

	@Test
	public void testIntersect() {
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
		assertEquals(1, inter.size());
		assertThat(inter, hasItem(t2));
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
		assertEquals(1, inter.size());
		assertThat(inter, hasItem(t2));
		assertEquals(resultName, inter.getKey());
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testUnion() {
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
		assertEquals(4, union.size());
		assertThat(union, hasItems(t1, t2, t3, t4));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testUnionAndStore() {
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
		assertEquals(4, union.size());
		assertThat(union, hasItems(t1, t2, t3, t4));
		assertEquals(resultName, union.getKey());
	}

	@Test
	public void testIterator() {
		T t1 = getT();
		T t2 = getT();
		T t3 = getT();
		T t4 = getT();

		List<T> list = Arrays.asList(t1, t2, t3, t4);

		assertThat(collection.addAll(list), is(true));
		Iterator<T> iterator = collection.iterator();

		List<T> result = new ArrayList<>(list);

		while (iterator.hasNext()) {
			T expected = iterator.next();
			Iterator<T> resultItr = result.iterator();
			while (resultItr.hasNext()) {
				T obj = resultItr.next();
				if (isEqual(expected).matches(obj)) {
					resultItr.remove();
				}
			}
		}

		assertEquals(0, result.size());
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testToArray() {
		Object[] expectedArray = new Object[] { getT(), getT(), getT() };
		List<T> list = (List<T>) Arrays.asList(expectedArray);

		assertThat(collection.addAll(list), is(true));

		Object[] array = collection.toArray();

		List<T> result = new ArrayList<>(list);

		for (int i = 0; i < array.length; i++) {
			Iterator<T> resultItr = result.iterator();
			while (resultItr.hasNext()) {
				T obj = resultItr.next();
				if (isEqual(array[i]).matches(obj)) {
					resultItr.remove();
				}
			}
		}

		assertEquals(0, result.size());
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testToArrayWithGenerics() {
		Object[] expectedArray = new Object[] { getT(), getT(), getT() };
		List<T> list = (List<T>) Arrays.asList(expectedArray);

		assertThat(collection.addAll(list), is(true));

		Object[] array = collection.toArray(new Object[expectedArray.length]);
		List<T> result = new ArrayList<>(list);

		for (int i = 0; i < array.length; i++) {
			Iterator<T> resultItr = result.iterator();
			while (resultItr.hasNext()) {
				T obj = resultItr.next();
				if (isEqual(array[i]).matches(obj)) {
					resultItr.remove();
				}
			}
		}

		assertEquals(0, result.size());
	}

	// DATAREDIS-314
	@SuppressWarnings("unchecked")
	@IfProfileValue(name = "redisVersion", value = "2.8+")
	@Test
	@WithRedisDriver({ RedisDriver.JEDIS, RedisDriver.LETTUCE })
	public void testScanWorksCorrectly() throws IOException {

		Object[] expectedArray = new Object[] { getT(), getT(), getT() };
		collection.addAll((List<T>) Arrays.asList(expectedArray));

		Cursor<T> cursor = (Cursor<T>) set.scan();
		while (cursor.hasNext()) {
			assertThat(cursor.next(), anyOf(equalTo(expectedArray[0]), equalTo(expectedArray[1]), equalTo(expectedArray[2])));
		}
		cursor.close();
	}
}
