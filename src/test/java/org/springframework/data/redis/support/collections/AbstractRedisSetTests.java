/*
 * Copyright 2010-2011-2013 the original author or authors.
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
import static org.junit.matchers.JUnitMatchers.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.springframework.data.redis.core.BoundSetOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.support.collections.DefaultRedisSet;
import org.springframework.data.redis.support.collections.RedisSet;

/**
 * Integration test for Redis set.
 * 
 * @author Costin Leau
 */
public abstract class AbstractRedisSetTests<T> extends AbstractRedisCollectionTests<T> {

	protected RedisSet<T> set;


	/**
	 * Constructs a new <code>AbstractRedisSetTests</code> instance.
	 *
	 * @param factory
	 * @param template
	 */
	public AbstractRedisSetTests(ObjectFactory<T> factory, RedisTemplate template) {
		super(factory, template);
	}


	@SuppressWarnings("unchecked")
	@Before
	public void setUp() throws Exception {
		super.setUp();
		set = (RedisSet<T>) collection;
	}

	private RedisSet<T> createSetFor(String key) {
		return new DefaultRedisSet<T>((BoundSetOperations<String, T>) set.getOperations().boundSetOps(key));
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

		List<T> result = new ArrayList<T>(list);

		while (iterator.hasNext()) {
			result.remove(iterator.next());
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

		List<T> result = new ArrayList<T>(list);

		for (int i = 0; i < array.length; i++) {
			result.remove(array[i]);
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
		List<T> result = new ArrayList<T>(list);

		for (int i = 0; i < array.length; i++) {
			result.remove(array[i]);
		}

		assertEquals(0, result.size());
	}
}