/*
 * Copyright 2010-2011 the original author or authors.
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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;
import static org.junit.matchers.JUnitMatchers.hasItem;
import static org.junit.matchers.JUnitMatchers.hasItems;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.Map.Entry;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.ConnectionFactoryTracker;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.RedisTemplate;

/**
 * Integration test for Redis Map.
 * 
 * @author Costin Leau
 */
@RunWith(Parameterized.class)
public abstract class AbstractRedisMapTests<K, V> {

	protected RedisMap<K, V> map;
	protected ObjectFactory<K> keyFactory;
	protected ObjectFactory<V> valueFactory;
	protected RedisTemplate template;

	abstract RedisMap<K, V> createMap();

	@Before
	public void setUp() throws Exception {
		map = createMap();
	}

	public AbstractRedisMapTests(ObjectFactory<K> keyFactory, ObjectFactory<V> valueFactory, RedisTemplate template) {
		this.keyFactory = keyFactory;
		this.valueFactory = valueFactory;
		this.template = template;
		ConnectionFactoryTracker.add(template.getConnectionFactory());
	}

	@AfterClass
	public static void cleanUp() {
		ConnectionFactoryTracker.cleanUp();
	}

	protected K getKey() {
		return keyFactory.instance();
	}

	protected V getValue() {
		return valueFactory.instance();
	}

	protected RedisStore copyStore(RedisStore store) {
		return new DefaultRedisMap(store.getKey(), store.getOperations());
	}

	@After
	public void tearDown() throws Exception {
		// remove the collection entirely since clear() doesn't always work
		map.getOperations().delete(Collections.singleton(map.getKey()));
		template.execute(new RedisCallback<Object>() {

			
			public Object doInRedis(RedisConnection connection) {
				connection.flushDb();
				return null;
			}
		});
	}

	@Test
	public void testClear() {
		map.clear();
		assertEquals(0, map.size());
		map.put(getKey(), getValue());
		assertEquals(1, map.size());
		map.clear();
		assertEquals(0, map.size());
	}

	@Test
	public void testContainsKey() {
		K k1 = getKey();
		K k2 = getKey();

		assertFalse(map.containsKey(k1));
		assertFalse(map.containsKey(k2));
		map.put(k1, getValue());
		assertTrue(map.containsKey(k1));
		map.put(k2, getValue());
		assertTrue(map.containsKey(k2));
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testContainsValue() {
		V v1 = getValue();
		V v2 = getValue();

		assertFalse(map.containsValue(v1));
		assertFalse(map.containsValue(v2));
		map.put(getKey(), v1);
		assertTrue(map.containsValue(v1));
		map.put(getKey(), v2);
		assertTrue(map.containsValue(v2));
	}

	public Set<Entry<K, V>> entrySet() {
		return map.entrySet();
	}

	@Test
	public void testEquals() {
		RedisStore clone = copyStore(map);
		assertEquals(clone, map);
		assertEquals(clone, clone);
		assertEquals(map, map);
	}

	@Test
	public void testNotEquals() {
		RedisOperations<String, ?> ops = map.getOperations();
		RedisStore newInstance = new DefaultRedisMap<K, V>(ops.<K, V> boundHashOps(map.getKey() + ":new"));
		assertFalse(map.equals(newInstance));
		assertFalse(newInstance.equals(map));
	}

	@Test
	public void testGet() {
		K k1 = getKey();
		V v1 = getValue();

		assertNull(map.get(UUID.randomUUID().toString()));
		assertNull(map.get(k1));
		map.put(k1, v1);
		assertEquals(v1, map.get(k1));
	}

	@Test
	public void testGetKey() {
		assertNotNull(map.getKey());
	}

	@Test
	public void testGetOperations() {
		assertEquals(template, map.getOperations());
	}

	@Test
	public void testHashCode() {
		assertThat(map.hashCode(), not(equalTo(map.getKey().hashCode())));
		assertEquals(map.hashCode(), copyStore(map).hashCode());
	}

	@Test
	public void testIncrement() {
		assumeTrue(!isJredis());
		K k1 = getKey();
		V v1 = getValue();

		map.put(k1, v1);
		try {
			Long value = map.increment(k1, 1);
			System.out.println("Value is " + value);
		} catch (InvalidDataAccessApiUsageException ex) {
			// expected
		}
	}

	@Test
	public void testIsEmpty() {
		map.clear();
		assertTrue(map.isEmpty());
		map.put(getKey(), getValue());
		assertFalse(map.isEmpty());
		map.clear();
		assertTrue(map.isEmpty());
	}

	@Test
	public void testKeySet() {
		map.clear();
		assertTrue(map.keySet().isEmpty());
		K k1 = getKey();
		K k2 = getKey();
		K k3 = getKey();

		map.put(k1, getValue());
		map.put(k2, getValue());
		map.put(k3, getValue());

		Set<K> keySet = map.keySet();
		assertThat(keySet, hasItems(k1, k2, k3));
		assertEquals(3, keySet.size());
	}

	@Test
	public void testPut() {
		K k1 = getKey();
		K k2 = getKey();
		V v1 = getValue();
		V v2 = getValue();

		map.put(k1, v1);
		map.put(k2, v2);

		assertEquals(v1, map.get(k1));
		assertEquals(v2, map.get(k2));
	}

	@Test
	public void testPutAll() {
		assumeTrue(!isJredis());
		Map<K, V> m = new LinkedHashMap<K, V>();
		K k1 = getKey();
		K k2 = getKey();

		V v1 = getValue();
		V v2 = getValue();

		m.put(k1, v1);
		m.put(k2, v2);

		assertNull(map.get(k1));
		assertNull(map.get(k2));

		map.putAll(m);

		assertEquals(v1, map.get(k1));
		assertEquals(v2, map.get(k2));
	}

	@Test
	public void testRemove() {
		K k1 = getKey();
		K k2 = getKey();

		V v1 = getValue();
		V v2 = getValue();

		assertNull(map.remove(k1));
		assertNull(map.remove(k2));

		map.put(k1, v1);
		map.put(k2, v2);

		assertEquals(v1, map.remove(k1));
		assertNull(map.remove(k1));
		assertNull(map.get(k1));

		assertEquals(v2, map.remove(k2));
		assertNull(map.remove(k2));
		assertNull(map.get(k2));
	}

	@Test
	public void testSize() {
		assertEquals(0, map.size());
		map.put(getKey(), getValue());
		assertEquals(1, map.size());
		K k = getKey();
		map.put(k, getValue());
		assertEquals(2, map.size());
		map.remove(k);
		assertEquals(1, map.size());

		map.clear();
		assertEquals(0, map.size());
	}

	@Test
	public void testValues() {
		V v1 = getValue();
		V v2 = getValue();
		V v3 = getValue();

		map.put(getKey(), v1);
		map.put(getKey(), v2);

		Collection<V> values = map.values();
		assertEquals(2, values.size());
		assertThat(values, hasItems(v1, v2));

		map.put(getKey(), v3);
		values = map.values();
		assertEquals(3, values.size());
		assertThat(values, hasItems(v1, v2, v3));
	}

	@Test
	public void testEntrySet() {
		assumeTrue(!isJredis());
		Set<Entry<K, V>> entries = map.entrySet();
		assertTrue(entries.isEmpty());

		K k1 = getKey();
		K k2 = getKey();

		V v1 = getValue();
		V v2 = getValue();

		map.put(k1, v1);
		map.put(k2, v1);

		entries = map.entrySet();

		Set<K> keys = new LinkedHashSet<K>();
		Collection<V> values = new ArrayList<V>();

		for (Entry<K, V> entry : entries) {
			keys.add(entry.getKey());
			values.add(entry.getValue());
		}

		assertEquals(2, keys.size());

		assertThat(keys, hasItems(k1, k2));
		assertThat(values, hasItem(v1));
		assertThat(values, not(hasItem(v2)));
	}


	//@Test(expected = UnsupportedOperationException.class)
	public void testConcurrentPutIfAbsent() {
		K k1 = getKey();
		K k2 = getKey();

		V v1 = getValue();
		V v2 = getValue();

		assertNull(map.get(k1));
		assertNull(map.putIfAbsent(k1, v1));
		assertEquals(v1, map.putIfAbsent(k1, v2));
		assertEquals(v1, map.get(k1));

		assertNull(map.putIfAbsent(k2, v2));
		assertEquals(v2, map.putIfAbsent(k2, v1));

		assertEquals(v2, map.get(k2));
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testConcurrentRemove() {
		K k1 = getKey();
		V v1 = getValue();
		V v2 = getValue();

		map.put(k1, v1);
		assertFalse(map.remove(k1, v1));
		assertEquals(v1, map.get(k1));
		assertTrue(map.remove(k1, v1));
		assertNull(map.get(k1));
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testConcurrentReplaceTwoArgs() {
		K k1 = getKey();
		V v1 = getValue();
		V v2 = getValue();

		map.put(k1, v1);

		assertFalse(map.replace(k1, v2, v1));
		assertEquals(v1, map.get(k1));
		assertTrue(map.replace(k1, v1, v2));
		assertEquals(v2, map.get(k1));
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testConcurrentReplaceOneArg() {
		K k1 = getKey();
		V v1 = getValue();
		V v2 = getValue();

		assertNull(map.replace(k1, v1));
		map.put(k1, v1);
		assertNull(map.replace(getKey(), v1));
		assertEquals(v1, map.replace(k1, v2));
		assertEquals(v2, map.get(k1));

	}

	private boolean isJredis() {
		return template.getConnectionFactory().getClass().getSimpleName().startsWith("Jredis");
	}
}