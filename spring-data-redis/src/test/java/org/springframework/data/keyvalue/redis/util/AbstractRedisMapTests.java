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
package org.springframework.data.keyvalue.redis.util;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.data.keyvalue.redis.connection.RedisConnection;
import org.springframework.data.keyvalue.redis.connection.RedisConnectionFactory;
import org.springframework.data.keyvalue.redis.core.RedisCallback;
import org.springframework.data.keyvalue.redis.core.RedisOperations;
import org.springframework.data.keyvalue.redis.core.RedisTemplate;

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

	private static Set<RedisConnectionFactory> connFactories = new LinkedHashSet<RedisConnectionFactory>();

	abstract RedisMap<K, V> createMap();

	@Before
	public void setUp() throws Exception {
		map = createMap();
	}

	public AbstractRedisMapTests(ObjectFactory<K> keyFactory, ObjectFactory<V> valueFactory, RedisTemplate template) {
		this.keyFactory = keyFactory;
		this.valueFactory = valueFactory;
		this.template = template;
		connFactories.add(template.getConnectionFactory());
	}

	@AfterClass
	public static void cleanUp() {
		if (connFactories != null) {
			for (RedisConnectionFactory connectionFactory : connFactories) {
				try {
					((DisposableBean) connectionFactory).destroy();
					System.out.println("Succesfully cleaned up factory " + connectionFactory);
				} catch (Exception ex) {
					System.err.println("Cannot clean factory " + connectionFactory + ex);
				}
			}
		}
	}

	protected K getKey() {
		return keyFactory.instance();
	}

	protected V getValue() {
		return valueFactory.instance();
	}

	protected RedisStore<String> copyStore(RedisStore<String> store) {
		return new DefaultRedisMap(store.getKey(), store.getOperations());
	}

	@After
	public void tearDown() throws Exception {
		// remove the collection entirely since clear() doesn't always work
		map.getOperations().delete(map.getKey());
		template.execute(new RedisCallback<Object>() {

			@Override
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
		RedisStore<String> clone = copyStore(map);
		assertEquals(clone, map);
		assertEquals(clone, clone);
		assertEquals(map, map);
	}

	@Test
	public void testNotEquals() {
		RedisOperations<String, ?> ops = map.getOperations();
		RedisStore<String> newInstance = new DefaultRedisMap<K, V>(ops.<K, V> forHash(map.getKey() + ":new"));
		assertFalse(map.equals(newInstance));
		assertFalse(newInstance.equals(map));
	}

	public V get(Object key) {
		return map.get(key);
	}

	@Test
	public void testGetKey() {
		assertNotNull(map.getKey());
	}

	public RedisOperations<String, ?> getOperations() {
		return map.getOperations();
	}

	@Test
	public void testHashCode() {
		assertThat(map.hashCode(), not(equalTo(map.getKey().hashCode())));
		assertEquals(map.hashCode(), copyStore(map).hashCode());
	}

	public Integer increment(K key, int delta) {
		return map.increment(key, delta);
	}

	public boolean isEmpty() {
		return map.isEmpty();
	}

	public Set<K> keySet() {
		return map.keySet();
	}

	public V put(K key, V value) {
		return map.put(key, value);
	}

	public void putAll(Map<? extends K, ? extends V> m) {
		map.putAll(m);
	}

	public boolean putIfAbsent(K key, V value) {
		return map.putIfAbsent(key, value);
	}

	public V remove(Object key) {
		return map.remove(key);
	}

	public int size() {
		return map.size();
	}

	public Collection<V> values() {
		return map.values();
	}
}