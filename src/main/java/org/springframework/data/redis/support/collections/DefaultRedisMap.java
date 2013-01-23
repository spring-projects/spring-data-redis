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

import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.core.BoundHashOperations;
import org.springframework.data.redis.core.RedisOperations;

/**
 * Default implementation for {@link RedisMap}.
 * 
 * Note that the current implementation doesn't provide the same locking semantics across all methods.
 * In highly concurrent environments, race conditions might appear.
 * 
 * @author Costin Leau
 */
public class DefaultRedisMap<K, V> implements RedisMap<K, V> {

	private final BoundHashOperations<String, K, V> hashOps;

	private class DefaultRedisMapEntry implements Map.Entry<K, V> {

		private K key;
		private V value;

		public DefaultRedisMapEntry(K key, V value) {
			this.key = key;
			this.value = value;
		}

		
		public K getKey() {
			return key;
		}

		
		public V getValue() {
			return value;
		}

		
		public V setValue(V value) {
			throw new UnsupportedOperationException();
		}
	}

	/**
	 * Constructs a new <code>DefaultRedisMap</code> instance.
	 *
	 * @param key
	 * @param operations
	 */
	public DefaultRedisMap(String key, RedisOperations<String, ?> operations) {
		this.hashOps = operations.boundHashOps(key);
	}

	/**
	 * Constructs a new <code>DefaultRedisMap</code> instance.
	 *
	 * @param boundOps
	 */
	public DefaultRedisMap(BoundHashOperations<String, K, V> boundOps) {
		this.hashOps = boundOps;
	}

	
	public Long increment(K key, long delta) {
		return hashOps.increment(key, delta);
	}
	
	public RedisOperations<String, ?> getOperations() {
		return hashOps.getOperations();
	}
	
	public void clear() {
		getOperations().delete(Collections.singleton(getKey()));
	}
	
	public boolean containsKey(Object key) {
		Boolean result = hashOps.hasKey(key);
		checkResult(result);
		return result;
	}

	
	public boolean containsValue(Object value) {
		throw new UnsupportedOperationException();
	}
	
	public Set<java.util.Map.Entry<K, V>> entrySet() {
		Set<K> keySet = keySet();
		checkResult(keySet);
		Collection<V> multiGet = hashOps.multiGet(keySet);

		Iterator<K> keys = keySet.iterator();
		Iterator<V> values = multiGet.iterator();

		Set<Map.Entry<K, V>> entries = new LinkedHashSet<Entry<K, V>>();
		while (keys.hasNext()) {
			entries.add(new DefaultRedisMapEntry(keys.next(), values.next()));
		}

		return entries;
	}
	
	public V get(Object key) {
		return hashOps.get(key);
	}
	
	public boolean isEmpty() {
		return size() == 0;
	}
	
	public Set<K> keySet() {
		return hashOps.keys();
	}

	public V put(K key, V value) {
		V oldV = get(key);
		hashOps.put(key, value);
		return oldV;
	}
	
	public void putAll(Map<? extends K, ? extends V> m) {
		hashOps.putAll(m);
	}
	
	public V remove(Object key) {
		V v = get(key);
		hashOps.delete(key);
		return v;
	}
	
	public int size() {
		Long size = hashOps.size();
		checkResult(size);
		return size.intValue();
	}
	
	public Collection<V> values() {
		return hashOps.values();
	}
	
	public boolean equals(Object o) {
		if (o == this)
			return true;

		if (o instanceof RedisMap) {
			return o.hashCode() == hashCode();
		}
		return false;
	}
	
	public int hashCode() {
		int result = 17 + getClass().hashCode();
		result = result * 31 + getKey().hashCode();
		return result;
	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("RedisStore for key:");
		sb.append(getKey());
		return sb.toString();
	}

	
	public V putIfAbsent(K key, V value) {
		return (hashOps.putIfAbsent(key, value) ? null : get(key));

		//		RedisOperations<String, ?> ops = hashOps.getOperations();
		//
		//		for (;;) {
		//			ops.watch(getKey());
		//			V v = get(key);
		//			if (v == null) {
		//				ops.multi();
		//				put(key, value);
		//				if (ops.exec() != null) {
		//					return null;
		//				}
		//			}
		//			else {
		//				return v;
		//			}
		//		}
	}

	
	public boolean remove(Object key, Object value) {
		throw new UnsupportedOperationException();

		//		if (value == null){
		//			throw new NullPointerException();
		//		}
		//
		//		RedisOperations<String, ?> ops = hashOps.getOperations();
		//
		//		for (;;) {
		//			ops.watch(getKey());
		//			V v = get(key);
		//			if (value.equals(v)) {
		//				ops.multi();
		//				remove(key);
		//				if (ops.exec() != null) {
		//					return true;
		//				}
		//			}
		//			else {
		//				return false;
		//			}
		//		}
	}

	
	public boolean replace(K key, V oldValue, V newValue) {
		throw new UnsupportedOperationException();

		//		if (newValue == null || oldValue == null) {
		//			throw new NullPointerException();
		//		}
		//
		//		RedisOperations<String, ?> ops = hashOps.getOperations();
		//
		//		for (;;) {
		//			ops.watch(getKey());
		//			V v = get(key);
		//			if (oldValue.equals(v)) {
		//				ops.multi();
		//				put(key, newValue);
		//				if (ops.exec() != null) {
		//					return true;
		//				}
		//			}
		//			else {
		//				return false;
		//			}
		//		}
	}

	
	public V replace(K key, V value) {
		throw new UnsupportedOperationException();


		//		if (value == null) {
		//			throw new NullPointerException();
		//		}
		//
		//		RedisOperations<String, ?> ops = hashOps.getOperations();
		//
		//		for (;;) {
		//			ops.watch(getKey());
		//			if (containsKey(key)) {
		//				ops.multi();
		//				V oldValue = put(key, value);
		//				if (ops.exec() != null) {
		//					return oldValue;
		//				}
		//			}
		//			else {
		//				return null;
		//			}
		//		}
	}

	
	public Boolean expire(long timeout, TimeUnit unit) {
		return hashOps.expire(timeout, unit);
	}

	
	public Boolean expireAt(Date date) {
		return hashOps.expireAt(date);
	}

	
	public Long getExpire() {
		return hashOps.getExpire();
	}

	
	public Boolean persist() {
		return hashOps.persist();
	}


	
	public String getKey() {
		return hashOps.getKey();
	}

	
	public void rename(String newKey) {
		hashOps.rename(newKey);
	}

	
	public DataType getType() {
		return hashOps.getType();
	}

	private void checkResult(Object obj) {
		if (obj == null) {
			throw new IllegalStateException("Cannot read collection with Redis connection in pipeline/multi-exec mode");
		}
	}
}