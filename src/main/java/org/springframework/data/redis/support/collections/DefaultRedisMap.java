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

import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.core.BoundHashOperations;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.lang.Nullable;

/**
 * Default implementation for {@link RedisMap}. Note that the current implementation doesn't provide the same locking
 * semantics across all methods. In highly concurrent environments, race conditions might appear.
 *
 * @author Costin Leau
 * @author Christoph Strobl
 * @author Christian Bühler
 */
public class DefaultRedisMap<K, V> implements RedisMap<K, V> {

	private final BoundHashOperations<String, K, V> hashOps;

	/**
	 * Constructs a new {@link DefaultRedisMap} instance.
	 *
	 * @param key Redis key of this map.
	 * @param operations {@link RedisOperations} for this map.
	 * @see RedisOperations#getHashKeySerializer()
	 * @see RedisOperations#getValueSerializer()
	 */
	public DefaultRedisMap(String key, RedisOperations<String, ?> operations) {
		this.hashOps = operations.boundHashOps(key);
	}

	/**
	 * Constructs a new {@link DefaultRedisMap} instance.
	 *
	 * @param boundOps {@link BoundHashOperations} for this map.
	 */
	public DefaultRedisMap(BoundHashOperations<String, K, V> boundOps) {
		this.hashOps = boundOps;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.support.collections.RedisMap#increment(java.lang.Object, long)
	 */
	@Override
	public Long increment(K key, long delta) {
		return hashOps.increment(key, delta);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.support.collections.RedisMap#increment(java.lang.Object, double)
	 */
	@Override
	public Double increment(K key, double delta) {
		return hashOps.increment(key, delta);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.support.collections.RedisStore#getOperations()
	 */
	@Override
	public RedisOperations<String, ?> getOperations() {
		return hashOps.getOperations();
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Map#clear()
	 */
	@Override
	public void clear() {
		getOperations().delete(Collections.singleton(getKey()));
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Map#containsKey(java.lang.Object)
	 */
	@Override
	public boolean containsKey(Object key) {

		Boolean result = hashOps.hasKey(key);
		checkResult(result);
		return result;
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Map#containsValue(java.lang.Object)
	 */
	@Override
	public boolean containsValue(Object value) {
		throw new UnsupportedOperationException();
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Map#entrySet()
	 */
	@Override
	public Set<java.util.Map.Entry<K, V>> entrySet() {

		Map<K, V> entries = hashOps.entries();
		checkResult(entries);
		return entries.entrySet();
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Map#get(java.lang.Object)
	 */
	@Override
	@Nullable
	public V get(Object key) {
		return hashOps.get(key);
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Map#isEmpty()
	 */
	@Override
	public boolean isEmpty() {
		return size() == 0;
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Map#keySet()
	 */
	@Override
	public Set<K> keySet() {
		return hashOps.keys();
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Map#put(java.lang.Object, java.lang.Object)
	 */
	@Override
	public V put(K key, V value) {

		V oldV = get(key);
		hashOps.put(key, value);
		return oldV;
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Map#putAll(java.util.Map)
	 */
	@Override
	public void putAll(Map<? extends K, ? extends V> m) {
		hashOps.putAll(m);
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Map#remove(java.lang.Object)
	 */
	@Override
	@Nullable
	public V remove(Object key) {

		V v = get(key);
		hashOps.delete(key);
		return v;
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Map#size()
	 */
	@Override
	public int size() {

		Long size = hashOps.size();
		checkResult(size);
		return size.intValue();
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Map#values()
	 */
	@Override
	public Collection<V> values() {
		return hashOps.values();
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object o) {

		if (o == this)
			return true;

		if (o instanceof RedisMap) {
			return o.hashCode() == hashCode();
		}
		return false;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {

		int result = 17 + getClass().hashCode();
		result = result * 31 + getKey().hashCode();
		return result;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {

		return "RedisStore for key:" + getKey();
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.concurrent.ConcurrentMap#putIfAbsent(java.lang.Object, java.lang.Object)
	 */
	@Override
	@Nullable
	public V putIfAbsent(K key, V value) {
		return (hashOps.putIfAbsent(key, value) ? null : get(key));
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.concurrent.ConcurrentMap#remove(java.lang.Object, java.lang.Object)
	 */
	@Override
	public boolean remove(Object key, Object value) {

		if (value == null) {
			throw new NullPointerException();
		}

		return hashOps.getOperations().execute(new SessionCallback<Boolean>() {
			@Override
			@SuppressWarnings({ "unchecked", "rawtypes" })
			public Boolean execute(RedisOperations ops) {
				for (;;) {
					ops.watch(Collections.singleton(getKey()));
					V v = get(key);
					if (value.equals(v)) {
						ops.multi();
						remove(key);
						if (ops.exec(ops.getHashValueSerializer()) != null) {
							return true;
						}
					} else {
						return false;
					}
				}
			}
		});
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.concurrent.ConcurrentMap#replace(java.lang.Object, java.lang.Object, java.lang.Object)
	 */
	@Override
	public boolean replace(K key, V oldValue, V newValue) {

		if (oldValue == null || newValue == null) {
			throw new NullPointerException();
		}

		return hashOps.getOperations().execute(new SessionCallback<Boolean>() {
			@Override
			@SuppressWarnings({ "unchecked", "rawtypes" })
			public Boolean execute(RedisOperations ops) {
				for (;;) {
					ops.watch(Collections.singleton(getKey()));
					V v = get(key);
					if (oldValue.equals(v)) {
						ops.multi();
						put(key, newValue);
						if (ops.exec(ops.getHashValueSerializer()) != null) {
							return true;
						}
					} else {
						return false;
					}
				}
			}
		});
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.concurrent.ConcurrentMap#replace(java.lang.Object, java.lang.Object)
	 */
	@Override
	@Nullable
	public V replace(K key, V value) {

		if (value == null) {
			throw new NullPointerException();
		}

		return hashOps.getOperations().execute(new SessionCallback<V>() {
			@Override
			@SuppressWarnings({ "unchecked", "rawtypes" })
			public V execute(RedisOperations ops) {
				for (;;) {
					ops.watch(Collections.singleton(getKey()));
					V v = get(key);
					if (v != null) {
						ops.multi();
						put(key, value);
						if (ops.exec(ops.getHashValueSerializer()) != null) {
							return v;
						}
					} else {
						return null;
					}
				}
			}
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundKeyOperations#expire(long, java.util.concurrent.TimeUnit)
	 */
	@Override
	public Boolean expire(long timeout, TimeUnit unit) {
		return hashOps.expire(timeout, unit);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundKeyOperations#expireAt(java.util.Date)
	 */
	@Override
	public Boolean expireAt(Date date) {
		return hashOps.expireAt(date);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundKeyOperations#getExpire()
	 */
	@Override
	public Long getExpire() {
		return hashOps.getExpire();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundKeyOperations#persist()
	 */
	@Override
	public Boolean persist() {
		return hashOps.persist();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundKeyOperations#getKey()
	 */
	@Override
	public String getKey() {
		return hashOps.getKey();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundKeyOperations#rename(java.lang.Object)
	 */
	@Override
	public void rename(String newKey) {
		hashOps.rename(newKey);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundKeyOperations#getType()
	 */
	@Override
	public DataType getType() {
		return hashOps.getType();
	}

	private void checkResult(@Nullable Object obj) {
		if (obj == null) {
			throw new IllegalStateException("Cannot read collection with Redis connection in pipeline/multi-exec mode");
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.support.collections.RedisMap#scan()
	 */
	@Override
	public Cursor<java.util.Map.Entry<K, V>> scan() {
		return scan(ScanOptions.NONE);
	}

	/**
	 * @since 1.4
	 * @param options
	 * @return
	 */
	private Cursor<java.util.Map.Entry<K, V>> scan(ScanOptions options) {
		return hashOps.scan(options);
	}
}
