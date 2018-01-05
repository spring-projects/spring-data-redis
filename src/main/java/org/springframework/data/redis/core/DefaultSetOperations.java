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
package org.springframework.data.redis.core;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.util.Assert;

/**
 * Default implementation of {@link SetOperations}.
 *
 * @author Costin Leau
 * @author Christoph Strobl
 * @author Mark Paluch
 */
class DefaultSetOperations<K, V> extends AbstractOperations<K, V> implements SetOperations<K, V> {

	DefaultSetOperations(RedisTemplate<K, V> template) {
		super(template);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.SetOperations#add(java.lang.Object, java.lang.Object[])
	 */
	@Override
	public Long add(K key, V... values) {

		byte[] rawKey = rawKey(key);
		byte[][] rawValues = rawValues((Object[]) values);
		return execute(connection -> connection.sAdd(rawKey, rawValues), true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.SetOperations#difference(java.lang.Object, java.lang.Object)
	 */
	@Override
	public Set<V> difference(K key, K otherKey) {
		return difference(key, Collections.singleton(otherKey));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.SetOperations#difference(java.lang.Object, java.util.Collection)
	 */
	@Override
	public Set<V> difference(K key, Collection<K> otherKeys) {

		byte[][] rawKeys = rawKeys(key, otherKeys);
		Set<byte[]> rawValues = execute(connection -> connection.sDiff(rawKeys), true);

		return deserializeValues(rawValues);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.SetOperations#differenceAndStore(java.lang.Object, java.lang.Object, java.lang.Object)
	 */
	@Override
	public Long differenceAndStore(K key, K otherKey, K destKey) {
		return differenceAndStore(key, Collections.singleton(otherKey), destKey);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.SetOperations#differenceAndStore(java.lang.Object, java.util.Collection, java.lang.Object)
	 */
	@Override
	public Long differenceAndStore(K key, Collection<K> otherKeys, K destKey) {

		byte[][] rawKeys = rawKeys(key, otherKeys);
		byte[] rawDestKey = rawKey(destKey);
		return execute(connection -> connection.sDiffStore(rawDestKey, rawKeys), true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.SetOperations#intersect(java.lang.Object, java.lang.Object)
	 */
	@Override
	public Set<V> intersect(K key, K otherKey) {
		return intersect(key, Collections.singleton(otherKey));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.SetOperations#intersect(java.lang.Object, java.util.Collection)
	 */
	@Override
	public Set<V> intersect(K key, Collection<K> otherKeys) {

		byte[][] rawKeys = rawKeys(key, otherKeys);
		Set<byte[]> rawValues = execute(connection -> connection.sInter(rawKeys), true);

		return deserializeValues(rawValues);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.SetOperations#intersectAndStore(java.lang.Object, java.lang.Object, java.lang.Object)
	 */
	@Override
	public Long intersectAndStore(K key, K otherKey, K destKey) {
		return intersectAndStore(key, Collections.singleton(otherKey), destKey);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.SetOperations#intersectAndStore(java.lang.Object, java.util.Collection, java.lang.Object)
	 */
	@Override
	public Long intersectAndStore(K key, Collection<K> otherKeys, K destKey) {

		byte[][] rawKeys = rawKeys(key, otherKeys);
		byte[] rawDestKey = rawKey(destKey);
		return execute(connection -> connection.sInterStore(rawDestKey, rawKeys), true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.SetOperations#isMember(java.lang.Object, java.lang.Object)
	 */
	@Override
	public Boolean isMember(K key, Object o) {

		byte[] rawKey = rawKey(key);
		byte[] rawValue = rawValue(o);
		return execute(connection -> connection.sIsMember(rawKey, rawValue), true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.SetOperations#members(java.lang.Object)
	 */
	@Override
	public Set<V> members(K key) {

		byte[] rawKey = rawKey(key);
		Set<byte[]> rawValues = execute(connection -> connection.sMembers(rawKey), true);

		return deserializeValues(rawValues);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.SetOperations#move(java.lang.Object, java.lang.Object, java.lang.Object)
	 */
	@Override
	public Boolean move(K key, V value, K destKey) {

		byte[] rawKey = rawKey(key);
		byte[] rawDestKey = rawKey(destKey);
		byte[] rawValue = rawValue(value);

		return execute(connection -> connection.sMove(rawKey, rawDestKey, rawValue), true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.SetOperations#randomMember(java.lang.Object)
	 */
	@Override
	public V randomMember(K key) {

		return execute(new ValueDeserializingRedisCallback(key) {

			@Override
			protected byte[] inRedis(byte[] rawKey, RedisConnection connection) {
				return connection.sRandMember(rawKey);
			}
		}, true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.SetOperations#distinctRandomMembers(java.lang.Object, long)
	 */
	@Override
	public Set<V> distinctRandomMembers(K key, long count) {

		Assert.isTrue(count >= 0, "Negative count not supported. " + "Use randomMembers to allow duplicate elements.");

		byte[] rawKey = rawKey(key);
		Set<byte[]> rawValues = execute(
				(RedisCallback<Set<byte[]>>) connection -> new HashSet<>(connection.sRandMember(rawKey, count)), true);

		return deserializeValues(rawValues);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.SetOperations#randomMembers(java.lang.Object, long)
	 */
	@Override
	public List<V> randomMembers(K key, long count) {

		Assert.isTrue(count >= 0,
				"Use a positive number for count. " + "This method is already allowing duplicate elements.");

		byte[] rawKey = rawKey(key);
		List<byte[]> rawValues = execute(connection -> connection.sRandMember(rawKey, -count), true);

		return deserializeValues(rawValues);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.SetOperations#remove(java.lang.Object, java.lang.Object[])
	 */
	@Override
	public Long remove(K key, Object... values) {

		byte[] rawKey = rawKey(key);
		byte[][] rawValues = rawValues(values);
		return execute(connection -> connection.sRem(rawKey, rawValues), true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.SetOperations#pop(java.lang.Object)
	 */
	@Override
	public V pop(K key) {

		return execute(new ValueDeserializingRedisCallback(key) {

			@Override
			protected byte[] inRedis(byte[] rawKey, RedisConnection connection) {
				return connection.sPop(rawKey);
			}
		}, true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.SetOperations(java.lang.Object, long)
	 */
	@Override
	public List<V> pop(K key, long count) {

		byte[] rawKey = rawKey(key);
		List<byte[]> rawValues = execute(connection -> connection.sPop(rawKey, count), true);

		return deserializeValues(rawValues);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.SetOperations#size(java.lang.Object)
	 */
	@Override
	public Long size(K key) {

		byte[] rawKey = rawKey(key);
		return execute(connection -> connection.sCard(rawKey), true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.SetOperations#union(java.lang.Object, java.lang.Object)
	 */
	@Override
	public Set<V> union(K key, K otherKey) {
		return union(key, Collections.singleton(otherKey));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.SetOperations#union(java.lang.Object, java.util.Collection)
	 */
	@Override
	public Set<V> union(K key, Collection<K> otherKeys) {

		byte[][] rawKeys = rawKeys(key, otherKeys);
		Set<byte[]> rawValues = execute(connection -> connection.sUnion(rawKeys), true);

		return deserializeValues(rawValues);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.SetOperations#union(java.lang.Object, java.lang.Object, java.lang.Object)
	 */
	@Override
	public Long unionAndStore(K key, K otherKey, K destKey) {
		return unionAndStore(key, Collections.singleton(otherKey), destKey);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.SetOperations#unionAndStore(java.lang.Object, java.util.Collection, java.lang.Object)
	 */
	@Override
	public Long unionAndStore(K key, Collection<K> otherKeys, K destKey) {

		byte[][] rawKeys = rawKeys(key, otherKeys);
		byte[] rawDestKey = rawKey(destKey);
		return execute(connection -> connection.sUnionStore(rawDestKey, rawKeys), true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.SetOperations#sScan(java.lang.Object, org.springframework.data.redis.core.ScanOptions)
	 */
	@Override
	public Cursor<V> scan(K key, ScanOptions options) {

		byte[] rawKey = rawKey(key);
		return template.executeWithStickyConnection(
				(RedisCallback<Cursor<V>>) connection -> new ConvertingCursor<>(connection.sScan(rawKey, options),
						this::deserializeValue));
	}
}
