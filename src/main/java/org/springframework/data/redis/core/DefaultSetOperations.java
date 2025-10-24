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
package org.springframework.data.redis.core;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.util.Assert;

/**
 * Default implementation of {@link SetOperations}.
 *
 * @author Costin Leau
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Roman Bezpalko
 */
class DefaultSetOperations<K, V> extends AbstractOperations<K, V> implements SetOperations<K, V> {

	DefaultSetOperations(RedisTemplate<K, V> template) {
		super(template);
	}

	@Override
	public Long add(K key, V... values) {

		byte[] rawKey = rawKey(key);
		byte[][] rawValues = rawValues((Object[]) values);
		return execute(connection -> connection.sAdd(rawKey, rawValues));
	}

	@Override
	public Set<V> difference(K key, K otherKey) {
		return difference(Arrays.asList(key, otherKey));
	}

	@Override
	public Set<V> difference(K key, Collection<K> otherKeys) {

		byte[][] rawKeys = rawKeys(key, otherKeys);
		Set<byte[]> rawValues = execute(connection -> connection.sDiff(rawKeys));

		return deserializeValues(rawValues);
	}

	@Override
	public Set<V> difference(Collection<K> keys) {

		byte[][] rawKeys = rawKeys(keys);
		Set<byte[]> rawValues = execute(connection -> connection.sDiff(rawKeys));

		return deserializeValues(rawValues);
	}

	@Override
	public Long differenceAndStore(K key, K otherKey, K destKey) {
		return differenceAndStore(Arrays.asList(key, otherKey), destKey);
	}

	@Override
	public Long differenceAndStore(K key, Collection<K> otherKeys, K destKey) {

		byte[][] rawKeys = rawKeys(key, otherKeys);
		byte[] rawDestKey = rawKey(destKey);

		return execute(connection -> connection.sDiffStore(rawDestKey, rawKeys));
	}

	@Override
	public Long differenceAndStore(Collection<K> keys, K destKey) {

		byte[][] rawKeys = rawKeys(keys);
		byte[] rawDestKey = rawKey(destKey);

		return execute(connection -> connection.sDiffStore(rawDestKey, rawKeys));
	}

	@Override
	public Set<V> intersect(K key, K otherKey) {
		return intersect(Arrays.asList(key, otherKey));
	}

	@Override
	public Set<V> intersect(K key, Collection<K> otherKeys) {

		byte[][] rawKeys = rawKeys(key, otherKeys);
		Set<byte[]> rawValues = execute(connection -> connection.sInter(rawKeys));

		return deserializeValues(rawValues);
	}

	@Override
	public Set<V> intersect(Collection<K> keys) {

		byte[][] rawKeys = rawKeys(keys);
		Set<byte[]> rawValues = execute(connection -> connection.sInter(rawKeys));

		return deserializeValues(rawValues);
	}

	@Override
	public Long intersectAndStore(K key, K otherKey, K destKey) {
		return intersectAndStore(Arrays.asList(key, otherKey), destKey);
	}

	@Override
	public Long intersectAndStore(K key, Collection<K> otherKeys, K destKey) {

		byte[][] rawKeys = rawKeys(key, otherKeys);
		byte[] rawDestKey = rawKey(destKey);

		return execute(connection -> connection.sInterStore(rawDestKey, rawKeys));
	}

	@Override
	public Long intersectAndStore(Collection<K> keys, K destKey) {

		byte[][] rawKeys = rawKeys(keys);
		byte[] rawDestKey = rawKey(destKey);

		return execute(connection -> connection.sInterStore(rawDestKey, rawKeys));
	}

	@Override
	public Boolean isMember(K key, Object o) {

		byte[] rawKey = rawKey(key);
		byte[] rawValue = rawValue(o);

		return execute(connection -> connection.sIsMember(rawKey, rawValue));
	}

	@Override
	public Map<Object, Boolean> isMember(K key, Object... objects) {

		byte[] rawKey = rawKey(key);
		byte[][] rawValues = rawValues(objects);

		return execute(connection -> {

			List<Boolean> result = connection.sMIsMember(rawKey, rawValues);

			if (result == null || result.size() != objects.length) {
				return null;
			}

			Map<Object, Boolean> isMember = new LinkedHashMap<>(result.size());

			for (int i = 0; i < objects.length; i++) {
				isMember.put(objects[i], result.get(i));
			}

			return isMember;
		});
	}

	@Override
	public Set<V> members(K key) {

		byte[] rawKey = rawKey(key);
		Set<byte[]> rawValues = execute(connection -> connection.sMembers(rawKey));

		return deserializeValues(rawValues);
	}

	@Override
	public Boolean move(K key, V value, K destKey) {

		byte[] rawKey = rawKey(key);
		byte[] rawDestKey = rawKey(destKey);
		byte[] rawValue = rawValue(value);

		return execute(connection -> connection.sMove(rawKey, rawDestKey, rawValue));
	}

	@Override
	public V randomMember(K key) {

		return execute(new ValueDeserializingRedisCallback(key) {

			@Override
			protected byte[] inRedis(byte[] rawKey, RedisConnection connection) {
				return connection.sRandMember(rawKey);
			}
		});
	}

	@Override
	public Set<V> distinctRandomMembers(K key, long count) {

		Assert.isTrue(count >= 0, "Negative count not supported; Use randomMembers to allow duplicate elements");

		byte[] rawKey = rawKey(key);
		Set<byte[]> rawValues = execute(
				(RedisCallback<Set<byte[]>>) connection -> new HashSet<>(connection.sRandMember(rawKey, count)));

		return deserializeValues(rawValues);
	}

	@Override
	public List<V> randomMembers(K key, long count) {

		Assert.isTrue(count >= 0, "Use a positive number for count; This method is already allowing duplicate elements");

		byte[] rawKey = rawKey(key);
		List<byte[]> rawValues = execute(connection -> connection.sRandMember(rawKey, -count));

		return deserializeValues(rawValues);
	}

	@Override
	public Long remove(K key, Object... values) {

		byte[] rawKey = rawKey(key);
		byte[][] rawValues = rawValues(values);
		return execute(connection -> connection.sRem(rawKey, rawValues));
	}

	@Override
	public V pop(K key) {

		return execute(new ValueDeserializingRedisCallback(key) {

			@Override
			protected byte[] inRedis(byte[] rawKey, RedisConnection connection) {
				return connection.sPop(rawKey);
			}
		});
	}

	@Override
	public List<V> pop(K key, long count) {

		byte[] rawKey = rawKey(key);
		List<byte[]> rawValues = execute(connection -> connection.sPop(rawKey, count));

		return deserializeValues(rawValues);
	}

	@Override
	public Long size(K key) {

		byte[] rawKey = rawKey(key);
		return execute(connection -> connection.sCard(rawKey));
	}

	@Override
	public Set<V> union(K key, K otherKey) {
		return union(Arrays.asList(key, otherKey));
	}

	@Override
	public Set<V> union(K key, Collection<K> otherKeys) {

		byte[][] rawKeys = rawKeys(key, otherKeys);
		Set<byte[]> rawValues = execute(connection -> connection.sUnion(rawKeys));

		return deserializeValues(rawValues);
	}

	@Override
	public Set<V> union(Collection<K> keys) {

		byte[][] rawKeys = rawKeys(keys);
		Set<byte[]> rawValues = execute(connection -> connection.sUnion(rawKeys));

		return deserializeValues(rawValues);
	}

	@Override
	public Long unionAndStore(K key, K otherKey, K destKey) {
		return unionAndStore(Arrays.asList(key, otherKey), destKey);
	}

	@Override
	public Long unionAndStore(K key, Collection<K> otherKeys, K destKey) {

		byte[][] rawKeys = rawKeys(key, otherKeys);
		byte[] rawDestKey = rawKey(destKey);

		return execute(connection -> connection.sUnionStore(rawDestKey, rawKeys));
	}

	@Override
	public Long unionAndStore(Collection<K> keys, K destKey) {

		byte[][] rawKeys = rawKeys(keys);
		byte[] rawDestKey = rawKey(destKey);

		return execute(connection -> connection.sUnionStore(rawDestKey, rawKeys));
	}

	@Override
	public Cursor<V> scan(K key, ScanOptions options) {

		byte[] rawKey = rawKey(key);
		return template.executeWithStickyConnection(
				(RedisCallback<Cursor<V>>) connection -> new ConvertingCursor<>(connection.sScan(rawKey, options),
						this::deserializeValue));
	}
}
