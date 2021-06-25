/*
 * Copyright 2011-2021 the original author or authors.
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

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.redis.connection.convert.Converters;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Default implementation of {@link HashOperations}.
 *
 * @author Costin Leau
 * @author Christoph Strobl
 * @author Ninad Divadkar
 */
class DefaultHashOperations<K, HK, HV> extends AbstractOperations<K, Object> implements HashOperations<K, HK, HV> {

	@SuppressWarnings("unchecked")
	DefaultHashOperations(RedisTemplate<K, ?> template) {
		super((RedisTemplate<K, Object>) template);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.HashOperations#get(java.lang.Object, java.lang.Object)
	 */
	@Override
	@SuppressWarnings("unchecked")
	public HV get(K key, Object hashKey) {

		byte[] rawKey = rawKey(key);
		byte[] rawHashKey = rawHashKey(hashKey);
		byte[] rawHashValue = execute(connection -> connection.hGet(rawKey, rawHashKey), true);

		return (HV) rawHashValue != null ? deserializeHashValue(rawHashValue) : null;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.HashOperations#hasKey(java.lang.Object, java.lang.Object)
	 */
	@Override
	public Boolean hasKey(K key, Object hashKey) {

		byte[] rawKey = rawKey(key);
		byte[] rawHashKey = rawHashKey(hashKey);
		return execute(connection -> connection.hExists(rawKey, rawHashKey), true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.HashOperations#increment(java.lang.Object, java.lang.Object, long)
	 */
	@Override
	public Long increment(K key, HK hashKey, long delta) {

		byte[] rawKey = rawKey(key);
		byte[] rawHashKey = rawHashKey(hashKey);
		return execute(connection -> connection.hIncrBy(rawKey, rawHashKey, delta), true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.HashOperations#increment(java.lang.Object, java.lang.Object, double)
	 */
	@Override
	public Double increment(K key, HK hashKey, double delta) {

		byte[] rawKey = rawKey(key);
		byte[] rawHashKey = rawHashKey(hashKey);
		return execute(connection -> connection.hIncrBy(rawKey, rawHashKey, delta), true);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.HashOperations#randomField(java.lang.Object)
	 */
	@Nullable
	@Override
	public HK randomField(K key) {

		byte[] rawKey = rawKey(key);
		return deserializeHashKey(execute(connection -> connection.hRandField(rawKey), true));
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.HashOperations#randomValue(java.lang.Object)
	 */
	@Nullable
	@Override
	public Entry<HK, HV> randomValue(K key) {

		byte[] rawKey = rawKey(key);
		Entry<byte[], byte[]> rawEntry = execute(connection -> connection.hRandFieldWithValues(rawKey), true);
		return rawEntry == null ? null
				: Converters.entryOf(deserializeHashKey(rawEntry.getKey()), deserializeHashValue(rawEntry.getValue()));
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.HashOperations#randomFields(java.lang.Object, long)
	 */
	@Nullable
	@Override
	public List<HK> randomFields(K key, long count) {

		byte[] rawKey = rawKey(key);
		List<byte[]> rawValues = execute(connection -> connection.hRandField(rawKey, count), true);
		return deserializeHashKeys(rawValues);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.HashOperations#randomValues(java.lang.Object, long)
	 */
	@Nullable
	@Override
	public Map<HK, HV> randomValues(K key, long count) {

		Assert.isTrue(count > 0, "Count must not be negative");
		byte[] rawKey = rawKey(key);
		List<Entry<byte[], byte[]>> rawEntries = execute(connection -> connection.hRandFieldWithValues(rawKey, count),
				true);

		if (rawEntries == null) {
			return null;
		}

		Map<byte[], byte[]> rawMap = new LinkedHashMap<>(rawEntries.size());
		rawEntries.forEach(entry -> rawMap.put(entry.getKey(), entry.getValue()));
		return deserializeHashMap(rawMap);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.HashOperations#keys(java.lang.Object)
	 */
	@Override
	public Set<HK> keys(K key) {

		byte[] rawKey = rawKey(key);
		Set<byte[]> rawValues = execute(connection -> connection.hKeys(rawKey), true);

		return rawValues != null ? deserializeHashKeys(rawValues) : Collections.emptySet();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.HashOperations#size(java.lang.Object)
	 */
	@Override
	public Long size(K key) {

		byte[] rawKey = rawKey(key);
		return execute(connection -> connection.hLen(rawKey), true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.HashOperations#lengthOfValue(java.lang.Object, java.lang.Object)
	 */
	@Nullable
	@Override
	public Long lengthOfValue(K key, HK hashKey) {

		byte[] rawKey = rawKey(key);
		byte[] rawHashKey = rawHashKey(hashKey);
		return execute(connection -> connection.hStrLen(rawKey, rawHashKey), true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.HashOperations#putAll(java.lang.Object, java.util.Map)
	 */
	@Override
	public void putAll(K key, Map<? extends HK, ? extends HV> m) {

		if (m.isEmpty()) {
			return;
		}

		byte[] rawKey = rawKey(key);

		Map<byte[], byte[]> hashes = new LinkedHashMap<>(m.size());

		for (Map.Entry<? extends HK, ? extends HV> entry : m.entrySet()) {
			hashes.put(rawHashKey(entry.getKey()), rawHashValue(entry.getValue()));
		}

		execute(connection -> {
			connection.hMSet(rawKey, hashes);
			return null;
		}, true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.HashOperations#multiGet(java.lang.Object, java.util.Collection)
	 */
	@Override
	public List<HV> multiGet(K key, Collection<HK> fields) {

		if (fields.isEmpty()) {
			return Collections.emptyList();
		}

		byte[] rawKey = rawKey(key);
		byte[][] rawHashKeys = new byte[fields.size()][];

		int counter = 0;
		for (HK hashKey : fields) {
			rawHashKeys[counter++] = rawHashKey(hashKey);
		}

		List<byte[]> rawValues = execute(connection -> connection.hMGet(rawKey, rawHashKeys), true);

		return deserializeHashValues(rawValues);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.HashOperations#put(java.lang.Object, java.lang.Object, java.lang.Object)
	 */
	@Override
	public void put(K key, HK hashKey, HV value) {

		byte[] rawKey = rawKey(key);
		byte[] rawHashKey = rawHashKey(hashKey);
		byte[] rawHashValue = rawHashValue(value);

		execute(connection -> {
			connection.hSet(rawKey, rawHashKey, rawHashValue);
			return null;
		}, true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.HashOperations#putIfAbsent(java.lang.Object, java.lang.Object, java.lang.Object)
	 */
	@Override
	public Boolean putIfAbsent(K key, HK hashKey, HV value) {

		byte[] rawKey = rawKey(key);
		byte[] rawHashKey = rawHashKey(hashKey);
		byte[] rawHashValue = rawHashValue(value);

		return execute(connection -> connection.hSetNX(rawKey, rawHashKey, rawHashValue), true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.HashOperations#values(java.lang.Object)
	 */
	@Override
	public List<HV> values(K key) {

		byte[] rawKey = rawKey(key);
		List<byte[]> rawValues = execute(connection -> connection.hVals(rawKey), true);

		return rawValues != null ? deserializeHashValues(rawValues) : Collections.emptyList();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.HashOperations#delete(java.lang.Object, java.lang.Object[])
	 */
	@Override
	public Long delete(K key, Object... hashKeys) {

		byte[] rawKey = rawKey(key);
		byte[][] rawHashKeys = rawHashKeys(hashKeys);

		return execute(connection -> connection.hDel(rawKey, rawHashKeys), true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.HashOperations#entries(java.lang.Object)
	 */
	@Override
	public Map<HK, HV> entries(K key) {

		byte[] rawKey = rawKey(key);
		Map<byte[], byte[]> entries = execute(connection -> connection.hGetAll(rawKey), true);

		return entries != null ? deserializeHashMap(entries) : Collections.emptyMap();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.HashOperations#scan(java.lang.Object, org.springframework.data.redis.core.ScanOptions)
	 */
	@Override
	public Cursor<Entry<HK, HV>> scan(K key, ScanOptions options) {

		byte[] rawKey = rawKey(key);
		return template.executeWithStickyConnection(
				(RedisCallback<Cursor<Entry<HK, HV>>>) connection -> new ConvertingCursor<>(connection.hScan(rawKey, options),
						new Converter<Entry<byte[], byte[]>, Entry<HK, HV>>() {

							@Override
							public Entry<HK, HV> convert(final Entry<byte[], byte[]> source) {

								return new Entry<HK, HV>() {

									@Override
									public HK getKey() {
										return deserializeHashKey(source.getKey());
									}

									@Override
									public HV getValue() {
										return deserializeHashValue(source.getValue());
									}

									@Override
									public HV setValue(HV value) {
										throw new UnsupportedOperationException("Values cannot be set when scanning through entries.");
									}
								};
							}
						}));

	}
}
