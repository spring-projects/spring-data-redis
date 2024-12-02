/*
 * Copyright 2011-2024 the original author or authors.
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

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

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
 * @author Tihomir Mateev
 */
class DefaultHashOperations<K, HK, HV> extends AbstractOperations<K, Object> implements HashOperations<K, HK, HV> {

	@SuppressWarnings("unchecked")
	DefaultHashOperations(RedisTemplate<K, ?> template) {
		super((RedisTemplate<K, Object>) template);
	}

	@Override
	@SuppressWarnings("unchecked")
	public HV get(K key, Object hashKey) {

		byte[] rawKey = rawKey(key);
		byte[] rawHashKey = rawHashKey(hashKey);
		byte[] rawHashValue = execute(connection -> connection.hGet(rawKey, rawHashKey));

		return (HV) rawHashValue != null ? deserializeHashValue(rawHashValue) : null;
	}

	@Override
	public Boolean hasKey(K key, Object hashKey) {

		byte[] rawKey = rawKey(key);
		byte[] rawHashKey = rawHashKey(hashKey);
		return execute(connection -> connection.hExists(rawKey, rawHashKey));
	}

	@Override
	public Long increment(K key, HK hashKey, long delta) {

		byte[] rawKey = rawKey(key);
		byte[] rawHashKey = rawHashKey(hashKey);
		return execute(connection -> connection.hIncrBy(rawKey, rawHashKey, delta));
	}

	@Override
	public Double increment(K key, HK hashKey, double delta) {

		byte[] rawKey = rawKey(key);
		byte[] rawHashKey = rawHashKey(hashKey);
		return execute(connection -> connection.hIncrBy(rawKey, rawHashKey, delta));
	}

	@Nullable
	@Override
	public HK randomKey(K key) {

		byte[] rawKey = rawKey(key);
		return deserializeHashKey(execute(connection -> connection.hRandField(rawKey)));
	}

	@Nullable
	@Override
	public Entry<HK, HV> randomEntry(K key) {

		byte[] rawKey = rawKey(key);
		Entry<byte[], byte[]> rawEntry = execute(connection -> connection.hRandFieldWithValues(rawKey));
		return rawEntry == null ? null
				: Converters.entryOf(deserializeHashKey(rawEntry.getKey()), deserializeHashValue(rawEntry.getValue()));
	}

	@Nullable
	@Override
	public List<HK> randomKeys(K key, long count) {

		byte[] rawKey = rawKey(key);
		List<byte[]> rawValues = execute(connection -> connection.hRandField(rawKey, count));
		return deserializeHashKeys(rawValues);
	}

	@Nullable
	@Override
	public Map<HK, HV> randomEntries(K key, long count) {

		Assert.isTrue(count > 0, "Count must not be negative");
		byte[] rawKey = rawKey(key);
		List<Entry<byte[], byte[]>> rawEntries = execute(connection -> connection.hRandFieldWithValues(rawKey, count));

		if (rawEntries == null) {
			return null;
		}

		Map<byte[], byte[]> rawMap = new LinkedHashMap<>(rawEntries.size());
		rawEntries.forEach(entry -> rawMap.put(entry.getKey(), entry.getValue()));
		return deserializeHashMap(rawMap);
	}

	@Override
	public Set<HK> keys(K key) {

		byte[] rawKey = rawKey(key);
		Set<byte[]> rawValues = execute(connection -> connection.hKeys(rawKey));

		return rawValues != null ? deserializeHashKeys(rawValues) : Collections.emptySet();
	}

	@Override
	public Long size(K key) {

		byte[] rawKey = rawKey(key);
		return execute(connection -> connection.hLen(rawKey));
	}

	@Nullable
	@Override
	public Long lengthOfValue(K key, HK hashKey) {

		byte[] rawKey = rawKey(key);
		byte[] rawHashKey = rawHashKey(hashKey);
		return execute(connection -> connection.hStrLen(rawKey, rawHashKey));
	}

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
		});
	}

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

		List<byte[]> rawValues = execute(connection -> connection.hMGet(rawKey, rawHashKeys));

		return deserializeHashValues(rawValues);
	}

	@Override
	public void put(K key, HK hashKey, HV value) {

		byte[] rawKey = rawKey(key);
		byte[] rawHashKey = rawHashKey(hashKey);
		byte[] rawHashValue = rawHashValue(value);

		execute(connection -> {
			connection.hSet(rawKey, rawHashKey, rawHashValue);
			return null;
		});
	}

	@Override
	public Boolean putIfAbsent(K key, HK hashKey, HV value) {

		byte[] rawKey = rawKey(key);
		byte[] rawHashKey = rawHashKey(hashKey);
		byte[] rawHashValue = rawHashValue(value);

		return execute(connection -> connection.hSetNX(rawKey, rawHashKey, rawHashValue));
	}

	@Override
	public List<Long> expire(K key, Duration duration, Collection<HK> hashKeys) {
		byte[] rawKey = rawKey(key);
		byte[][] rawHashKeys = rawHashKeys(hashKeys.toArray());
		long rawTimeout = duration.toMillis();

		return execute(connection -> connection.hpExpire(rawKey, rawTimeout, rawHashKeys));
	}

	@Override
	public List<Long> expireAt(K key, Instant instant, Collection<HK> hashKeys) {
		byte[] rawKey = rawKey(key);
		byte[][] rawHashKeys = rawHashKeys(hashKeys.toArray());

		return execute(connection -> connection.hpExpireAt(rawKey, instant.toEpochMilli(), rawHashKeys));
	}

	@Override
	public List<Long> persist(K key, Collection<HK> hashKeys) {
		byte[] rawKey = rawKey(key);
		byte[][] rawHashKeys = rawHashKeys(hashKeys.toArray());

		return execute(connection -> connection.hPersist(rawKey, rawHashKeys));
	}

	@Override
	public List<Long> getExpire(K key, Collection<HK> hashKeys) {
		byte[] rawKey = rawKey(key);
		byte[][] rawHashKeys = rawHashKeys(hashKeys.toArray());

		return execute(connection -> connection.hTtl(rawKey, rawHashKeys));
	}

	@Override
	public List<Long> getExpire(K key, TimeUnit timeUnit, Collection<HK> hashKeys) {
		byte[] rawKey = rawKey(key);
		byte[][] rawHashKeys = rawHashKeys(hashKeys.toArray());

		return execute(connection -> connection.hTtl(rawKey, timeUnit, rawHashKeys));
	}

	@Override
	public List<HV> values(K key) {

		byte[] rawKey = rawKey(key);
		List<byte[]> rawValues = execute(connection -> connection.hVals(rawKey));

		return rawValues != null ? deserializeHashValues(rawValues) : Collections.emptyList();
	}

	@Override
	public Long delete(K key, Object... hashKeys) {

		byte[] rawKey = rawKey(key);
		byte[][] rawHashKeys = rawHashKeys(hashKeys);

		return execute(connection -> connection.hDel(rawKey, rawHashKeys));
	}

	@Override
	public Map<HK, HV> entries(K key) {

		byte[] rawKey = rawKey(key);
		Map<byte[], byte[]> entries = execute(connection -> connection.hGetAll(rawKey));

		return entries != null ? deserializeHashMap(entries) : Collections.emptyMap();
	}

	@Override
	public Cursor<Entry<HK, HV>> scan(K key, ScanOptions options) {

		byte[] rawKey = rawKey(key);
		return template.executeWithStickyConnection(
				(RedisCallback<Cursor<Entry<HK, HV>>>) connection -> new ConvertingCursor<>(connection.hScan(rawKey, options),
						new Converter<Entry<byte[], byte[]>, Entry<HK, HV>>() {

							@Override
							public Entry<HK, HV> convert(final Entry<byte[], byte[]> source) {
								return Converters.entryOf(deserializeHashKey(source.getKey()), deserializeHashValue(source.getValue()));
							}
						}));

	}
}
