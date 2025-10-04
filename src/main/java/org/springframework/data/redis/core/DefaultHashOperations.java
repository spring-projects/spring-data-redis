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

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullUnmarked;
import org.jspecify.annotations.Nullable;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.redis.connection.ExpirationOptions;
import org.springframework.data.redis.connection.RedisHashCommands;
import org.springframework.data.redis.connection.convert.Converters;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.data.redis.core.types.Expirations;
import org.springframework.data.redis.core.types.Expirations.Timeouts;
import org.springframework.util.Assert;

/**
 * Default implementation of {@link HashOperations}.
 *
 * @author Costin Leau
 * @author Christoph Strobl
 * @author Ninad Divadkar
 * @author Tihomir Mateev
 */
@NullUnmarked
class DefaultHashOperations<K, HK, HV> extends AbstractOperations<K, Object> implements HashOperations<K, HK, HV> {

	@SuppressWarnings("unchecked")
	DefaultHashOperations(@NonNull RedisTemplate<K, ?> template) {
		super((RedisTemplate<K, Object>) template);
	}

	@Override
	@SuppressWarnings("unchecked")
	public HV get(@NonNull K key, @NonNull Object hashKey) {

		byte[] rawKey = rawKey(key);
		byte[] rawHashKey = rawHashKey(hashKey);
		byte[] rawHashValue = execute(connection -> connection.hGet(rawKey, rawHashKey));

		return (HV) rawHashValue != null ? deserializeHashValue(rawHashValue) : null;
	}

	@Override
	public Boolean hasKey(@NonNull K key, @NonNull Object hashKey) {

		byte[] rawKey = rawKey(key);
		byte[] rawHashKey = rawHashKey(hashKey);
		return execute(connection -> connection.hExists(rawKey, rawHashKey));
	}

	@Override
	public Long increment(@NonNull K key, @NonNull HK hashKey, long delta) {

		byte[] rawKey = rawKey(key);
		byte[] rawHashKey = rawHashKey(hashKey);
		return execute(connection -> connection.hIncrBy(rawKey, rawHashKey, delta));
	}

	@Override
	public Double increment(@NonNull K key, @NonNull HK hashKey, double delta) {

		byte[] rawKey = rawKey(key);
		byte[] rawHashKey = rawHashKey(hashKey);
		return execute(connection -> connection.hIncrBy(rawKey, rawHashKey, delta));
	}

	@Nullable
	@Override
	public HK randomKey(@NonNull K key) {

		byte[] rawKey = rawKey(key);
		return deserializeHashKey(execute(connection -> connection.hRandField(rawKey)));
	}

	@Override
	public Entry<@NonNull HK, HV> randomEntry(@NonNull K key) {

		byte[] rawKey = rawKey(key);
		Entry<byte[], byte[]> rawEntry = execute(connection -> connection.hRandFieldWithValues(rawKey));
		return rawEntry == null ? null
				: Converters.entryOf(deserializeHashKey(rawEntry.getKey()), deserializeHashValue(rawEntry.getValue()));
	}

	@Override
	public List<@NonNull HK> randomKeys(@NonNull K key, long count) {

		byte[] rawKey = rawKey(key);
		List<byte[]> rawValues = execute(connection -> connection.hRandField(rawKey, count));
		return deserializeHashKeys(rawValues);
	}

	@Override
	public Map<@NonNull HK, HV> randomEntries(@NonNull K key, long count) {

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
	public Set<@NonNull HK> keys(@NonNull K key) {

		byte[] rawKey = rawKey(key);
		Set<byte[]> rawValues = execute(connection -> connection.hKeys(rawKey));

		return rawValues != null ? deserializeHashKeys(rawValues) : Collections.emptySet();
	}

	@Override
	public Long size(@NonNull K key) {

		byte[] rawKey = rawKey(key);
		return execute(connection -> connection.hLen(rawKey));
	}

	@Override
	public Long lengthOfValue(@NonNull K key, @NonNull HK hashKey) {

		byte[] rawKey = rawKey(key);
		byte[] rawHashKey = rawHashKey(hashKey);
		return execute(connection -> connection.hStrLen(rawKey, rawHashKey));
	}

	@Override
	public void putAll(@NonNull K key, @NonNull Map<? extends @NonNull HK, ? extends HV> m) {

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
	public List<HV> multiGet(@NonNull K key, @NonNull Collection<@NonNull HK> fields) {

		if (fields.isEmpty()) {
			return Collections.emptyList();
		}

		byte[] rawKey = rawKey(key);
		byte[][] rawHashKeys = new byte[fields.size()][];

		int counter = 0;
		for (@NonNull
		HK hashKey : fields) {
			rawHashKeys[counter++] = rawHashKey(hashKey);
		}

		List<byte[]> rawValues = execute(connection -> connection.hMGet(rawKey, rawHashKeys));

		return deserializeHashValues(rawValues);
	}

    @Override
    public List<HV> getAndDelete(@NonNull K key, @NonNull Collection<@NonNull HK> fields) {

		if (fields.isEmpty()) {
			return Collections.emptyList();
		}

		byte[] rawKey = rawKey(key);
		byte[][] rawHashKeys = new byte[fields.size()][];
        int counter = 0;
		for (@NonNull
		HK hashKey : fields) {
			rawHashKeys[counter++] = rawHashKey(hashKey);
		}
        List<byte[]> rawValues = execute(connection -> connection.hashCommands().hGetDel(rawKey, rawHashKeys));

		return deserializeHashValues(rawValues);
	}

    @Override
    public List<HV> getAndExpire(@NonNull K key, @NonNull Expiration expiration,
			@NonNull Collection<@NonNull HK> fields) {

		if (fields.isEmpty()) {
			return Collections.emptyList();
		}

		byte[] rawKey = rawKey(key);
		byte[][] rawHashKeys = new byte[fields.size()][];
		int counter = 0;
		for (@NonNull
		HK hashKey : fields) {
			rawHashKeys[counter++] = rawHashKey(hashKey);
		}
		List<byte[]> rawValues = execute(connection -> connection.hashCommands().hGetEx(rawKey, expiration, rawHashKeys));

		return deserializeHashValues(rawValues);
	}

    @Override
    public Boolean putAndExpire(@NonNull K key, @NonNull Map<? extends @NonNull HK, ? extends HV> m,
                                RedisHashCommands.HashFieldSetOption condition, Expiration expiration) {
        if (m.isEmpty()) {
            return false;
        }

        byte[] rawKey = rawKey(key);

        Map<byte[], byte[]> hashes = new LinkedHashMap<>(m.size());

        for (Map.Entry<? extends HK, ? extends HV> entry : m.entrySet()) {
            hashes.put(rawHashKey(entry.getKey()), rawHashValue(entry.getValue()));
        }

        return execute(connection -> connection.hashCommands().hSetEx(rawKey, hashes, condition, expiration));
    }

	@Override
	public void put(@NonNull K key, @NonNull HK hashKey, HV value) {

		byte[] rawKey = rawKey(key);
		byte[] rawHashKey = rawHashKey(hashKey);
		byte[] rawHashValue = rawHashValue(value);

		execute(connection -> {
			connection.hSet(rawKey, rawHashKey, rawHashValue);
			return null;
		});
	}

	@Override
	public Boolean putIfAbsent(@NonNull K key, @NonNull HK hashKey, HV value) {

		byte[] rawKey = rawKey(key);
		byte[] rawHashKey = rawHashKey(hashKey);
		byte[] rawHashValue = rawHashValue(value);

		return execute(connection -> connection.hSetNX(rawKey, rawHashKey, rawHashValue));
	}

	@Override
	public ExpireChanges<@NonNull HK> expire(@NonNull K key, @NonNull Duration duration,
			@NonNull Collection<@NonNull HK> hashKeys) {

		List<HK> orderedKeys = List.copyOf(hashKeys);

		byte[] rawKey = rawKey(key);
		byte[][] rawHashKeys = rawHashKeys(orderedKeys.toArray());

		List<Long> raw = execute(connection -> TimeoutUtils.hasMillis(duration)
				? connection.hashCommands().hpExpire(rawKey, duration.toMillis(), rawHashKeys)
				: connection.hashCommands().hExpire(rawKey, TimeoutUtils.toSeconds(duration), rawHashKeys));

		return raw != null ? ExpireChanges.of(orderedKeys, raw) : null;
	}

	@Override
	public ExpireChanges<@NonNull HK> expireAt(@NonNull K key, @NonNull Instant instant,
			@NonNull Collection<@NonNull HK> hashKeys) {

		List<HK> orderedKeys = List.copyOf(hashKeys);

		byte[] rawKey = rawKey(key);
		byte[][] rawHashKeys = rawHashKeys(orderedKeys.toArray());
		long millis = instant.toEpochMilli();

		List<Long> raw = execute(connection -> TimeoutUtils.containsSplitSecond(millis)
				? connection.hashCommands().hpExpireAt(rawKey, millis, rawHashKeys)
				: connection.hashCommands().hExpireAt(rawKey, instant.getEpochSecond(), rawHashKeys));

		return raw != null ? ExpireChanges.of(orderedKeys, raw) : null;
	}

	@Override
	public ExpireChanges<@NonNull HK> expire(@NonNull K key, @NonNull Expiration expiration,
			@NonNull ExpirationOptions options, @NonNull Collection<@NonNull HK> hashKeys) {

		List<HK> orderedKeys = List.copyOf(hashKeys);
		byte[] rawKey = rawKey(key);
		byte[][] rawHashKeys = rawHashKeys(orderedKeys.toArray());
		List<Long> raw = execute(
				connection -> connection.hashCommands().applyHashFieldExpiration(rawKey, expiration, options, rawHashKeys));

		return raw != null ? ExpireChanges.of(orderedKeys, raw) : null;
	}

	@Override
	public ExpireChanges<@NonNull HK> persist(@NonNull K key, @NonNull Collection<@NonNull HK> hashKeys) {

		List<HK> orderedKeys = List.copyOf(hashKeys);
		byte[] rawKey = rawKey(key);
		byte[][] rawHashKeys = rawHashKeys(orderedKeys.toArray());

		List<Long> raw = execute(connection -> connection.hashCommands().hPersist(rawKey, rawHashKeys));

		return raw != null ? ExpireChanges.of(orderedKeys, raw) : null;
	}

	@Override
	public Expirations<@NonNull HK> getTimeToLive(@NonNull K key, @NonNull TimeUnit timeUnit,
			@NonNull Collection<@NonNull HK> hashKeys) {

		if (timeUnit.compareTo(TimeUnit.MILLISECONDS) < 0) {
			throw new IllegalArgumentException("%s precision is not supported must be >= MILLISECONDS".formatted(timeUnit));
		}

		List<HK> orderedKeys = List.copyOf(hashKeys);

		byte[] rawKey = rawKey(key);
		byte[][] rawHashKeys = rawHashKeys(orderedKeys.toArray());

		List<Long> raw = execute(
				connection -> TimeUnit.MILLISECONDS.equals(timeUnit) ? connection.hashCommands().hpTtl(rawKey, rawHashKeys)
						: connection.hashCommands().hTtl(rawKey, timeUnit, rawHashKeys));

		if (raw == null) {
			return null;
		}

		Timeouts timeouts = new Timeouts(TimeUnit.MILLISECONDS.equals(timeUnit) ? timeUnit : TimeUnit.SECONDS, raw);
		return Expirations.of(timeUnit, orderedKeys, timeouts);
	}

	@Override
	public List<HV> values(@NonNull K key) {

		byte[] rawKey = rawKey(key);
		List<byte[]> rawValues = execute(connection -> connection.hVals(rawKey));

		return rawValues != null ? deserializeHashValues(rawValues) : Collections.emptyList();
	}

	@Override
	public Long delete(@NonNull K key, @NonNull Object @NonNull... hashKeys) {

		byte[] rawKey = rawKey(key);
		byte[][] rawHashKeys = rawHashKeys(hashKeys);

		return execute(connection -> connection.hDel(rawKey, rawHashKeys));
	}

	@Override
	public Map<@NonNull HK, HV> entries(@NonNull K key) {

		byte[] rawKey = rawKey(key);
		Map<byte[], byte[]> entries = execute(connection -> connection.hGetAll(rawKey));

		return entries != null ? deserializeHashMap(entries) : Collections.emptyMap();
	}

	@Override
	public Cursor<Entry<@NonNull HK, HV>> scan(@NonNull K key, @Nullable ScanOptions options) {

		byte[] rawKey = rawKey(key);
		return template
				.executeWithStickyConnection((RedisCallback<Cursor<Entry<HK, HV>>>) connection -> new ConvertingCursor<>(
						connection.hScan(rawKey, options != null ? options : ScanOptions.NONE),
						new Converter<Entry<byte[], byte[]>, Entry<HK, HV>>() {

							@Override
							public Entry<HK, HV> convert(final Entry<byte[], byte[]> source) {
								return Converters.entryOf(deserializeHashKey(source.getKey()), deserializeHashValue(source.getValue()));
							}
						}));

	}
}
