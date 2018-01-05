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
package org.springframework.data.redis.serializer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.core.CollectionFactory;
import org.springframework.lang.Nullable;

/**
 * Utility class with various serialization-related methods.
 *
 * @author Costin Leau
 */
public abstract class SerializationUtils {

	static final byte[] EMPTY_ARRAY = new byte[0];

	static boolean isEmpty(@Nullable byte[] data) {
		return (data == null || data.length == 0);
	}

	@SuppressWarnings("unchecked")
	static <T extends Collection<?>> T deserializeValues(@Nullable Collection<byte[]> rawValues, Class<T> type,
			@Nullable RedisSerializer<?> redisSerializer) {
		// connection in pipeline/multi mode
		if (rawValues == null) {
			return (T) CollectionFactory.createCollection(type, 0);
		}

		Collection<Object> values = (List.class.isAssignableFrom(type) ? new ArrayList<>(rawValues.size())
				: new LinkedHashSet<>(rawValues.size()));
		for (byte[] bs : rawValues) {
			values.add(redisSerializer.deserialize(bs));
		}

		return (T) values;
	}

	@SuppressWarnings("unchecked")
	public static <T> Set<T> deserialize(@Nullable Set<byte[]> rawValues, @Nullable RedisSerializer<T> redisSerializer) {
		return deserializeValues(rawValues, Set.class, redisSerializer);
	}

	@SuppressWarnings("unchecked")
	public static <T> List<T> deserialize(@Nullable List<byte[]> rawValues,
			@Nullable RedisSerializer<T> redisSerializer) {
		return deserializeValues(rawValues, List.class, redisSerializer);
	}

	@SuppressWarnings("unchecked")
	public static <T> Collection<T> deserialize(@Nullable Collection<byte[]> rawValues,
			RedisSerializer<T> redisSerializer) {
		return deserializeValues(rawValues, List.class, redisSerializer);
	}

	public static <T> Map<T, T> deserialize(@Nullable Map<byte[], byte[]> rawValues, RedisSerializer<T> redisSerializer) {

		if (rawValues == null) {
			return Collections.emptyMap();
		}
		Map<T, T> ret = new LinkedHashMap<>(rawValues.size());
		for (Map.Entry<byte[], byte[]> entry : rawValues.entrySet()) {
			ret.put(redisSerializer.deserialize(entry.getKey()), redisSerializer.deserialize(entry.getValue()));
		}
		return ret;
	}

	@SuppressWarnings("unchecked")
	public static <HK, HV> Map<HK, HV> deserialize(@Nullable Map<byte[], byte[]> rawValues,
			@Nullable RedisSerializer<HK> hashKeySerializer, @Nullable RedisSerializer<HV> hashValueSerializer) {

		if (rawValues == null) {
			return Collections.emptyMap();
		}
		Map<HK, HV> map = new LinkedHashMap<>(rawValues.size());
		for (Map.Entry<byte[], byte[]> entry : rawValues.entrySet()) {
			// May want to deserialize only key or value
			HK key = hashKeySerializer != null ? hashKeySerializer.deserialize(entry.getKey()) : (HK) entry.getKey();
			HV value = hashValueSerializer != null ? hashValueSerializer.deserialize(entry.getValue())
					: (HV) entry.getValue();
			map.put(key, value);
		}
		return map;
	}
}
