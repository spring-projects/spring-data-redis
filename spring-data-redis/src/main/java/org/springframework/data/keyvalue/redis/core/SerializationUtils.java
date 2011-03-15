/*
 * Copyright 2011 the original author or authors.
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
package org.springframework.data.keyvalue.redis.core;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;

import org.springframework.data.keyvalue.redis.connection.DefaultSortParameters;
import org.springframework.data.keyvalue.redis.connection.SortParameters;
import org.springframework.data.keyvalue.redis.core.query.SortQuery;
import org.springframework.data.keyvalue.redis.serializer.RedisSerializer;

/**
 * Utility class with various serialization-related methods. 
 * 
 * @author Costin Leau
 */
public abstract class SerializationUtils {

	public static <T> T deserialize(byte[] value, RedisSerializer<T> serializer) {
		return serializer.deserialize(value);
	}

	@SuppressWarnings("unchecked")
	static <T extends Collection<?>> T deserializeValues(Collection<byte[]> rawValues, Class<T> type, RedisSerializer<?> redisSerializer) {
		Collection<Object> values = (List.class.isAssignableFrom(type) ? new ArrayList<Object>(rawValues.size())
				: new LinkedHashSet<Object>(rawValues.size()));
		for (byte[] bs : rawValues) {
			values.add(redisSerializer.deserialize(bs));
		}

		return (T) values;
	}

	public static <K> SortParameters convertQuery(SortQuery<K> query, RedisSerializer<String> stringSerializer) {

		return new DefaultSortParameters(stringSerializer.serialize(query.getBy()), query.getLimit(), serialize(
				query.getGetPattern(), stringSerializer), query.getOrder(), query.isAlphabetic());
	}

	public static byte[][] serialize(List<String> strings, RedisSerializer<String> stringSerializer) {
		List<byte[]> raw = null;

		if (strings == null) {
			raw = Collections.emptyList();
		}
		else {
			raw = new ArrayList<byte[]>(strings.size());
			for (String key : strings) {
				raw.add(stringSerializer.serialize(key));
			}
		}
		return raw.toArray(new byte[raw.size()][]);
	}
}