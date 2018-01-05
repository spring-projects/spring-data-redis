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
package org.springframework.data.redis.core.query;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.springframework.data.redis.connection.DefaultSortParameters;
import org.springframework.data.redis.connection.SortParameters;
import org.springframework.data.redis.serializer.RedisSerializer;

/**
 * Utilities for {@link SortQuery} implementations.
 *
 * @author Costin Leau
 */
public abstract class QueryUtils {

	public static <K> SortParameters convertQuery(SortQuery<K> query, RedisSerializer<String> stringSerializer) {

		return new DefaultSortParameters(stringSerializer.serialize(query.getBy()), query.getLimit(), serialize(
				query.getGetPattern(), stringSerializer), query.getOrder(), query.isAlphabetic());
	}

	private static byte[][] serialize(List<String> strings, RedisSerializer<String> stringSerializer) {
		List<byte[]> raw = null;

		if (strings == null) {
			raw = Collections.emptyList();
		} else {
			raw = new ArrayList<>(strings.size());
			for (String key : strings) {
				raw.add(stringSerializer.serialize(key));
			}
		}
		return raw.toArray(new byte[raw.size()][]);
	}
}
