/*
 * Copyright 2011-2023 the original author or authors.
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
package org.springframework.data.redis.support.collections;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;

/**
 * Utility class used mainly for type conversion by the default collection implementations. Meant for internal use.
 *
 * @author Costin Leau
 * @author John Blum
 */
public abstract class CollectionUtils {

	static Collection<String> extractKeys(Collection<? extends RedisStore> stores) {

		Collection<String> keys = new ArrayList<>(stores.size());

		for (RedisStore store : stores) {
			keys.add(store.getKey());
		}

		return keys;
	}

	public static <T> List<T> initializeList(@NonNull List<T> list, int size) {

		for (int count = 0; count < size; count++) {
			list.add(null);
		}

		return list;
	}

	@NonNull
	public static <T> List<T> nullSafeList(@Nullable List<T> list) {
		return list != null ? list : Collections.emptyList();
	}

	static <K> void rename(final K key, final K newKey, RedisOperations<K, ?> operations) {

		operations.execute(new SessionCallback<Object>() {

			@SuppressWarnings("unchecked")
			public Object execute(RedisOperations operations) throws DataAccessException {

				do {
					operations.watch(key);

					if (operations.hasKey(key)) {
						operations.multi();
						operations.rename(key, newKey);
					} else {
						operations.multi();
					}
				} while (operations.exec() == null);

				return null;
			}
		});
	}

	@SuppressWarnings("unchecked")
	static <E> Collection<E> reverse(Collection<? extends E> c) {

		Object[] reverse = new Object[c.size()];
		int index = c.size();

		for (E e : c) {
			reverse[--index] = e;
		}

		return (List<E>) Arrays.asList(reverse);
	}
}
