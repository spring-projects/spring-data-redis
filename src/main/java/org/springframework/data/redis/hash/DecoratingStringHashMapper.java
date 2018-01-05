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
package org.springframework.data.redis.hash;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Delegating hash mapper used for flattening objects into Strings. Suitable when dealing with mappers that support
 * Strings and type conversion.
 *
 * @author Costin Leau
 */
public class DecoratingStringHashMapper<T> implements HashMapper<T, String, String> {

	private final HashMapper<T, ?, ?> delegate;

	public DecoratingStringHashMapper(HashMapper<T, ?, ?> mapper) {
		this.delegate = mapper;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.hash.HashMapper#fromHash(java.util.Map)
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public T fromHash(Map hash) {
		return (T) delegate.fromHash(hash);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.hash.HashMapper#toHash(java.lang.Object)
	 */
	@Override
	public Map<String, String> toHash(T object) {

		Map<?, ?> hash = delegate.toHash(object);
		Map<String, String> flatten = new LinkedHashMap<>(hash.size());
		for (Map.Entry<?, ?> entry : hash.entrySet()) {
			flatten.put(String.valueOf(entry.getKey()), String.valueOf(entry.getValue()));
		}

		return flatten;
	}
}
