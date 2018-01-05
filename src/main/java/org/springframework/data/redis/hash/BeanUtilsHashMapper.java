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
import java.util.Map.Entry;

import org.apache.commons.beanutils.BeanUtils;

/**
 * HashMapper based on Apache Commons BeanUtils project. Does NOT supports nested properties.
 *
 * @author Costin Leau
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public class BeanUtilsHashMapper<T> implements HashMapper<T, String, String> {

	private final Class<T> type;

	/**
	 * Create a new {@link BeanUtilsHashMapper} for the given {@code type}.
	 *
	 * @param type must not be {@literal null}.
	 */
	public BeanUtilsHashMapper(Class<T> type) {
		this.type = type;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.hash.HashMapper#fromHash(java.util.Map)
	 */
	@Override
	public T fromHash(Map<String, String> hash) {

		T instance = org.springframework.beans.BeanUtils.instantiateClass(type);

		try {

			BeanUtils.populate(instance, hash);
			return instance;
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.hash.HashMapper#toHash(java.lang.Object)
	 */
	@Override
	public Map<String, String> toHash(T object) {

		try {

			Map<String, String> map = BeanUtils.describe(object);
			Map<String, String> result = new LinkedHashMap<>();

			for (Entry<String, String> entry : map.entrySet()) {
				if (entry.getValue() != null) {
					result.put(entry.getKey(), entry.getValue());
				}
			}

			return result;
		} catch (Exception ex) {
			throw new IllegalArgumentException("Cannot describe object " + object, ex);
		}
	}
}
