/*
 * Copyright 2015-2018 the original author or authors.
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
package org.springframework.data.redis.core.convert;

import java.util.Set;

import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.util.TypeInformation;
import org.springframework.lang.Nullable;

/**
 * {@link IndexResolver} extracts secondary index structures to be applied on a given path, {@link PersistentProperty}
 * and value.
 *
 * @author Christoph Strobl
 * @since 1.7
 */
public interface IndexResolver {

	/**
	 * Resolves all indexes for given type information / value combination.
	 *
	 * @param typeInformation must not be {@literal null}.
	 * @param value the actual value. Can be {@literal null}.
	 * @return never {@literal null}.
	 */
	Set<IndexedData> resolveIndexesFor(TypeInformation<?> typeInformation, @Nullable Object value);

	/**
	 * Resolves all indexes for given type information / value combination.
	 *
	 * @param keyspace must not be {@literal null}.
	 * @param path must not be {@literal null}.
	 * @param typeInformation must not be {@literal null}.
	 * @param value the actual value. Can be {@literal null}.
	 * @return never {@literal null}.
	 */
	Set<IndexedData> resolveIndexesFor(String keyspace, String path, TypeInformation<?> typeInformation,
			@Nullable Object value);

}
