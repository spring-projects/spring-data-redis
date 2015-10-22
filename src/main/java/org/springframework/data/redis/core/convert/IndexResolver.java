/*
 * Copyright 2015 the original author or authors.
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

import org.springframework.data.mapping.PersistentProperty;

/**
 * {@link IndexResolver} extracts secondary index structures to be applied on a given path, {@link PersistentProperty}
 * and value.
 * 
 * @author Christoph Strobl
 * @since 1.7
 */
public interface IndexResolver {

	/**
	 * Extracts a potential secondary index.
	 * 
	 * @param keyspace must not be {@literal null}.
	 * @param path can be {@literal null}.
	 * @param property must not be {@literal null}.
	 * @param value can be {@literal null}.
	 * @return {@literal null} if no index could be resolved for given arguments.
	 */
	IndexedData resolveIndex(String keyspace, String path, PersistentProperty<?> property, Object value);

}
