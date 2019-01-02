/*
 * Copyright 2018-2019 the original author or authors.
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
package org.springframework.data.redis.core;

import org.springframework.data.redis.hash.HashMapper;

/**
 * Function that returns a {@link HashMapper} for a given {@link Class type}.
 * <p/>
 * Implementors of this interface can return a generic or a specific {@link HashMapper} implementation, depending on the
 * serialization strategy for the requested {@link Class target type}.
 * 
 * @param <HK>
 * @param <HV>
 * @author Mark Paluch
 * @since 2.2
 */
@FunctionalInterface
public interface HashMapperProvider<HK, HV> {

	/**
	 * Get the {@link HashMapper} for a specific type.
	 *
	 * @param targetType must not be {@literal null}.
	 * @param <V> the value target type.
	 * @return the {@link HashMapper} suitable for a given type;
	 */
	<V> HashMapper<V, HK, HV> getHashMapper(Class<V> targetType);
}
