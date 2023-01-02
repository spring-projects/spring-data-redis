/*
 * Copyright 2022-2023 the original author or authors.
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
package org.springframework.data.redis.hash;

import java.util.Map;

/**
 * Core mapping contract to materialize an object using particular Java class from a Redis Hash.
 *
 * @param <K> Redis Hash field type
 * @param <V> Redis Hash value type
 * @author Mark Paluch
 * @since 2.7
 * @see HashMapper
 */
public interface HashObjectReader<K, V> {

	/**
	 * Materialize an object of the {@link Class type} from a {@code hash}.
	 *
	 * @param hash must not be {@literal null}.
	 * @return the materialized object from the given {@code hash}.
	 */
	<R> R fromHash(Class<R> type, Map<K, V> hash);
}
