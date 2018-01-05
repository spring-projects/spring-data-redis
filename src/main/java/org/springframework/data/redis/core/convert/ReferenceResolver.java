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

import java.util.Map;

import org.springframework.data.annotation.Reference;
import org.springframework.lang.Nullable;

/**
 * {@link ReferenceResolver} retrieves Objects marked with {@link Reference} from Redis.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 1.7
 */
public interface ReferenceResolver {

	/**
	 * @param id must not be {@literal null}.
	 * @param keyspace must not be {@literal null}.
	 * @return {@literal null} if referenced object does not exist.
	 */
	@Nullable
	Map<byte[], byte[]> resolveReference(Object id, String keyspace);
}
