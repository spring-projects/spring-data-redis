/*
 * Copyright 2015-2025 the original author or authors.
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
package org.springframework.data.redis.core;

import org.jspecify.annotations.Nullable;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisClusterConnection;

/**
 * Callback interface for low level operations executed against a clustered Redis environment.
 *
 * @author Christoph Strobl
 * @since 1.7
 * @param <T>
 */
public interface RedisClusterCallback<T> {

	/**
	 * Gets called by {@link ClusterOperations} with an active Redis connection. Does not need to care about activating or
	 * closing the connection or handling exceptions.
	 *
	 * @param connection never {@literal null}.
	 * @return
	 * @throws DataAccessException
	 */
	@Nullable
	T doInRedis(RedisClusterConnection connection) throws DataAccessException;

}
