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
package org.springframework.data.redis.core;

import org.springframework.dao.DataAccessException;
import org.springframework.lang.Nullable;

/**
 * Callback executing all operations against a surrogate 'session' (basically against the same underlying Redis
 * connection). Allows 'transactions' to take place through the use of multi/discard/exec/watch/unwatch commands.
 *
 * @author Costin Leau
 */
public interface SessionCallback<T> {

	/**
	 * Executes all the given operations inside the same session.
	 *
	 * @param operations Redis operations
	 * @return return value
	 */
	@Nullable
	<K, V> T execute(RedisOperations<K, V> operations) throws DataAccessException;
}
