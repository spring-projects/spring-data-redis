/*
 * Copyright 2011-2025 the original author or authors.
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
package org.springframework.data.redis;

import org.jspecify.annotations.Nullable;
import org.springframework.dao.DataAccessResourceFailureException;

/**
 * Fatal exception thrown when the Redis connection fails completely.
 *
 * @author Mark Pollack
 */
public class RedisConnectionFailureException extends DataAccessResourceFailureException {

	/**
	 * @param msg the detail message.
	 */
	public RedisConnectionFailureException(@Nullable String msg) {
		super(msg);
	}

	/**
	 * @param msg the detail message.
	 * @param cause the nested exception.
	 */
	public RedisConnectionFailureException(@Nullable String msg, @Nullable Throwable cause) {
		super(msg, cause);
	}

}
