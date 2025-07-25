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
package org.springframework.data.redis.listener.adapter;

import org.jspecify.annotations.Nullable;
import org.springframework.dao.InvalidDataAccessApiUsageException;

/**
 * Exception thrown when the execution of a listener method failed.
 *
 * @author Costin Leau
 * @see MessageListenerAdapter
 */
public class RedisListenerExecutionFailedException extends InvalidDataAccessApiUsageException {

	/**
	 * Constructs a new {@code RedisListenerExecutionFailedException} instance.
	 *
	 * @param msg
	 * @param cause
	 */
	public RedisListenerExecutionFailedException(@Nullable String msg, @Nullable Throwable cause) {
		super(msg, cause);
	}

	/**
	 * Constructs a new {@code RedisListenerExecutionFailedException} instance.
	 *
	 * @param msg
	 */
	public RedisListenerExecutionFailedException(@Nullable String msg) {
		super(msg);
	}
}
