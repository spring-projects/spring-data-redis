/*
 * Copyright 2013-2018 the original author or authors.
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
package org.springframework.data.redis.connection;

import org.springframework.core.NestedRuntimeException;
import org.springframework.lang.Nullable;

/**
 * Exception thrown when there are issues with a resource pool
 *
 * @author Jennifer Hickey
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@SuppressWarnings("serial")
public class PoolException extends NestedRuntimeException {

	/**
	 * Constructs a new <code>PoolException</code> instance.
	 *
	 * @param msg the detail message.
	 */
	public PoolException(String msg) {
		super(msg);
	}

	/**
	 * Constructs a new <code>PoolException</code> instance.
	 *
	 * @param msg the detail message.
	 * @param cause the nested exception.
	 */
	public PoolException(@Nullable String msg, @Nullable Throwable cause) {
		super(msg, cause);
	}
}
