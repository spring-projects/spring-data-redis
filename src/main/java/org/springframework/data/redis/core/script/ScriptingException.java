/*
 * Copyright 2013-2025 the original author or authors.
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
package org.springframework.data.redis.core.script;

import org.jspecify.annotations.Nullable;

import org.springframework.core.NestedRuntimeException;

/**
 * {@link RuntimeException} thrown when issues occur with {@link RedisScript}s
 *
 * @author Jennifer Hickey
 * @author Mark Paluch
 */
public class ScriptingException extends NestedRuntimeException {

	/**
	 * Constructs a new {@link ScriptingException} instance.
	 *
	 * @param msg the detail message.
	 */
	public ScriptingException(@Nullable String msg) {
		super(msg);
	}

	/**
	 * Constructs a new {@link ScriptingException} instance.
	 *
	 * @param msg the detail message.
	 * @param cause the nested exception.
	 */
	public ScriptingException(@Nullable String msg, @Nullable Throwable cause) {
		super(msg, cause);
	}

}
