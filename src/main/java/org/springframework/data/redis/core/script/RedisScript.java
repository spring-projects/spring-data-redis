/*
 * Copyright 2013-2019 the original author or authors.
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

import org.springframework.core.io.Resource;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * A script to be executed using the <a href="https://redis.io/commands/eval">Redis scripting support</a> available as
 * of version 2.6
 *
 * @author Jennifer Hickey
 * @author Christoph Strobl
 * @author Mark Paluch
 * @param <T> The script result type. Should be one of Long, Boolean, List, or deserialized value type. Can be
 *          {@literal null} if the script returns a throw-away status (i.e "OK")
 */
public interface RedisScript<T> {

	/**
	 * @return The SHA1 of the script, used for executing Redis {@code evalsha} command.
	 */
	String getSha1();

	/**
	 * @return The script result type. Should be one of Long, Boolean, List, or deserialized value type. {@literal null}
	 *         if the script returns a throw-away status (i.e "OK").
	 */
	@Nullable
	Class<T> getResultType();

	/**
	 * @return The script contents.
	 */
	String getScriptAsString();

	/**
	 * @return {@literal true} if result type is {@literal null} and does not need any further deserialization.
	 * @since 2.0
	 */
	default boolean returnsRawValue() {
		return getResultType() == null;
	}

	/**
	 * Creates new {@link RedisScript} from {@link String}.
	 *
	 * @param script must not be {@literal null}.
	 * @return new instance of {@link RedisScript}.
	 * @since 2.0
	 */
	static <T> RedisScript<T> of(String script) {

		Assert.notNull(script, "Script must not be null!");

		return new DefaultRedisScript<>(script);
	}

	/**
	 * Creates new {@link RedisScript} from {@link String}.
	 *
	 * @param script must not be {@literal null}.
	 * @param resultType must not be {@literal null}.
	 * @return new instance of {@link RedisScript}.
	 * @since 2.0
	 */
	static <T> RedisScript<T> of(String script, Class<T> resultType) {

		Assert.notNull(script, "Script must not be null!");
		Assert.notNull(resultType, "ResultType must not be null!");

		return new DefaultRedisScript<>(script, resultType);
	}

	/**
	 * Creates new {@link RedisScript} from {@link Resource}.
	 *
	 * @param resource must not be {@literal null}.
	 * @return new instance of {@link RedisScript}.
	 * @since 2.2
	 */
	static <T> RedisScript<T> of(Resource resource) {

		Assert.notNull(resource, "Resource must not be null!");

		DefaultRedisScript<T> script = new DefaultRedisScript<>();
		script.setLocation(resource);
		script.afterPropertiesSet();

		return script;
	}

	/**
	 * Creates new {@link RedisScript} from {@link Resource}.
	 *
	 * @param resource must not be {@literal null}.
	 * @param resultType must not be {@literal null}.
	 * @return new instance of {@link RedisScript}.
	 * @since 2.2
	 */
	static <T> RedisScript<T> of(Resource resource, Class<T> resultType) {

		Assert.notNull(resource, "Resource must not be null!");
		Assert.notNull(resultType, "ResultType must not be null!");

		DefaultRedisScript<T> script = new DefaultRedisScript<>();
		script.setResultType(resultType);
		script.setLocation(resource);
		script.afterPropertiesSet();

		return script;
	}
}
