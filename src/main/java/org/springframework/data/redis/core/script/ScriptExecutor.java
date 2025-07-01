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

import java.util.List;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullUnmarked;
import org.jspecify.annotations.Nullable;

import org.springframework.data.redis.serializer.RedisSerializer;

/**
 * Executes {@link RedisScript}s
 *
 * @author Jennifer Hickey
 * @param <K> The type of keys that may be passed during script execution
 */
@NullUnmarked
public interface ScriptExecutor<K> {

	/**
	 * Executes the given {@link RedisScript}
	 *
	 * @param script the script to execute.
	 * @param keys any keys that need to be passed to the script.
	 * @param args any args that need to be passed to the script.
	 * @return The return value of the script or {@literal null} if {@link RedisScript#getResultType()} is
	 *         {@literal null}, likely indicating a throw-away status reply (i.e. "OK")
	 */
	<T extends @Nullable Object> T execute(@NonNull RedisScript<T> script, @NonNull List<@NonNull K> keys,
			@NonNull Object @NonNull... args);

	/**
	 * Executes the given {@link RedisScript}, using the provided {@link RedisSerializer}s to serialize the script
	 * arguments and result.
	 *
	 * @param script the script to execute.
	 * @param argsSerializer The {@link RedisSerializer} to use for serializing args.
	 * @param resultSerializer The {@link RedisSerializer} to use for serializing the script return value.
	 * @param keys any keys that need to be passed to the script.
	 * @param args any args that need to be passed to the script.
	 * @return The return value of the script or {@literal null} if {@link RedisScript#getResultType()} is
	 *         {@literal null}, likely indicating a throw-away status reply (i.e. "OK")
	 */
	<T extends @Nullable Object> T execute(@NonNull RedisScript<T> script, @NonNull RedisSerializer<?> argsSerializer,
			@NonNull RedisSerializer<T> resultSerializer, @NonNull List<@NonNull K> keys, @NonNull Object... args);

}
