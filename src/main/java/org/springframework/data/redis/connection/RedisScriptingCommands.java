/*
 * Copyright 2012-2025 the original author or authors.
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
package org.springframework.data.redis.connection;

import java.util.List;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullUnmarked;

/**
 * Scripting commands.
 *
 * @author Costin Leau
 * @author Christoph Strobl
 * @author David Liu
 * @author Mark Paluch
 * @see RedisCommands
 */
@NullUnmarked
public interface RedisScriptingCommands {

	/**
	 * Flush lua script cache.
	 *
	 * @see <a href="https://redis.io/commands/script-flush">Redis Documentation: SCRIPT FLUSH</a>
	 */
	void scriptFlush();

	/**
	 * Kill current lua script execution.
	 *
	 * @see <a href="https://redis.io/commands/script-kill">Redis Documentation: SCRIPT KILL</a>
	 */
	void scriptKill();

	/**
	 * Load lua script into scripts cache, without executing it.<br>
	 * Execute the script by calling {@link #evalSha(byte[], ReturnType, int, byte[]...)}.
	 *
	 * @param script must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/script-load">Redis Documentation: SCRIPT LOAD</a>
	 */
	String scriptLoad(byte @NonNull [] script);

	/**
	 * Check if given {@code scriptShas} exist in script cache.
	 *
	 * @param scriptShas
	 * @return one entry per given scriptSha in returned {@link List} or {@literal null} when used in pipeline /
	 *         transaction.
	 * @see <a href="https://redis.io/commands/script-exists">Redis Documentation: SCRIPT EXISTS</a>
	 */
	List<@NonNull Boolean> scriptExists(@NonNull String @NonNull... scriptShas);

	/**
	 * Evaluate given {@code script}.
	 *
	 * @param script must not be {@literal null}.
	 * @param returnType must not be {@literal null}.
	 * @param numKeys
	 * @param keysAndArgs must not be {@literal null}.
	 * @return script result. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/eval">Redis Documentation: EVAL</a>
	 */
	<T> T eval(byte @NonNull [] script, @NonNull ReturnType returnType, int numKeys,
			byte @NonNull [] @NonNull... keysAndArgs);

	/**
	 * Evaluate given {@code scriptSha}.
	 *
	 * @param scriptSha must not be {@literal null}.
	 * @param returnType must not be {@literal null}.
	 * @param numKeys
	 * @param keysAndArgs must not be {@literal null}.
	 * @return script result. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/evalsha">Redis Documentation: EVALSHA</a>
	 */
	<T> T evalSha(@NonNull String scriptSha, @NonNull ReturnType returnType, int numKeys,
			byte @NonNull [] @NonNull... keysAndArgs);

	/**
	 * Evaluate given {@code scriptSha}.
	 *
	 * @param scriptSha must not be {@literal null}.
	 * @param returnType must not be {@literal null}.
	 * @param numKeys
	 * @param keysAndArgs must not be {@literal null}.
	 * @return script result. {@literal null} when used in pipeline / transaction.
	 * @since 1.5
	 * @see <a href="https://redis.io/commands/evalsha">Redis Documentation: EVALSHA</a>
	 */
	<T> T evalSha(byte @NonNull [] scriptSha, @NonNull ReturnType returnType, int numKeys,
			byte @NonNull [] @NonNull... keysAndArgs);
}
