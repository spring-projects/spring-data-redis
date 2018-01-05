/*
 * Copyright 2017-2018 the original author or authors.
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

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

import org.springframework.util.Assert;

/**
 * Redis <a href="https://redis.io/commands/#scripting">Scripting</a> commands executed using reactive infrastructure.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.0
 */
public interface ReactiveScriptingCommands {

	/**
	 * Flush lua script cache.
	 *
	 * @see <a href="http://redis.io/commands/script-flush">Redis Documentation: SCRIPT FLUSH</a>
	 */
	Mono<String> scriptFlush();

	/**
	 * Kill current lua script execution.
	 *
	 * @see <a href="http://redis.io/commands/script-kill">Redis Documentation: SCRIPT KILL</a>
	 */
	Mono<String> scriptKill();

	/**
	 * Load lua script into scripts cache, without executing it.<br>
	 * Execute the script by calling {@link #evalSha(String, ReturnType, int, ByteBuffer...)}.
	 *
	 * @param script must not be {@literal null}.
	 * @return never {@literal null}.
	 * @see <a href="http://redis.io/commands/script-load">Redis Documentation: SCRIPT LOAD</a>
	 */
	Mono<String> scriptLoad(ByteBuffer script);

	/**
	 * Check if given {@code scriptSha} exist in script cache.
	 *
	 * @param scriptSha The sha1 of the script is present in script cache. Must not be {@literal null}.
	 * @return a {@link Mono} indicating if script is present.
	 */
	default Mono<Boolean> scriptExists(String scriptSha) {

		Assert.notNull(scriptSha, "ScriptSha must not be null!");
		return scriptExists(Collections.singletonList(scriptSha)).singleOrEmpty();
	}

	/**
	 * Check if given {@code scriptShas} exist in script cache.
	 *
	 * @param scriptShas must not be {@literal null}.
	 * @return {@link Flux} emitting one entry per scriptSha in given {@link List}.
	 * @see <a href="http://redis.io/commands/script-exists">Redis Documentation: SCRIPT EXISTS</a>
	 */
	Flux<Boolean> scriptExists(List<String> scriptShas);

	/**
	 * Evaluate given {@code script}.
	 *
	 * @param script must not be {@literal null}.
	 * @param returnType must not be {@literal null}. Using {@link ReturnType#MULTI} emits a {@link List} as-is instead of
	 *          emitting the individual elements from the array response.
	 * @param numKeys
	 * @param keysAndArgs must not be {@literal null}.
	 * @return never {@literal null}.
	 * @see <a href="http://redis.io/commands/eval">Redis Documentation: EVAL</a>
	 */
	<T> Flux<T> eval(ByteBuffer script, ReturnType returnType, int numKeys, ByteBuffer... keysAndArgs);

	/**
	 * Evaluate given {@code scriptSha}.
	 *
	 * @param scriptSha must not be {@literal null}.
	 * @param returnType must not be {@literal null}. Using {@link ReturnType#MULTI} emits a {@link List} as-is instead of
	 *          emitting the individual elements from the array response.
	 * @param numKeys
	 * @param keysAndArgs must not be {@literal null}.
	 * @return never {@literal null}.
	 * @see <a href="http://redis.io/commands/evalsha">Redis Documentation: EVALSHA</a>
	 */
	<T> Flux<T> evalSha(String scriptSha, ReturnType returnType, int numKeys, ByteBuffer... keysAndArgs);
}
