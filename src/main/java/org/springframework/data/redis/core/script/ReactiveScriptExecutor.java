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
package org.springframework.data.redis.core.script;

import reactor.core.publisher.Flux;

import java.util.Collections;
import java.util.List;

import org.springframework.data.redis.serializer.RedisElementReader;
import org.springframework.data.redis.serializer.RedisElementWriter;
import org.springframework.data.redis.serializer.RedisSerializer;

/**
 * Executes {@link RedisScript}s using reactive infrastructure.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @param <K> The type of keys that may be passed during script execution
 * @since 2.0
 */
public interface ReactiveScriptExecutor<K> {

	/**
	 * Execute the given {@link RedisScript}
	 *
	 * @param script must not be {@literal null}.
	 * @return the return value of the script or {@link Flux#empty()} if {@link RedisScript#getResultType()} is
	 *         {@literal null}, likely indicating a throw-away status reply (i.e. "OK")
	 */
	default <T> Flux<T> execute(RedisScript<T> script) {
		return execute(script, Collections.emptyList());
	}

	/**
	 * Execute the given {@link RedisScript}
	 *
	 * @param script must not be {@literal null}.
	 * @param keys must not be {@literal null}.
	 * @return the return value of the script or {@link Flux#empty()} if {@link RedisScript#getResultType()} is
	 *         {@literal null}, likely indicating a throw-away status reply (i.e. "OK")
	 */
	default <T> Flux<T> execute(RedisScript<T> script, List<K> keys) {
		return execute(script, keys, Collections.emptyList());
	}

	/**
	 * Executes the given {@link RedisScript}
	 *
	 * @param script The script to execute. Must not be {@literal null}.
	 * @param keys Any keys that need to be passed to the script. Must not be {@literal null}.
	 * @param args Any args that need to be passed to the script. Can be {@literal empty}.
	 * @return The return value of the script or {@link Flux#empty()} if {@link RedisScript#getResultType()} is
	 *         {@literal null}, likely indicating a throw-away status reply (i.e. "OK")
	 */
	<T> Flux<T> execute(RedisScript<T> script, List<K> keys, List<?> args);

	/**
	 * Executes the given {@link RedisScript}, using the provided {@link RedisSerializer}s to serialize the script
	 * arguments and result.
	 *
	 * @param script The script to execute. must not be {@literal null}.
	 * @param keys Any keys that need to be passed to the script
	 * @param args Any args that need to be passed to the script
	 * @param argsWriter The {@link RedisElementWriter} to use for serializing args. Must not be {@literal null}.
	 * @param resultReader The {@link RedisElementReader} to use for serializing the script return value. Must not be
	 *          {@literal null}.
	 * @return The return value of the script or {@link Flux#empty()} if {@link RedisScript#getResultType()} is
	 *         {@literal null}, likely indicating a throw-away status reply (i.e. "OK")
	 */
	<T> Flux<T> execute(RedisScript<T> script, List<K> keys, List<?> args, RedisElementWriter<?> argsWriter,
			RedisElementReader<T> resultReader);
}
