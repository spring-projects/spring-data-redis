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
package org.springframework.data.redis.connection.lettuce;

import io.lettuce.core.api.reactive.RedisScriptingReactiveCommands;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;

import org.springframework.data.redis.connection.ReactiveScriptingCommands;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.util.Assert;

/**
 * {@link ReactiveScriptingCommands} implementation for the <a href="https://lettuce.io/">Lettuce</a> Redis driver.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.0
 */
class LettuceReactiveScriptingCommands implements ReactiveScriptingCommands {

	private static final ByteBuffer[] EMPTY_BUFFER_ARRAY = new ByteBuffer[0];

	private final LettuceReactiveRedisConnection connection;

	/**
	 * Create new {@link LettuceReactiveScriptingCommands}.
	 *
	 * @param connection must not be {@literal null}.
	 */
	LettuceReactiveScriptingCommands(LettuceReactiveRedisConnection connection) {

		Assert.notNull(connection, "Connection must not be null!");

		this.connection = connection;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveScriptingCommands#scriptFlush()
	 */
	@Override
	public Mono<String> scriptFlush() {
		return connection.execute(RedisScriptingReactiveCommands::scriptFlush).next();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveScriptingCommands#scriptKill()
	 */
	@Override
	public Mono<String> scriptKill() {
		return connection.execute(RedisScriptingReactiveCommands::scriptKill).next();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveScriptingCommands#scriptLoad(java.nio.ByteBuffer)
	 */
	@Override
	public Mono<String> scriptLoad(ByteBuffer script) {

		Assert.notNull(script, "Script must not be null!");

		return connection.execute(cmd -> cmd.scriptLoad(script)).next();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveScriptingCommands#scriptExists(java.util.List)
	 */
	@Override
	public Flux<Boolean> scriptExists(List<String> scriptShas) {

		Assert.notEmpty(scriptShas, "Script digests must not be empty!");

		return connection.execute(cmd -> cmd.scriptExists(scriptShas.toArray(new String[scriptShas.size()])));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveScriptingCommands#eval(java.nio.ByteBuffer, org.springframework.data.redis.connection.ReturnType, int, java.nio.ByteBuffer[])
	 */
	@Override
	public <T> Flux<T> eval(ByteBuffer script, ReturnType returnType, int numKeys, ByteBuffer... keysAndArgs) {

		Assert.notNull(script, "Script must not be null!");
		Assert.notNull(returnType, "ReturnType must not be null!");
		Assert.notNull(keysAndArgs, "Keys and args must not be null!");

		ByteBuffer[] keys = extractScriptKeys(numKeys, keysAndArgs);
		ByteBuffer[] args = extractScriptArgs(numKeys, keysAndArgs);

		String scriptBody = Charset.defaultCharset().decode(script).toString();

		return convertIfNecessary(
				connection.execute(cmd -> cmd.eval(scriptBody, LettuceConverters.toScriptOutputType(returnType), keys, args)),
				returnType);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveScriptingCommands#evalSha(java.lang.String, org.springframework.data.redis.connection.ReturnType, int, java.nio.ByteBuffer[])
	 */
	@Override
	public <T> Flux<T> evalSha(String scriptSha, ReturnType returnType, int numKeys, ByteBuffer... keysAndArgs) {

		Assert.notNull(scriptSha, "Script digest must not be null!");
		Assert.notNull(returnType, "ReturnType must not be null!");
		Assert.notNull(keysAndArgs, "Keys and args must not be null!");

		ByteBuffer[] keys = extractScriptKeys(numKeys, keysAndArgs);
		ByteBuffer[] args = extractScriptArgs(numKeys, keysAndArgs);

		return convertIfNecessary(
				connection.execute(cmd -> cmd.evalsha(scriptSha, LettuceConverters.toScriptOutputType(returnType), keys, args)),
				returnType);
	}

	@SuppressWarnings("unchecked")
	private <T> Flux<T> convertIfNecessary(Flux<T> eval, ReturnType returnType) {

		if (returnType == ReturnType.MULTI) {

			return eval.concatMap(t -> {
				return t instanceof Exception ? Flux.error(connection.translateException().apply((Exception) t)) : Flux.just(t);
			});
		}

		return eval;
	}

	private static ByteBuffer[] extractScriptKeys(int numKeys, ByteBuffer... keysAndArgs) {
		return numKeys > 0 ? Arrays.copyOfRange(keysAndArgs, 0, numKeys) : EMPTY_BUFFER_ARRAY;
	}

	private static ByteBuffer[] extractScriptArgs(int numKeys, ByteBuffer... keysAndArgs) {

		return keysAndArgs.length > numKeys ? Arrays.copyOfRange(keysAndArgs, numKeys, keysAndArgs.length)
				: EMPTY_BUFFER_ARRAY;
	}
}
