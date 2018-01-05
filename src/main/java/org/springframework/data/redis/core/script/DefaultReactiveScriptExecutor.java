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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Stream;

import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.ReactiveRedisConnection;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.core.ReactiveRedisCallback;
import org.springframework.data.redis.serializer.RedisElementReader;
import org.springframework.data.redis.serializer.RedisElementWriter;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.data.redis.serializer.RedisSerializationContext.SerializationPair;
import org.springframework.util.Assert;

/**
 * Default implementation of {@link ReactiveScriptExecutor}. Optimizes performance by attempting to execute script first
 * using {@code EVALSHA}, then falling back to {@code EVAL} if Redis has not yet cached the script.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @param <K> The type of keys that may be passed during script execution
 * @since 2.0
 */
public class DefaultReactiveScriptExecutor<K> implements ReactiveScriptExecutor<K> {

	private final ReactiveRedisConnectionFactory connectionFactory;
	private final RedisSerializationContext<K, ?> serializationContext;

	/**
	 * Creates a new {@link DefaultReactiveScriptExecutor} given {@link ReactiveRedisConnectionFactory} and
	 * {@link RedisSerializationContext}.
	 *
	 * @param connectionFactory must not be {@literal null}.
	 * @param serializationContext must not be {@literal null}.
	 */
	public DefaultReactiveScriptExecutor(ReactiveRedisConnectionFactory connectionFactory,
			RedisSerializationContext<K, ?> serializationContext) {

		Assert.notNull(connectionFactory, "ReactiveRedisConnectionFactory must not be null!");
		Assert.notNull(serializationContext, "RedisSerializationContext must not be null!");

		this.connectionFactory = connectionFactory;
		this.serializationContext = serializationContext;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.script.ReactiveScriptExecutor#execute(org.springframework.data.redis.core.script.RedisScript, java.util.List, java.util.List)
	 */
	@Override
	@SuppressWarnings("unchecked")
	public <T> Flux<T> execute(RedisScript<T> script, List<K> keys, List<?> args) {

		Assert.notNull(script, "RedisScript must not be null!");
		Assert.notNull(keys, "Keys must not be null!");
		Assert.notNull(args, "Args must not be null!");

		SerializationPair<?> serializationPair = serializationContext.getValueSerializationPair();

		return execute(script, keys, args, serializationPair.getWriter(),
				(RedisElementReader<T>) serializationPair.getReader());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.script.ReactiveScriptExecutor#execute(org.springframework.data.redis.core.script.RedisScript, java.util.List, java.util.List, org.springframework.data.redis.serializer.RedisElementWriter, org.springframework.data.redis.serializer.RedisElementReader)
	 */
	@Override
	@SuppressWarnings("unchecked")
	public <T> Flux<T> execute(RedisScript<T> script, List<K> keys, List<?> args, RedisElementWriter<?> argsWriter,
			RedisElementReader<T> resultReader) {

		Assert.notNull(script, "RedisScript must not be null!");
		Assert.notNull(argsWriter, "Argument Writer must not be null!");
		Assert.notNull(resultReader, "Result Reader must not be null!");
		Assert.notNull(keys, "Keys must not be null!");
		Assert.notNull(args, "Args must not be null!");

		return execute(connection -> {

			ReturnType returnType = ReturnType.fromJavaType(script.getResultType());
			ByteBuffer[] keysAndArgs = keysAndArgs(argsWriter, keys, args);
			int keySize = keys.size();

			return eval(connection, script, returnType, keySize, keysAndArgs, resultReader);

		});
	}

	protected <T> Flux<T> eval(ReactiveRedisConnection connection, RedisScript<T> script, ReturnType returnType,
			int numKeys, ByteBuffer[] keysAndArgs, RedisElementReader<T> resultReader) {

		Flux<T> result = connection.scriptingCommands().evalSha(script.getSha1(), returnType, numKeys, keysAndArgs);

		result = result.onErrorResume(e -> {

			if (ScriptUtils.exceptionContainsNoScriptError(e)) {
				return connection.scriptingCommands().eval(scriptBytes(script), returnType, numKeys, keysAndArgs);
			}

			return Flux
					.error(e instanceof RuntimeException ? (RuntimeException) e : new RedisSystemException(e.getMessage(), e));
		});

		return script.returnsRawValue() ? result : deserializeResult(resultReader, result);
	}

	@SuppressWarnings("Convert2MethodRef")
	protected ByteBuffer[] keysAndArgs(RedisElementWriter argsWriter, List<K> keys, List<?> args) {

		return Stream.concat(keys.stream().map(t -> keySerializer().getWriter().write(t)),
				args.stream().map(t -> argsWriter.write(t))).toArray(size -> new ByteBuffer[size]);
	}

	/**
	 * @param script
	 * @return
	 */
	protected ByteBuffer scriptBytes(RedisScript<?> script) {
		return serializationContext.getStringSerializationPair().getWriter().write(script.getScriptAsString());
	}

	protected <T> Flux<T> deserializeResult(RedisElementReader<T> reader, Flux<T> result) {
		return result.map(it -> ScriptUtils.deserializeResult(reader, it));
	}

	protected SerializationPair<K> keySerializer() {
		return serializationContext.getKeySerializationPair();
	}

	/**
	 * Executes the given action object within a connection that is allocated eagerly and released after {@link Flux}
	 * termination.
	 *
	 * @param <T> return type
	 * @param action callback object to execute
	 * @return object returned by the action
	 */
	private <T> Flux<T> execute(ReactiveRedisCallback<T> action) {

		Assert.notNull(action, "Callback object must not be null");

		ReactiveRedisConnectionFactory factory = getConnectionFactory();
		ReactiveRedisConnection conn = factory.getReactiveConnection();

		try {
			return Flux.defer(() -> action.doInRedis(conn)).doFinally(signal -> conn.close());
		} catch (RuntimeException e) {

			conn.close();
			throw e;
		}
	}

	public ReactiveRedisConnectionFactory getConnectionFactory() {
		return connectionFactory;
	}
}
