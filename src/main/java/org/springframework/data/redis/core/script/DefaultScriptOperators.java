/*
 * Copyright 2017 the original author or authors.
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

import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.ReactiveRedisConnection;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.core.ReactiveRedisCallback;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.data.redis.serializer.RedisSerializationContext.SerializationPair;
import org.springframework.util.Assert;

/**
 * Default implementation of {@link ScriptOperators}. Optimizes performance by attempting to execute script first using
 * evalsha, then falling back to eval if Redis has not yet cached the script.
 *
 * @author Mark Paluch
 * @param <K> The type of keys that may be passed during script execution
 * @since 2.0
 */
public class DefaultScriptOperators<K> implements ScriptOperators<K> {

	private final ReactiveRedisConnectionFactory connectionFactory;
	private final RedisSerializationContext<K, ?> serializationContext;

	/**
	 * Creates a new {@link DefaultScriptOperators} given {@link ReactiveRedisConnectionFactory} and
	 * {@link RedisSerializationContext}.
	 *
	 * @param connectionFactory must not be {@literal null}.
	 * @param serializationContext must not be {@literal null}.
	 */
	public DefaultScriptOperators(ReactiveRedisConnectionFactory connectionFactory,
			RedisSerializationContext<K, ?> serializationContext) {

		Assert.notNull(connectionFactory, "ReactiveRedisConnectionFactory must not be null!");
		Assert.notNull(serializationContext, "RedisSerializationContext must not be null!");

		this.connectionFactory = connectionFactory;
		this.serializationContext = serializationContext;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.script.ScriptOperators#execute(org.springframework.data.redis.core.script.RedisScript, java.util.List, java.lang.Object[])
	 */
	@Override
	@SuppressWarnings("unchecked")
	public <T> Flux<T> execute(RedisScript<T> script, List<K> keys, Object... args) {

		Assert.notNull(script, "RedisScript must not be null!");
		Assert.notNull(keys, "Keys must not be null!");
		Assert.notNull(args, "Args must not be null!");

		// use the Template's value serializer for args and result
		return execute(script, serializationContext.getKeySerializationPair(),
				(SerializationPair<T>) serializationContext.getValueSerializationPair(), keys, args);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.script.ScriptOperators#execute(org.springframework.data.redis.core.script.RedisScript, org.springframework.data.redis.serializer.RedisSerializationContext.SerializationPair, org.springframework.data.redis.serializer.RedisSerializationContext.SerializationPair, java.util.List, java.lang.Object[])
	 */
	@Override
	@SuppressWarnings("unchecked")
	public <T> Flux<T> execute(RedisScript<T> script, SerializationPair<?> argsSerializerPair,
			SerializationPair<T> resultSerializationPair, List<K> keys, Object... args) {

		Assert.notNull(script, "RedisScript must not be null!");
		Assert.notNull(argsSerializerPair, "Argument SerializationPair must not be null!");
		Assert.notNull(resultSerializationPair, "Result SerializationPair must not be null!");
		Assert.notNull(keys, "Keys must not be null!");
		Assert.notNull(args, "Args must not be null!");

		return execute(connection -> {

			ReturnType returnType = ReturnType.fromJavaType(script.getResultType());
			ByteBuffer[] keysAndArgs = keysAndArgs((SerializationPair<Object>) argsSerializerPair, keys, args);
			int keySize = keys.size();

			return eval(connection, script, returnType, keySize, keysAndArgs, resultSerializationPair);

		});
	}

	protected <T> Flux<T> eval(ReactiveRedisConnection connection, RedisScript<T> script, ReturnType returnType,
			int numKeys, ByteBuffer[] keysAndArgs, SerializationPair<T> resultSerializer) {

		Flux<T> result = connection.scriptingCommands().evalSha(script.getSha1(), returnType, numKeys, keysAndArgs);

		result = result.onErrorResume(e -> {

			if (ScriptUtils.exceptionContainsNoScriptError(e)) {
				return connection.scriptingCommands().eval(scriptBytes(script), returnType, numKeys, keysAndArgs);
			}

			return Flux
					.error(e instanceof RuntimeException ? (RuntimeException) e : new RedisSystemException(e.getMessage(), e));
		});

		return script.getResultType() == null ? result : deserializeResult(resultSerializer, result);
	}

	protected ByteBuffer[] keysAndArgs(SerializationPair<Object> argsSerializer, List<K> keys, Object[] args) {

		int keySize = keys != null ? keys.size() : 0;
		ByteBuffer[] keysAndArgs = new ByteBuffer[args.length + keySize];
		int i = 0;

		if (keys != null) {
			for (K key : keys) {
				if (key instanceof ByteBuffer) {
					keysAndArgs[i++] = (ByteBuffer) key;
				} else {
					keysAndArgs[i++] = keySerializer().getWriter().write(key);
				}
			}
		}

		for (Object arg : args) {
			if (arg instanceof ByteBuffer) {
				keysAndArgs[i++] = (ByteBuffer) arg;
			} else {
				keysAndArgs[i++] = argsSerializer.getWriter().write(arg);
			}
		}
		return keysAndArgs;
	}

	protected ByteBuffer scriptBytes(RedisScript<?> script) {
		return serializationContext.getStringSerializationPair().getWriter().write(script.getScriptAsString());
	}

	protected <T> Flux<T> deserializeResult(SerializationPair<T> pair, Flux<T> result) {
		return result.map(it -> ScriptUtils.deserializeResult(pair.getReader(), it));
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
