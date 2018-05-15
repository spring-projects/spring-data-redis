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
package org.springframework.data.redis.core;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;

import org.reactivestreams.Publisher;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.connection.ReactiveRedisConnection;
import org.springframework.data.redis.connection.ReactiveRedisConnection.CommandResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.KeyCommand;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.connection.ReactiveSubscription.Message;
import org.springframework.data.redis.core.script.DefaultReactiveScriptExecutor;
import org.springframework.data.redis.core.script.ReactiveScriptExecutor;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.data.redis.listener.ReactiveRedisMessageListenerContainer;
import org.springframework.data.redis.listener.Topic;
import org.springframework.data.redis.serializer.RedisElementReader;
import org.springframework.data.redis.serializer.RedisElementWriter;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * Central abstraction for reactive Redis data access implementing {@link ReactiveRedisOperations}.
 * <p/>
 * Performs automatic serialization/deserialization between the given objects and the underlying binary data in the
 * Redis store.
 * <p/>
 * Note that while the template is generified, it is up to the serializers/deserializers to properly convert the given
 * Objects to and from binary data.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.0
 * @param <K> the Redis key type against which the template works (usually a String)
 * @param <V> the Redis value type against which the template works
 */
public class ReactiveRedisTemplate<K, V> implements ReactiveRedisOperations<K, V> {

	private final ReactiveRedisConnectionFactory connectionFactory;
	private final RedisSerializationContext<K, V> serializationContext;
	private final boolean exposeConnection;
	private final ReactiveScriptExecutor<K> reactiveScriptExecutor;

	/**
	 * Creates new {@link ReactiveRedisTemplate} using given {@link ReactiveRedisConnectionFactory} and
	 * {@link RedisSerializationContext}.
	 *
	 * @param connectionFactory must not be {@literal null}.
	 * @param serializationContext must not be {@literal null}.
	 */
	public ReactiveRedisTemplate(ReactiveRedisConnectionFactory connectionFactory,
			RedisSerializationContext<K, V> serializationContext) {
		this(connectionFactory, serializationContext, false);
	}

	/**
	 * Creates new {@link ReactiveRedisTemplate} using given {@link ReactiveRedisConnectionFactory} and
	 * {@link RedisSerializationContext}.
	 *
	 * @param connectionFactory must not be {@literal null}.
	 * @param serializationContext must not be {@literal null}.
	 * @param exposeConnection flag indicating to expose the connection used.
	 */
	public ReactiveRedisTemplate(ReactiveRedisConnectionFactory connectionFactory,
			RedisSerializationContext<K, V> serializationContext, boolean exposeConnection) {

		Assert.notNull(connectionFactory, "ConnectionFactory must not be null!");
		Assert.notNull(serializationContext, "SerializationContext must not be null!");

		this.connectionFactory = connectionFactory;
		this.serializationContext = serializationContext;
		this.exposeConnection = exposeConnection;
		this.reactiveScriptExecutor = new DefaultReactiveScriptExecutor<>(connectionFactory, serializationContext);
	}

	/**
	 * Returns the connectionFactory.
	 *
	 * @return Returns the connectionFactory
	 */
	public ReactiveRedisConnectionFactory getConnectionFactory() {
		return connectionFactory;
	}

	// -------------------------------------------------------------------------
	// Execution methods
	// -------------------------------------------------------------------------

	@Override
	public <T> Flux<T> execute(ReactiveRedisCallback<T> action) {
		return execute(action, exposeConnection);
	}

	/**
	 * Executes the given action object within a connection that can be exposed or not. Additionally, the connection can
	 * be pipelined. Note the results of the pipeline are discarded (making it suitable for write-only scenarios).
	 *
	 * @param <T> return type
	 * @param action callback object to execute
	 * @param exposeConnection whether to enforce exposure of the native Redis Connection to callback code
	 * @return object returned by the action
	 */
	public <T> Flux<T> execute(ReactiveRedisCallback<T> action, boolean exposeConnection) {

		Assert.notNull(action, "Callback object must not be null");

		ReactiveRedisConnectionFactory factory = getConnectionFactory();
		ReactiveRedisConnection conn = factory.getReactiveConnection();

		try {

			ReactiveRedisConnection connToUse = preProcessConnection(conn, false);

			ReactiveRedisConnection connToExpose = (exposeConnection ? connToUse : createRedisConnectionProxy(connToUse));
			Publisher<T> result = action.doInRedis(connToExpose);

			return Flux.from(postProcessResult(result, connToUse, false)).doFinally(signalType -> conn.close());
		} catch (RuntimeException e) {
			conn.close();
			throw e;
		}
	}

	/**
	 * Create a reusable Flux for a {@link ReactiveRedisCallback}. Callback is executed within a connection context. The
	 * connection is released outside the callback.
	 *
	 * @param callback must not be {@literal null}
	 * @return a {@link Flux} wrapping the {@link ReactiveRedisCallback}.
	 */
	public <T> Flux<T> createFlux(ReactiveRedisCallback<T> callback) {

		Assert.notNull(callback, "ReactiveRedisCallback must not be null!");

		return Flux.defer(() -> doInConnection(callback, exposeConnection));
	}

	/**
	 * Create a reusable Mono for a {@link ReactiveRedisCallback}. Callback is executed within a connection context. The
	 * connection is released outside the callback.
	 *
	 * @param callback must not be {@literal null}
	 * @return a {@link Mono} wrapping the {@link ReactiveRedisCallback}.
	 */
	public <T> Mono<T> createMono(final ReactiveRedisCallback<T> callback) {

		Assert.notNull(callback, "ReactiveRedisCallback must not be null!");

		return Mono.defer(() -> Mono.from(doInConnection(callback, exposeConnection)));
	}

	/**
	 * Executes the given action object within a connection that can be exposed or not. Additionally, the connection can
	 * be pipelined. Note the results of the pipeline are discarded (making it suitable for write-only scenarios).
	 *
	 * @param <T> return type
	 * @param action callback object to execute
	 * @param exposeConnection whether to enforce exposure of the native Redis Connection to callback code
	 * @return object returned by the action
	 */
	private <T> Publisher<T> doInConnection(ReactiveRedisCallback<T> action, boolean exposeConnection) {

		Assert.notNull(action, "Callback object must not be null");

		ReactiveRedisConnectionFactory factory = getConnectionFactory();
		ReactiveRedisConnection conn = factory.getReactiveConnection();

		ReactiveRedisConnection connToUse = preProcessConnection(conn, false);

		ReactiveRedisConnection connToExpose = (exposeConnection ? connToUse : createRedisConnectionProxy(connToUse));
		Publisher<T> result = action.doInRedis(connToExpose);

		return Flux.from(postProcessResult(result, connToUse, false)).doFinally(signal -> conn.close());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveRedisOperations#convertAndSend(java.lang.String, java.lang.Object)
	 */
	@Override
	public Mono<Long> convertAndSend(String destination, V message) {

		Assert.hasText(destination, "Destination channel must not be empty!");
		Assert.notNull(message, "Message must not be null!");

		return createMono(connection -> connection.pubSubCommands().publish(
				getSerializationContext().getStringSerializationPair().write(destination),
				getSerializationContext().getValueSerializationPair().write(message)));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveRedisOperations#listenTo(org.springframework.data.redis.listener.Topic[])
	 */
	@Override
	public Flux<? extends Message<String, V>> listenTo(Topic... topics) {

		ReactiveRedisMessageListenerContainer container = new ReactiveRedisMessageListenerContainer(getConnectionFactory());

		return container
				.receive(Arrays.asList(topics), getSerializationContext().getStringSerializationPair(),
						getSerializationContext().getValueSerializationPair()) //
				.doFinally((signalType) -> container.destroyLater().subscribeOn(Schedulers.elastic()));
	}

	// -------------------------------------------------------------------------
	// Methods dealing with Redis keys
	// -------------------------------------------------------------------------

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveRedisOperations#hasKey(java.lang.Object)
	 */
	@Override
	public Mono<Boolean> hasKey(K key) {

		Assert.notNull(key, "Key must not be null!");

		return createMono(connection -> connection.keyCommands().exists(rawKey(key)));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveRedisOperations#type(java.lang.Object)
	 */
	@Override
	public Mono<DataType> type(K key) {

		Assert.notNull(key, "Key must not be null!");

		return createMono(connection -> connection.keyCommands().type(rawKey(key)));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveRedisOperations#keys(java.lang.Object)
	 */
	@Override
	public Flux<K> keys(K pattern) {

		Assert.notNull(pattern, "Pattern must not be null!");

		return createFlux(connection -> connection.keyCommands().keys(rawKey(pattern))) //
				.flatMap(Flux::fromIterable) //
				.map(this::readKey);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveRedisOperations#scan(org.springframework.data.redis.core.ScanOptions)
	 */
	@Override
	public Flux<K> scan(ScanOptions options) {

		Assert.notNull(options, "ScanOptions must not be null!");

		return createFlux(connection -> connection.keyCommands().scan(options)) //
				.map(this::readKey);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveRedisOperations#randomKey()
	 */
	@Override
	public Mono<K> randomKey() {
		return createMono(connection -> connection.keyCommands().randomKey()).map(this::readKey);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveRedisOperations#rename(java.lang.Object, java.lang.Object)
	 */
	@Override
	public Mono<Boolean> rename(K oldKey, K newKey) {

		Assert.notNull(oldKey, "Old key must not be null!");
		Assert.notNull(newKey, "New Key must not be null!");

		return createMono(connection -> connection.keyCommands().rename(rawKey(oldKey), rawKey(newKey)));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveRedisOperations#renameIfAbsent(java.lang.Object, java.lang.Object)
	 */
	@Override
	public Mono<Boolean> renameIfAbsent(K oldKey, K newKey) {

		Assert.notNull(oldKey, "Old key must not be null!");
		Assert.notNull(newKey, "New Key must not be null!");

		return createMono(connection -> connection.keyCommands().renameNX(rawKey(oldKey), rawKey(newKey)));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveRedisOperations#delete(java.lang.Object[])
	 */
	@Override
	@SafeVarargs
	public final Mono<Long> delete(K... keys) {

		Assert.notNull(keys, "Keys must not be null!");
		Assert.notEmpty(keys, "Keys must not be empty!");
		Assert.noNullElements(keys, "Keys must not contain null elements!");

		if (keys.length == 1) {
			return createMono(connection -> connection.keyCommands().del(rawKey(keys[0])));
		}

		Mono<List<ByteBuffer>> listOfKeys = Flux.fromArray(keys).map(this::rawKey).collectList();
		return createMono(connection -> listOfKeys.flatMap(rawKeys -> connection.keyCommands().mDel(rawKeys)));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveRedisOperations#delete(org.reactivestreams.Publisher)
	 */
	@Override
	public Mono<Long> delete(Publisher<K> keys) {

		Assert.notNull(keys, "Keys must not be null!");

		return createMono(connection -> connection.keyCommands() //
				.del(Flux.from(keys).map(this::rawKey).map(KeyCommand::new)) //
				.map(CommandResponse::getOutput));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveRedisOperations#unlink(java.lang.Object[])
	 */
	@Override
	@SafeVarargs
	public final Mono<Long> unlink(K... keys) {

		Assert.notNull(keys, "Keys must not be null!");
		Assert.notEmpty(keys, "Keys must not be empty!");
		Assert.noNullElements(keys, "Keys must not contain null elements!");

		if (keys.length == 1) {
			return createMono(connection -> connection.keyCommands().unlink(rawKey(keys[0])));
		}

		Mono<List<ByteBuffer>> listOfKeys = Flux.fromArray(keys).map(this::rawKey).collectList();
		return createMono(connection -> listOfKeys.flatMap(rawKeys -> connection.keyCommands().mUnlink(rawKeys)));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveRedisOperations#unlink(org.reactivestreams.Publisher)
	 */
	@Override
	public Mono<Long> unlink(Publisher<K> keys) {

		Assert.notNull(keys, "Keys must not be null!");

		return createMono(connection -> connection.keyCommands() //
				.unlink(Flux.from(keys).map(this::rawKey).map(KeyCommand::new)) //
				.map(CommandResponse::getOutput));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveRedisOperations#expire(java.lang.Object, java.time.Duration)
	 */
	@Override
	public Mono<Boolean> expire(K key, Duration timeout) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(timeout, "Timeout must not be null!");

		if (timeout.getNano() == 0) {
			return createMono(connection -> connection.keyCommands() //
					.expire(rawKey(key), timeout));
		}

		return createMono(connection -> connection.keyCommands().pExpire(rawKey(key), timeout));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveRedisOperations#expireAt(java.lang.Object, java.time.Instant)
	 */
	@Override
	public Mono<Boolean> expireAt(K key, Instant expireAt) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(expireAt, "Expire at must not be null!");

		if (expireAt.getNano() == 0) {
			return createMono(connection -> connection.keyCommands() //
					.expireAt(rawKey(key), expireAt));
		}

		return createMono(connection -> connection.keyCommands().pExpireAt(rawKey(key), expireAt));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveRedisOperations#persist(java.lang.Object)
	 */
	@Override
	public Mono<Boolean> persist(K key) {

		Assert.notNull(key, "Key must not be null!");

		return createMono(connection -> connection.keyCommands().persist(rawKey(key)));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveRedisOperations#getExpire(java.lang.Object)
	 */
	@Override
	public Mono<Duration> getExpire(K key) {

		Assert.notNull(key, "Key must not be null!");

		return createMono(connection -> connection.keyCommands().pTtl(rawKey(key)).flatMap(expiry -> {

			if (expiry == -1) {
				return Mono.just(Duration.ZERO);
			}

			if (expiry == -2) {
				return Mono.empty();
			}

			return Mono.just(Duration.ofMillis(expiry));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveRedisOperations#move(java.lang.Object, int)
	 */
	@Override
	public Mono<Boolean> move(K key, int dbIndex) {

		Assert.notNull(key, "Key must not be null!");

		return createMono(connection -> connection.keyCommands().move(rawKey(key), dbIndex));
	}

	// -------------------------------------------------------------------------
	// Methods dealing with Redis Lua scripts
	// -------------------------------------------------------------------------

	/*
	 *(non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveRedisOperations#execute(org.springframework.data.redis.core.script.RedisScript, java.util.List, java.util.List)
	 */
	@Override
	public <T> Flux<T> execute(RedisScript<T> script, List<K> keys, List<?> args) {
		return reactiveScriptExecutor.execute(script, keys, args);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveRedisOperations#execute(org.springframework.data.redis.core.script.RedisScript, java.util.List, java.util.List, org.springframework.data.redis.serializer.RedisElementWriter, org.springframework.data.redis.serializer.RedisElementReader)
	 */
	@Override
	public <T> Flux<T> execute(RedisScript<T> script, List<K> keys, List<?> args, RedisElementWriter<?> argsWriter,
			RedisElementReader<T> resultReader) {
		return reactiveScriptExecutor.execute(script, keys, args, argsWriter, resultReader);
	}

	// -------------------------------------------------------------------------
	// Implementation hooks and helper methods
	// -------------------------------------------------------------------------

	/**
	 * Processes the connection (before any settings are executed on it). Default implementation returns the connection as
	 * is.
	 *
	 * @param connection must not be {@literal null}.
	 * @param existingConnection
	 */
	protected ReactiveRedisConnection preProcessConnection(ReactiveRedisConnection connection,
			boolean existingConnection) {
		return connection;
	}

	/**
	 * Processes the result before returning the {@link Publisher}. Default implementation returns the result as is.
	 *
	 * @param result must not be {@literal null}.
	 * @param connection must not be {@literal null}.
	 * @param existingConnection
	 * @return
	 */
	protected <T> Publisher<T> postProcessResult(Publisher<T> result, ReactiveRedisConnection connection,
			boolean existingConnection) {
		return result;
	}

	protected ReactiveRedisConnection createRedisConnectionProxy(ReactiveRedisConnection reactiveRedisConnection) {

		Class<?>[] ifcs = ClassUtils.getAllInterfacesForClass(reactiveRedisConnection.getClass(),
				getClass().getClassLoader());
		return (ReactiveRedisConnection) Proxy.newProxyInstance(reactiveRedisConnection.getClass().getClassLoader(), ifcs,
				new CloseSuppressingInvocationHandler(reactiveRedisConnection));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveRedisOperations#opsForGeo()
	 */
	@Override
	public ReactiveGeoOperations<K, V> opsForGeo() {
		return opsForGeo(serializationContext);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveRedisOperations#opsForGeo(org.springframework.data.redis.serializer.RedisSerializationContext)
	 */
	@Override
	public <K1, V1> ReactiveGeoOperations<K1, V1> opsForGeo(RedisSerializationContext<K1, V1> serializationContext) {
		return new DefaultReactiveGeoOperations<>(this, serializationContext);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveRedisOperations#opsForHash()
	 */
	@Override
	public <HK, HV> ReactiveHashOperations<K, HK, HV> opsForHash() {
		return opsForHash(serializationContext);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveRedisOperations#opsForHash(org.springframework.data.redis.serializer.RedisSerializationContext)
	 */
	@Override
	public <K1, HK, HV> ReactiveHashOperations<K1, HK, HV> opsForHash(
			RedisSerializationContext<K1, ?> serializationContext) {
		return new DefaultReactiveHashOperations<>(this, serializationContext);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveRedisOperations#opsForHyperLogLog()
	 */
	@Override
	public ReactiveHyperLogLogOperations<K, V> opsForHyperLogLog() {
		return opsForHyperLogLog(serializationContext);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveRedisOperations#opsForHyperLogLog(org.springframework.data.redis.serializer.RedisSerializationContext)
	 */
	@Override
	public <K1, V1> ReactiveHyperLogLogOperations<K1, V1> opsForHyperLogLog(
			RedisSerializationContext<K1, V1> serializationContext) {
		return new DefaultReactiveHyperLogLogOperations<>(this, serializationContext);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveRedisOperations#opsForList()
	 */
	@Override
	public ReactiveListOperations<K, V> opsForList() {
		return opsForList(serializationContext);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveRedisOperations#opsForList(org.springframework.data.redis.serializer.RedisSerializationContext)
	 */
	@Override
	public <K1, V1> ReactiveListOperations<K1, V1> opsForList(RedisSerializationContext<K1, V1> serializationContext) {
		return new DefaultReactiveListOperations<>(this, serializationContext);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveRedisOperations#opsForSet()
	 */
	@Override
	public ReactiveSetOperations<K, V> opsForSet() {
		return opsForSet(serializationContext);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveRedisOperations#opsForSet(org.springframework.data.redis.serializer.RedisSerializationContext)
	 */
	@Override
	public <K1, V1> ReactiveSetOperations<K1, V1> opsForSet(RedisSerializationContext<K1, V1> serializationContext) {
		return new DefaultReactiveSetOperations<>(this, serializationContext);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveRedisOperations#opsForValue()
	 */
	@Override
	public ReactiveValueOperations<K, V> opsForValue() {
		return opsForValue(serializationContext);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveRedisOperations#opsForValue(org.springframework.data.redis.serializer.RedisSerializationContext)
	 */
	@Override
	public <K1, V1> ReactiveValueOperations<K1, V1> opsForValue(RedisSerializationContext<K1, V1> serializationContext) {
		return new DefaultReactiveValueOperations<>(this, serializationContext);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveRedisOperations#opsForZSet()
	 */
	@Override
	public ReactiveZSetOperations<K, V> opsForZSet() {
		return opsForZSet(serializationContext);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveRedisOperations#opsForZSet(org.springframework.data.redis.serializer.RedisSerializationContext)
	 */
	@Override
	public <K1, V1> ReactiveZSetOperations<K1, V1> opsForZSet(RedisSerializationContext<K1, V1> serializationContext) {
		return new DefaultReactiveZSetOperations<>(this, serializationContext);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveRedisOperations#serialization()
	 */
	@Override
	public RedisSerializationContext<K, V> getSerializationContext() {
		return serializationContext;
	}

	private ByteBuffer rawKey(K key) {
		return getSerializationContext().getKeySerializationPair().getWriter().write(key);
	}

	private K readKey(ByteBuffer buffer) {
		return getSerializationContext().getKeySerializationPair().getReader().read(buffer);
	}
}
