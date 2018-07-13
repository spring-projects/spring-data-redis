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

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.reactivestreams.Publisher;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.connection.ReactiveSubscription.Message;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.PatternTopic;
import org.springframework.data.redis.listener.Topic;
import org.springframework.data.redis.serializer.RedisElementReader;
import org.springframework.data.redis.serializer.RedisElementWriter;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.util.Assert;

/**
 * Interface that specified a basic set of Redis operations, implemented by {@link ReactiveRedisTemplate}. Not often
 * used but a useful option for extensibility and testability (as it can be easily mocked or stubbed).
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.0
 */
public interface ReactiveRedisOperations<K, V> {

	/**
	 * Executes the given action within a Redis connection. Application exceptions thrown by the action object get
	 * propagated to the caller (can only be unchecked) whenever possible. Redis exceptions are transformed into
	 * appropriate DAO ones. Allows for returning a result object, that is a domain object or a collection of domain
	 * objects. Performs automatic serialization/deserialization for the given objects to and from binary data suitable
	 * for the Redis storage. Note: Callback code is not supposed to handle transactions itself! Use an appropriate
	 * transaction manager. Generally, callback code must not touch any Connection lifecycle methods, like close, to let
	 * the template do its work.
	 *
	 * @param <T> return type
	 * @param action callback object that specifies the Redis action
	 * @return a result object returned by the action or {@link Flux#empty()}.
	 */
	<T> Flux<T> execute(ReactiveRedisCallback<T> action);

	// -------------------------------------------------------------------------
	// Methods dealing with Redis Pub/Sub
	// -------------------------------------------------------------------------

	/**
	 * Publishes the given message to the given channel.
	 *
	 * @param destination the channel to publish to, must not be {@literal null} nor empty.
	 * @param message message to publish. Must not be {@literal null}.
	 * @return the number of clients that received the message
	 * @since 2.1
	 * @see <a href="http://redis.io/commands/publish">Redis Documentation: PUBLISH</a>
	 */
	Mono<Long> convertAndSend(String destination, V message);

	/**
	 * Subscribe to the given Redis {@code channels} and emit {@link Message messages} received for those.
	 *
	 * @param channels must not be {@literal null}.
	 * @return a hot sequence of {@link Message messages}.
	 * @since 2.1
	 */
	default Flux<? extends Message<String, V>> listenToChannel(String... channels) {

		Assert.notNull(channels, "Channels must not be null!");

		return listenTo(Arrays.stream(channels).map(ChannelTopic::of).toArray(ChannelTopic[]::new));
	}

	/**
	 * Subscribe to the Redis channels matching the given {@code pattern} and emit {@link Message messages} received for
	 * those.
	 *
	 * @param patterns must not be {@literal null}.
	 * @return a hot sequence of {@link Message messages}.
	 * @since 2.1
	 */
	default Flux<? extends Message<String, V>> listenToPattern(String... patterns) {

		Assert.notNull(patterns, "Patterns must not be null!");
		return listenTo(Arrays.stream(patterns).map(PatternTopic::of).toArray(PatternTopic[]::new));
	}

	/**
	 * Subscribe to the Redis channels for the given {@link Topic topics} and emit {@link Message messages} received for
	 * those.
	 *
	 * @param topics must not be {@literal null}.
	 * @return a hot sequence of {@link Message messages}.
	 * @since 2.1
	 */
	Flux<? extends Message<String, V>> listenTo(Topic... topics);

	// -------------------------------------------------------------------------
	// Methods dealing with Redis Keys
	// -------------------------------------------------------------------------

	/**
	 * Determine if given {@code key} exists.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/exists">Redis Documentation: EXISTS</a>
	 */
	Mono<Boolean> hasKey(K key);

	/**
	 * Determine the type stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/type">Redis Documentation: TYPE</a>
	 */
	Mono<DataType> type(K key);

	/**
	 * Find all keys matching the given {@code pattern}. <br />
	 * <strong>IMPORTANT:</strong> It is recommended to use {@link #scan()} to iterate over the keyspace as
	 * {@link #keys(Object)} is a
	 * non-interruptible and expensive Redis operation.
	 *
	 * @param pattern must not be {@literal null}.
	 * @return the {@link Flux} emitting matching keys one by one.
	 * @throws IllegalArgumentException in case the pattern is {@literal null}.
	 * @see <a href="http://redis.io/commands/keys">Redis Documentation: KEYS</a>
	 */
	Flux<K> keys(K pattern);

	/**
	 * Use a {@link Flux} to iterate over keys. The resulting {@link Flux} acts as a cursor and issues {@code SCAN}
	 * commands itself as long as the subscriber signals demand.
	 *
	 * @return the {@link Flux} emitting the {@literal keys} one by one or an {@link Flux#empty() empty flux} if none
	 *         exist.
	 * @see <a href="http://redis.io/commands/scan">Redis Documentation: SCAN</a>
	 * @since 2.1
	 */
	default Flux<K> scan() {
		return scan(ScanOptions.NONE);
	}

	/**
	 * Use a {@link Flux} to iterate over keys. The resulting {@link Flux} acts as a cursor and issues {@code SCAN}
	 * commands itself as long as the subscriber signals demand.
	 *
	 * @param options must not be {@literal null}. Use {@link ScanOptions#NONE} instead.
	 * @return the {@link Flux} emitting the {@literal keys} one by one or an {@link Flux#empty() empty flux} if none
	 *         exist.
	 * @throws IllegalArgumentException when the given {@code options} is {@literal null}.
	 * @see <a href="http://redis.io/commands/scan">Redis Documentation: SCAN</a>
	 * @since 2.1
	 */
	Flux<K> scan(ScanOptions options);

	/**
	 * Return a random key from the keyspace.
	 *
	 * @return
	 * @see <a href="http://redis.io/commands/randomkey">Redis Documentation: RANDOMKEY</a>
	 */
	Mono<K> randomKey();

	/**
	 * Rename key {@code oldKey} to {@code newKey}.
	 *
	 * @param oldKey must not be {@literal null}.
	 * @param newKey must not be {@literal null}.
	 * @see <a href="http://redis.io/commands/rename">Redis Documentation: RENAME</a>
	 */
	Mono<Boolean> rename(K oldKey, K newKey);

	/**
	 * Rename key {@code oleName} to {@code newKey} only if {@code newKey} does not exist.
	 *
	 * @param oldKey must not be {@literal null}.
	 * @param newKey must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/renamenx">Redis Documentation: RENAMENX</a>
	 */
	Mono<Boolean> renameIfAbsent(K oldKey, K newKey);

	/**
	 * Delete given {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return The number of keys that were removed.
	 * @see <a href="http://redis.io/commands/del">Redis Documentation: DEL</a>
	 */
	Mono<Long> delete(K... key);

	/**
	 * Delete given {@code keys}.
	 *
	 * @param keys must not be {@literal null}.
	 * @return The number of keys that were removed.
	 * @see <a href="http://redis.io/commands/del">Redis Documentation: DEL</a>
	 */
	Mono<Long> delete(Publisher<K> keys);

	/**
	 * Unlink the {@code key} from the keyspace. Unlike with {@link #delete(Object[])} the actual memory reclaiming here
	 * happens asynchronously.
	 *
	 * @param key must not be {@literal null}.
	 * @return The number of keys that were removed. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/unlink">Redis Documentation: UNLINK</a>
	 * @since 2.1
	 */
	Mono<Long> unlink(K... key);

	/**
	 * Unlink the {@code keys} from the keyspace. Unlike with {@link #delete(Publisher)} the actual memory reclaiming here
	 * happens asynchronously.
	 *
	 * @param keys must not be {@literal null}.
	 * @return The number of keys that were removed. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/unlink">Redis Documentation: UNLINK</a>
	 * @since 2.1
	 */
	Mono<Long> unlink(Publisher<K> keys);

	/**
	 * Set time to live for given {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param timeout must not be {@literal null}.
	 * @return
	 */
	Mono<Boolean> expire(K key, Duration timeout);

	/**
	 * Set the expiration for given {@code key} as a {@literal expireAt} timestamp.
	 *
	 * @param key must not be {@literal null}.
	 * @param expireAt must not be {@literal null}.
	 * @return
	 */
	Mono<Boolean> expireAt(K key, Instant expireAt);

	/**
	 * Remove the expiration from given {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/persist">Redis Documentation: PERSIST</a>
	 */
	Mono<Boolean> persist(K key);

	/**
	 * Move given {@code key} to database with {@code index}.
	 *
	 * @param key must not be {@literal null}.
	 * @param dbIndex
	 * @return
	 * @see <a href="http://redis.io/commands/move">Redis Documentation: MOVE</a>
	 */
	Mono<Boolean> move(K key, int dbIndex);

	/**
	 * Get the time to live for {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return the {@link Duration} of the associated key. {@link Duration#ZERO} if no timeout associated or empty
	 *         {@link Mono} if the key does not exist.
	 * @see <a href="http://redis.io/commands/pttl">Redis Documentation: PTTL</a>
	 */
	Mono<Duration> getExpire(K key);

	// -------------------------------------------------------------------------
	// Methods dealing with Redis Lua scripts
	// -------------------------------------------------------------------------

	/**
	 * Executes the given {@link RedisScript}.
	 *
	 * @param script must not be {@literal null}.
	 * @return result value of the script {@link Flux#empty()} if {@link RedisScript#getResultType()} is {@literal null},
	 *         likely indicating a throw-away status reply (i.e. "OK").
	 */
	default <T> Flux<T> execute(RedisScript<T> script) {
		return execute(script, Collections.emptyList());
	}

	/**
	 * Executes the given {@link RedisScript}.
	 *
	 * @param script must not be {@literal null}.
	 * @param keys must not be {@literal null}.
	 * @return result value of the script {@link Flux#empty()} if {@link RedisScript#getResultType()} is {@literal null},
	 *         likely indicating a throw-away status reply (i.e. "OK").
	 */
	default <T> Flux<T> execute(RedisScript<T> script, List<K> keys) {
		return execute(script, keys, Collections.emptyList());
	}

	/**
	 * Executes the given {@link RedisScript}
	 *
	 * @param script The script to execute. Must not be {@literal null}.
	 * @param keys keys that need to be passed to the script. Must not be {@literal null}.
	 * @param args args that need to be passed to the script. Must not be {@literal null}.
	 * @return result value of the script {@link Flux#empty()} if {@link RedisScript#getResultType()} is {@literal null},
	 *         likely indicating a throw-away status reply (i.e. "OK").
	 */
	<T> Flux<T> execute(RedisScript<T> script, List<K> keys, List<?> args);

	/**
	 * Executes the given {@link RedisScript}, using the provided {@link RedisSerializer}s to serialize the script
	 * arguments and result.
	 *
	 * @param script The script to execute
	 * @param argsWriter The {@link RedisElementWriter} to use for serializing args
	 * @param resultReader The {@link RedisElementReader} to use for serializing the script return value
	 * @param keys keys that need to be passed to the script.
	 * @param args args that need to be passed to the script.
	 * @return result value of the script {@link Flux#empty()} if {@link RedisScript#getResultType()} is {@literal null},
	 *         likely indicating a throw-away status reply (i.e. "OK").
	 */
	<T> Flux<T> execute(RedisScript<T> script, List<K> keys, List<?> args, RedisElementWriter<?> argsWriter,
			RedisElementReader<T> resultReader);

	// -------------------------------------------------------------------------
	// Methods to obtain specific operations interface objects.
	// -------------------------------------------------------------------------

	// operation types

	/**
	 * Returns geospatial specific operations interface.
	 *
	 * @return geospatial specific operations.
	 */
	ReactiveGeoOperations<K, V> opsForGeo();

	/**
	 * Returns geospatial specific operations interface.
	 *
	 * @param serializationContext serializers to be used with the returned operations, must not be {@literal null}.
	 * @return geospatial specific operations.
	 */
	<K, V> ReactiveGeoOperations<K, V> opsForGeo(RedisSerializationContext<K, V> serializationContext);

	/**
	 * Returns the operations performed on hash values.
	 *
	 * @param <HK> hash key (or field) type.
	 * @param <HV> hash value type.
	 * @return hash operations.
	 */
	<HK, HV> ReactiveHashOperations<K, HK, HV> opsForHash();

	/**
	 * Returns the operations performed on hash values given a {@link RedisSerializationContext}.
	 *
	 * @param serializationContext serializers to be used with the returned operations, must not be {@literal null}.
	 * @param <HK> hash key (or field) type.
	 * @param <HV> hash value type.
	 * @return hash operations.
	 */
	<K, HK, HV> ReactiveHashOperations<K, HK, HV> opsForHash(RedisSerializationContext<K, ?> serializationContext);

	/**
	 * Returns the operations performed on multisets using HyperLogLog.
	 *
	 * @return never {@literal null}.
	 */
	ReactiveHyperLogLogOperations<K, V> opsForHyperLogLog();

	/**
	 * Returns the operations performed on multisets using HyperLogLog given a {@link RedisSerializationContext}.
	 *
	 * @param serializationContext serializers to be used with the returned operations, must not be {@literal null}.
	 * @return never {@literal null}.
	 */
	<K, V> ReactiveHyperLogLogOperations<K, V> opsForHyperLogLog(RedisSerializationContext<K, V> serializationContext);

	/**
	 * Returns the operations performed on list values.
	 *
	 * @return list operations.
	 */
	ReactiveListOperations<K, V> opsForList();

	/**
	 * Returns the operations performed on list values given a {@link RedisSerializationContext}.
	 *
	 * @param serializationContext serializers to be used with the returned operations, must not be {@literal null}.
	 * @return list operations.
	 */
	<K, V> ReactiveListOperations<K, V> opsForList(RedisSerializationContext<K, V> serializationContext);

	/**
	 * Returns the operations performed on set values.
	 *
	 * @return set operations.
	 */
	ReactiveSetOperations<K, V> opsForSet();

	/**
	 * Returns the operations performed on set values given a {@link RedisSerializationContext}.
	 *
	 * @param serializationContext serializers to be used with the returned operations, must not be {@literal null}.
	 * @return set operations.
	 */
	<K, V> ReactiveSetOperations<K, V> opsForSet(RedisSerializationContext<K, V> serializationContext);

	/**
	 * Returns the operations performed on simple values (or Strings in Redis terminology).
	 *
	 * @return value operations
	 */
	ReactiveValueOperations<K, V> opsForValue();

	/**
	 * Returns the operations performed on simple values (or Strings in Redis terminology) given a
	 * {@link RedisSerializationContext}.
	 *
	 * @param serializationContext serializers to be used with the returned operations, must not be {@literal null}.
	 * @return value operations.
	 */
	<K, V> ReactiveValueOperations<K, V> opsForValue(RedisSerializationContext<K, V> serializationContext);

	/**
	 * Returns the operations performed on zset values (also known as sorted sets).
	 *
	 * @return zset operations.
	 */
	ReactiveZSetOperations<K, V> opsForZSet();

	/**
	 * Returns the operations performed on zset values (also known as sorted sets) given a
	 * {@link RedisSerializationContext}.
	 *
	 * @param serializationContext serializers to be used with the returned operations, must not be {@literal null}.
	 * @return zset operations.
	 */
	<K, V> ReactiveZSetOperations<K, V> opsForZSet(RedisSerializationContext<K, V> serializationContext);

	/**
	 * @return the {@link RedisSerializationContext}.
	 */
	RedisSerializationContext<K, V> getSerializationContext();
}
