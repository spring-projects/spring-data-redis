/*
 * Copyright 2019 the original author or authors.
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
package org.springframework.data.redis.core

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactive.asPublisher
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.coroutines.reactive.awaitSingle
import org.springframework.data.redis.connection.DataType
import org.springframework.data.redis.connection.ReactiveRedisConnection
import org.springframework.data.redis.connection.ReactiveSubscription.*
import org.springframework.data.redis.core.script.RedisScript
import org.springframework.data.redis.listener.Topic
import org.springframework.data.redis.serializer.RedisElementReader
import org.springframework.data.redis.serializer.RedisElementWriter
import java.time.Duration
import java.time.Instant

/**
 * Coroutines variant of [ReactiveRedisOperations.execute].
 *
 * @author Sebastien Deleuze
 * @since 2.2
 */
fun <K : Any, V : Any, T : Any> ReactiveRedisOperations<K, V>.executeAsFlow(action: (ReactiveRedisConnection) -> Flow<T>): Flow<T> =
		execute { action(it).asPublisher() }.asFlow()

/**
 * Coroutines variant of [ReactiveRedisOperations.execute].
 *
 * @author Sebastien Deleuze
 * @since 2.2
 */
fun <K : Any, V : Any, T : Any> ReactiveRedisOperations<K, V>.executeAsFlow(script: RedisScript<T>, keys: List<K> = emptyList(), args: List<*> = emptyList<Any>()): Flow<T> =
		execute(script, keys, args).asFlow()

/**
 * Coroutines variant of [ReactiveRedisOperations.execute].
 *
 * @author Sebastien Deleuze
 * @since 2.2
 */
fun <K : Any, V : Any, T : Any> ReactiveRedisOperations<K, V>.executeAsFlow(script: RedisScript<T>, keys: List<K> = emptyList(), args: List<*> = emptyList<Any>(), argsWriter: RedisElementWriter<*>, resultReader: RedisElementReader<T>): Flow<T> =
		execute(script, keys, args, argsWriter, resultReader).asFlow()

/**
 * Coroutines variant of [ReactiveRedisOperations.convertAndSend].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <K : Any, V : Any> ReactiveRedisOperations<K, V>.sendAndAwait(destination: String, message: V): Long =
		convertAndSend(destination, message).awaitSingle()

/**
 * Coroutines variant of [ReactiveRedisOperations.listenToChannel].
 *
 * @author Sebastien Deleuze
 * @since 2.2
 */
fun <K : Any, V : Any> ReactiveRedisOperations<K, V>.listenToChannelAsFlow(vararg channels: String): Flow<Message<String, V>> =
		listenToChannel(*channels).asFlow()

/**
 * Coroutines variant of [ReactiveRedisOperations.listenToPattern].
 *
 * @author Sebastien Deleuze
 * @since 2.2
 */
fun <K : Any, V : Any> ReactiveRedisOperations<K, V>.listenToPatternAsFlow(vararg patterns: String): Flow<Message<String, V>> =
		listenToPattern(*patterns).asFlow()

/**
 * Coroutines variant of [ReactiveRedisOperations.listenTo].
 *
 * @author Sebastien Deleuze
 * @since 2.2
 */
fun <K : Any, V : Any> ReactiveRedisOperations<K, V>.listenToAsFlow(vararg topics: Topic): Flow<Message<String, V>> =
		listenTo(*topics).asFlow()

/**
 * Coroutines variant of [ReactiveRedisOperations.hasKey].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <K : Any, V : Any> ReactiveRedisOperations<K, V>.hasKeyAndAwait(key: K): Boolean =
		hasKey(key).awaitSingle()

/**
 * Coroutines variant of [ReactiveRedisOperations.type].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <K : Any, V : Any> ReactiveRedisOperations<K, V>.typeAndAwait(key: K): DataType =
		type(key).awaitSingle()

/**
 * Coroutines variant of [ReactiveRedisOperations.keys].
 *
 * @author Sebastien Deleuze
 * @since 2.2
 */
fun <K : Any, V : Any> ReactiveRedisOperations<K, V>.keysAsFlow(pattern: K): Flow<K> =
		keys(pattern).asFlow()

/**
 * Coroutines variant of [ReactiveRedisOperations.scan].
 *
 * @author Sebastien Deleuze
 * @since 2.2
 */
fun <K : Any, V : Any> ReactiveRedisOperations<K, V>.scanAsFlow(options: ScanOptions = ScanOptions.NONE): Flow<K> =
		scan(options).asFlow()

/**
 * Coroutines variant of [ReactiveRedisOperations.randomKey].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <K : Any, V : Any> ReactiveRedisOperations<K, V>.randomKeyAndAwait(): K? =
		randomKey().awaitFirstOrNull()

/**
 * Coroutines variant of [ReactiveRedisOperations.rename].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <K : Any, V : Any> ReactiveRedisOperations<K, V>.renameAndAwait(oldKey: K, newKey: K): Boolean =
		rename(oldKey, newKey).awaitSingle()

/**
 * Coroutines variant of [ReactiveRedisOperations.renameIfAbsent].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <K : Any, V : Any> ReactiveRedisOperations<K, V>.renameIfAbsentAndAwait(oldKey: K, newKey: K): Boolean =
		renameIfAbsent(oldKey, newKey).awaitSingle()

/**
 * Coroutines variant of [ReactiveRedisOperations.delete].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <K : Any, V : Any> ReactiveRedisOperations<K, V>.deleteAndAwait(vararg key: K): Long =
		delete(*key).awaitSingle()

/**
 * Coroutines variant of [ReactiveRedisOperations.unlink].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <K : Any, V : Any> ReactiveRedisOperations<K, V>.unlinkAndAwait(vararg key: K): Long =
		unlink(*key).awaitSingle()

/**
 * Coroutines variant of [ReactiveRedisOperations.expire].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <K : Any, V : Any> ReactiveRedisOperations<K, V>.expireAndAwait(key: K, timeout: Duration): Boolean =
		expire(key, timeout).awaitSingle()

/**
 * Coroutines variant of [ReactiveRedisOperations.expireAt].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <K : Any, V : Any> ReactiveRedisOperations<K, V>.expireAtAndAwait(key: K, expireAt: Instant): Boolean = expireAt(key, expireAt).awaitSingle()


/**
 * Coroutines variant of [ReactiveRedisOperations.persist].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <K : Any, V : Any> ReactiveRedisOperations<K, V>.persistAndAwait(key: K): Boolean =
		persist(key).awaitSingle()

/**
 * Coroutines variant of [ReactiveRedisOperations.move].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <K : Any, V : Any> ReactiveRedisOperations<K, V>.moveAndAwait(key: K, dbIndex: Int): Boolean = move(key, dbIndex).awaitSingle()

/**
 * Coroutines variant of [ReactiveRedisOperations.getExpire].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <K : Any, V : Any> ReactiveRedisOperations<K, V>.getExpireAndAwait(key: K): Duration? = getExpire(key).awaitFirstOrNull()
