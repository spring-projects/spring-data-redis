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

import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.coroutines.reactive.awaitSingle
import org.springframework.data.redis.connection.DataType
import java.time.Duration
import java.time.Instant

/**
 * Coroutines variant of [ReactiveRedisOperations.convertAndSend].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <K : Any, V : Any> ReactiveRedisOperations<K, V>.sendAndAwait(destination: String, message: V): Long =
		convertAndSend(destination, message).awaitSingle()

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
