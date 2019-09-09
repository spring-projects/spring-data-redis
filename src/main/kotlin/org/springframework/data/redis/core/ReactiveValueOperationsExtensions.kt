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
import org.springframework.data.redis.connection.BitFieldSubCommands
import java.time.Duration

/**
 * Coroutines variant of [ReactiveValueOperations.set].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <K : Any, V : Any> ReactiveValueOperations<K, V>.setAndAwait(key: K, value: V): Boolean =
		set(key, value).awaitSingle()

/**
 * Coroutines variant of [ReactiveValueOperations.set].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <K : Any, V : Any> ReactiveValueOperations<K, V>.setAndAwait(key: K, value: V, timeout: Duration): Boolean =
		set(key, value, timeout).awaitSingle()

/**
 * Coroutines variant of [ReactiveValueOperations.setIfAbsent].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <K : Any, V : Any> ReactiveValueOperations<K, V>.setIfAbsentAndAwait(key: K, value: V): Boolean =
		setIfAbsent(key, value).awaitSingle()

/**
 * Coroutines variant of [ReactiveValueOperations.setIfAbsent].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <K : Any, V : Any> ReactiveValueOperations<K, V>.setIfAbsentAndAwait(key: K, value: V, timeout: Duration): Boolean =
		setIfAbsent(key, value, timeout).awaitSingle()

/**
 * Coroutines variant of [ReactiveValueOperations.setIfPresent].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <K : Any, V : Any> ReactiveValueOperations<K, V>.setIfPresentAndAwait(key: K, value: V): Boolean =
		setIfPresent(key, value).awaitSingle()

/**
 * Coroutines variant of [ReactiveValueOperations.setIfPresent].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <K : Any, V : Any> ReactiveValueOperations<K, V>.setIfPresentAndAwait(key: K, value: V, timeout: Duration): Boolean =
		setIfPresent(key, value, timeout).awaitSingle()

/**
 * Coroutines variant of [ReactiveValueOperations.multiSet].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <K : Any, V : Any> ReactiveValueOperations<K, V>.multiSetAndAwait(map: Map<K, V>): Boolean =
		multiSet(map).awaitSingle()

/**
 * Coroutines variant of [ReactiveValueOperations.multiSetIfAbsent].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <K : Any, V : Any> ReactiveValueOperations<K, V>.multiSetIfAbsentAndAwait(map: Map<K, V>): Boolean =
		multiSetIfAbsent(map).awaitSingle()

/**
 * Coroutines variant of [ReactiveValueOperations.get].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <K : Any, V : Any> ReactiveValueOperations<K, V>.getAndAwait(key: K): V? =
		get(key).awaitFirstOrNull()

/**
 * Coroutines variant of [ReactiveValueOperations.getAndSet].
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.2
 */
suspend fun <K : Any, V : Any> ReactiveValueOperations<K, V>.getAndSetAndAwait(key: K, value: V): V? =
		getAndSet(key, value).awaitFirstOrNull()

/**
 * Coroutines variant of [ReactiveValueOperations.multiGet].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <K : Any, V : Any> ReactiveValueOperations<K, V>.multiGetAndAwait(vararg keys: K): List<V?> =
		multiGet(keys.toCollection(ArrayList())).awaitSingle()

/**
 * Coroutines variant of [ReactiveValueOperations.multiGet].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <K : Any, V : Any> ReactiveValueOperations<K, V>.multiGetAndAwait(keys: Collection<K>): List<V?> =
		multiGet(keys).awaitSingle()

/**
 * Coroutines variant of [ReactiveValueOperations.increment].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <K : Any, V : Any> ReactiveValueOperations<K, V>.incrementAndAwait(key: K): Long =
		increment(key).awaitSingle()

/**
 * Coroutines variant of [ReactiveValueOperations.increment].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <K : Any, V : Any> ReactiveValueOperations<K, V>.incrementAndAwait(key: K, delta: Long): Long =
		increment(key, delta).awaitSingle()

/**
 * Coroutines variant of [ReactiveValueOperations.increment].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <K : Any, V : Any> ReactiveValueOperations<K, V>.incrementAndAwait(key: K, delta: Double): Double =
		increment(key, delta).awaitSingle()

/**
 * Coroutines variant of [ReactiveValueOperations.decrement].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <K : Any, V : Any> ReactiveValueOperations<K, V>.decrementAndAwait(key: K): Long =
		decrement(key).awaitSingle()

/**
 * Coroutines variant of [ReactiveValueOperations.decrement].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <K : Any, V : Any> ReactiveValueOperations<K, V>.decrementAndAwait(key: K, delta: Long): Long =
		decrement(key, delta).awaitSingle()

/**
 * Coroutines variant of [ReactiveValueOperations.append].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <K : Any, V : Any> ReactiveValueOperations<K, V>.appendAndAwait(key: K, value: String): Long =
		append(key, value).awaitSingle()

/**
 * Coroutines variant of [ReactiveValueOperations.get].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <K : Any, V : Any> ReactiveValueOperations<K, V>.getAndAwait(key: K, start: Long, end: Long): String? =
		get(key, start, end).awaitFirstOrNull()

/**
 * Coroutines variant of [ReactiveValueOperations.set].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <K : Any, V : Any> ReactiveValueOperations<K, V>.setAndAwait(key: K, value: V, offset: Long): Long =
		set(key, value, offset).awaitSingle()

/**
 * Coroutines variant of [ReactiveValueOperations.size].
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.2
 */
suspend fun <K : Any, V : Any> ReactiveValueOperations<K, V>.sizeAndAwait(key: K): Long =
		size(key).awaitSingle()

/**
 * Coroutines variant of [ReactiveValueOperations.setBit].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <K : Any, V : Any> ReactiveValueOperations<K, V>.setBitAndAwait(key: K, offset: Long, value: Boolean): Boolean =
		setBit(key, offset, value).awaitSingle()

/**
 * Coroutines variant of [ReactiveValueOperations.getBit].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <K : Any, V : Any> ReactiveValueOperations<K, V>.getBitAndAwait(key: K, offset: Long): Boolean =
		getBit(key, offset).awaitSingle()

/**
 * Coroutines variant of [ReactiveValueOperations.bitField].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <K : Any, V : Any> ReactiveValueOperations<K, V>.bitFieldAndAwait(key: K, commands: BitFieldSubCommands): List<Long?> =
		bitField(key, commands).awaitSingle()

/**
 * Coroutines variant of [ReactiveValueOperations.delete].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <K : Any, V : Any> ReactiveValueOperations<K, V>.deleteAndAwait(key: K): Boolean =
		delete(key).awaitSingle()
