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
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.coroutines.reactive.awaitSingle
import java.time.Duration

/**
 * Coroutines variant of [ReactiveListOperations.range].
 *
 * @author Sebastien Deleuze
 * @since 2.2
 */
fun <K : Any, V : Any> ReactiveListOperations<K, V>.rangeAsFlow(key: K, start: Long, end: Long): Flow<V> =
		range(key, start, end).asFlow()

/**
 * Coroutines variant of [ReactiveListOperations.trim].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <K : Any, V : Any> ReactiveListOperations<K, V>.trimAndAwait(key: K, start: Long, end: Long): Boolean =
		trim(key, start, end).awaitSingle()

/**
 * Coroutines variant of [ReactiveListOperations.size].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <K : Any, V : Any> ReactiveListOperations<K, V>.sizeAndAwait(key: K): Long =
		size(key).awaitSingle()

/**
 * Coroutines variant of [ReactiveListOperations.leftPush].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <K : Any, V : Any> ReactiveListOperations<K, V>.leftPushAndAwait(key: K, value: V): Long =
		leftPush(key, value).awaitSingle()

/**
 * Coroutines variant of [ReactiveListOperations.leftPushAll].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <K : Any, V : Any> ReactiveListOperations<K, V>.leftPushAllAndAwait(key: K, vararg values: V): Long =
		leftPushAll(key, *values).awaitSingle()

/**
 * Coroutines variant of [ReactiveListOperations.leftPushAll].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <K : Any, V : Any> ReactiveListOperations<K, V>.leftPushAllAndAwait(key: K, values: Collection<V>): Long =
		leftPushAll(key, values).awaitSingle()

/**
 * Coroutines variant of [ReactiveListOperations.leftPushIfPresent].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <K : Any, V : Any> ReactiveListOperations<K, V>.leftPushIfPresentAndAwait(key: K, value: V): Long =
		leftPushIfPresent(key, value).awaitSingle()

/**
 * Coroutines variant of [ReactiveListOperations.leftPushIfPresent].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <K : Any, V : Any> ReactiveListOperations<K, V>.leftPushAndAwait(key: K, pivot: V, value: V): Long =
		leftPush(key, pivot, value).awaitSingle()

/**
 * Coroutines variant of [ReactiveListOperations.rightPush].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <K : Any, V : Any> ReactiveListOperations<K, V>.rightPushAndAwait(key: K, value: V): Long =
		rightPush(key, value).awaitSingle()

/**
 * Coroutines variant of [ReactiveListOperations.rightPushAll].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <K : Any, V : Any> ReactiveListOperations<K, V>.rightPushAllAndAwait(key: K, vararg values: V): Long =
		rightPushAll(key, *values).awaitSingle()

/**
 * Coroutines variant of [ReactiveListOperations.rightPushAll].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <K : Any, V : Any> ReactiveListOperations<K, V>.rightPushAllAndAwait(key: K, values: Collection<V>): Long =
		rightPushAll(key, values).awaitSingle()

/**
 * Coroutines variant of [ReactiveListOperations.rightPushIfPresent].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <K : Any, V : Any> ReactiveListOperations<K, V>.rightPushIfPresentAndAwait(key: K, value: V): Long =
		rightPushIfPresent(key, value).awaitSingle()

/**
 * Coroutines variant of [ReactiveListOperations.rightPushIfPresent].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <K : Any, V : Any> ReactiveListOperations<K, V>.rightPushAndAwait(key: K, pivot: V, value: V): Long =
		rightPush(key, pivot, value).awaitSingle()

/**
 * Coroutines variant of [ReactiveListOperations.set].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <K : Any, V : Any> ReactiveListOperations<K, V>.setAndAwait(key: K, index: Long, value: V): Boolean =
		set(key, index, value).awaitSingle()

/**
 * Coroutines variant of [ReactiveListOperations.remove].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <K : Any, V : Any> ReactiveListOperations<K, V>.removeAndAwait(key: K, count: Long, value: V): Long =
		remove(key, count, value).awaitSingle()

/**
 * Coroutines variant of [ReactiveListOperations.index].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <K : Any, V : Any> ReactiveListOperations<K, V>.indexAndAwait(key: K, index: Long): V? =
		index(key, index).awaitFirstOrNull()

/**
 * Coroutines variant of [ReactiveListOperations.leftPop].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <K : Any, V : Any> ReactiveListOperations<K, V>.leftPopAndAwait(key: K): V? =
		leftPop(key).awaitFirstOrNull()

/**
 * Coroutines variant of [ReactiveListOperations.leftPop].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <K : Any, V : Any> ReactiveListOperations<K, V>.leftPopAndAwait(key: K, timeout: Duration): V? =
		leftPop(key, timeout).awaitFirstOrNull()

/**
 * Coroutines variant of [ReactiveListOperations.rightPop].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <K : Any, V : Any> ReactiveListOperations<K, V>.rightPopAndAwait(key: K): V? =
		rightPop(key).awaitFirstOrNull()

/**
 * Coroutines variant of [ReactiveListOperations.rightPop].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <K : Any, V : Any> ReactiveListOperations<K, V>.rightPopAndAwait(key: K, timeout: Duration): V? =
		rightPop(key, timeout).awaitFirstOrNull()

/**
 * Coroutines variant of [ReactiveListOperations.delete].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <K : Any, V : Any> ReactiveListOperations<K, V>.deleteAndAwait(key: K): Boolean =
		delete(key).awaitSingle()
