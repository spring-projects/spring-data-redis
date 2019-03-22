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

/**
 * Coroutines variant of [ReactiveSetOperations.add].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend inline fun <reified K : Any, reified V : Any> ReactiveSetOperations<K, V>.addAndAwait(key: K, vararg values: V): Long =
		add(key, *values).awaitSingle()

/**
 * Coroutines variant of [ReactiveSetOperations.remove].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend inline fun <reified K : Any, reified V : Any> ReactiveSetOperations<K, V>.removeAndAwait(key: K, vararg values: V): Long =
		remove(key, *values).awaitSingle()

/**
 * Coroutines variant of [ReactiveSetOperations.pop].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend inline fun <reified K : Any, reified V : Any> ReactiveSetOperations<K, V>.popAndAwait(key: K): V? =
		pop(key).awaitFirstOrNull()

/**
 * Coroutines variant of [ReactiveSetOperations.move].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend inline fun <reified K : Any, reified V : Any> ReactiveSetOperations<K, V>.moveAndAwait(sourceKey: K, value: V, destKey: K): Boolean =
		move(sourceKey, value, destKey).awaitSingle()

/**
 * Coroutines variant of [ReactiveSetOperations.size].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend inline fun <reified K : Any, reified V : Any> ReactiveSetOperations<K, V>.sizeAndAwait(key: K): Long =
		size(key).awaitSingle()

/**
 * Coroutines variant of [ReactiveSetOperations.isMember].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend inline fun <reified K : Any, reified V : Any> ReactiveSetOperations<K, V>.isMemberAndAwait(key: K, value: V): Boolean =
		isMember(key, value).awaitSingle()

/**
 * Coroutines variant of [ReactiveSetOperations.intersectAndStore].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend inline fun <reified K : Any, reified V : Any> ReactiveSetOperations<K, V>.intersectAndStoreAndAwait(key: K, otherKey: K, destKey: K): Long =
		intersectAndStore(key, otherKey, destKey).awaitSingle()

/**
 * Coroutines variant of [ReactiveSetOperations.intersectAndStore].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend inline fun <reified K : Any, reified V : Any> ReactiveSetOperations<K, V>.intersectAndStoreAndAwait(keys: Collection<K>, destKey: K): Long =
		intersectAndStore(keys, destKey).awaitSingle()

/**
 * Coroutines variant of [ReactiveSetOperations.unionAndStore].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend inline fun <reified K : Any, reified V : Any> ReactiveSetOperations<K, V>.unionAndStoreAndAwait(key: K, otherKey: K, destKey: K): Long =
		unionAndStore(key, otherKey, destKey).awaitSingle()

/**
 * Coroutines variant of [ReactiveSetOperations.unionAndStore].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend inline fun <reified K : Any, reified V : Any> ReactiveSetOperations<K, V>.unionAndStoreAndAwait(keys: Collection<K>, destKey: K): Long =
		unionAndStore(keys, destKey).awaitSingle()

/**
 * Coroutines variant of [ReactiveSetOperations.differenceAndStore].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend inline fun <reified K : Any, reified V : Any> ReactiveSetOperations<K, V>.differenceAndStoreAndAwait(key: K, otherKey: K, destKey: K): Long =
		differenceAndStore(key, otherKey, destKey).awaitSingle()

/**
 * Coroutines variant of [ReactiveSetOperations.differenceAndStore].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend inline fun <reified K : Any, reified V : Any> ReactiveSetOperations<K, V>.differenceAndStoreAndAwait(keys: Collection<K>, destKey: K): Long =
		differenceAndStore(keys, destKey).awaitSingle()

/**
 * Coroutines variant of [ReactiveSetOperations.randomMember].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend inline fun <reified K : Any, reified V : Any> ReactiveSetOperations<K, V>.randomMemberAndAwait(key: K): V? =
		randomMember(key).awaitFirstOrNull()

/**
 * Coroutines variant of [ReactiveSetOperations.delete].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend inline fun <reified K : Any, reified V : Any> ReactiveSetOperations<K, V>.deleteAndAwait(key: K): Boolean =
		delete(key).awaitSingle()


