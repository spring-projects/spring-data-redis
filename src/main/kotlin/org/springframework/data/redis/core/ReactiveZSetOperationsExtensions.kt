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
import org.springframework.data.domain.Range
import org.springframework.data.redis.connection.RedisZSetCommands

/**
 * Coroutines variant of [ReactiveZSetOperations.add].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend inline fun <reified K : Any, reified V : Any> ReactiveZSetOperations<K, V>.addAndAwait(key: K, value: V, score: Double): Boolean =
		add(key, value, score).awaitSingle()

/**
 * Coroutines variant of [ReactiveZSetOperations.addAll].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend inline fun <reified K : Any, reified V : Any> ReactiveZSetOperations<K, V>.addAllAndAwait(key: K, values: Collection<ZSetOperations.TypedTuple<V>>): Long =
		addAll(key, values).awaitSingle()

/**
 * Coroutines variant of [ReactiveZSetOperations.remove].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend inline fun <reified K : Any, reified V : Any> ReactiveZSetOperations<K, V>.removeAndAwait(key: K, vararg values: Any): Long =
		remove(key, *values).awaitSingle()

/**
 * Coroutines variant of [ReactiveZSetOperations.incrementScore].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend inline fun <reified K : Any, reified V : Any> ReactiveZSetOperations<K, V>.incrementScoreAndAwait(key: K, value: V, score: Double): Double =
		incrementScore(key, value, score).awaitSingle()

/**
 * Coroutines variant of [ReactiveZSetOperations.rank].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend inline fun <reified K : Any, reified V : Any> ReactiveZSetOperations<K, V>.rankAndAwait(key: K, value: V): Long? =
		rank(key, value).awaitFirstOrNull()

/**
 * Coroutines variant of [ReactiveZSetOperations.reverseRank].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend inline fun <reified K : Any, reified V : Any> ReactiveZSetOperations<K, V>.reverseRankAndAwait(key: K, value: V): Long? =
		reverseRank(key, value).awaitFirstOrNull()

/**
 * Coroutines variant of [ReactiveZSetOperations.count].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend inline fun <reified K : Any, reified V : Any> ReactiveZSetOperations<K, V>.countAndAwait(key: K, range: Range<Double>): Long =
		count(key, range).awaitSingle()

/**
 * Coroutines variant of [ReactiveZSetOperations.score].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend inline fun <reified K : Any, reified V : Any> ReactiveZSetOperations<K, V>.scoreAndAwait(key: K, value: V): Double? =
		score(key, value).awaitFirstOrNull()

/**
 * Coroutines variant of [ReactiveZSetOperations.removeRange].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend inline fun <reified K : Any, reified V : Any> ReactiveZSetOperations<K, V>.removeRangeAndAwait(key: K, range: Range<Long>): Long =
		removeRange(key, range).awaitSingle()

/**
 * Coroutines variant of [ReactiveZSetOperations.removeRangeByScore].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend inline fun <reified K : Any, reified V : Any> ReactiveZSetOperations<K, V>.removeRangeByScoreAndAwait(key: K, range: Range<Double>): Long =
		removeRangeByScore(key, range).awaitSingle()

/**
 * Coroutines variant of [ReactiveZSetOperations.unionAndStore].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend inline fun <reified K : Any, reified V : Any> ReactiveZSetOperations<K, V>.unionAndStoreAndAwait(key: K, otherKey: K, destKey: K): Long =
		unionAndStore(key, otherKey, destKey).awaitSingle()

/**
 * Coroutines variant of [ReactiveZSetOperations.unionAndStore].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend inline fun <reified K : Any, reified V : Any> ReactiveZSetOperations<K, V>.unionAndStoreAndAwait(key: K, otherKeys: Collection<K>, destKey: K): Long =
		unionAndStore(key, otherKeys, destKey).awaitSingle()

/**
 * Coroutines variant of [ReactiveZSetOperations.unionAndStore].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend inline fun <reified K : Any, reified V : Any> ReactiveZSetOperations<K, V>.unionAndStoreAndAwait(key: K, otherKeys: Collection<K>, destKey: K, aggregate: RedisZSetCommands.Aggregate): Long =
		unionAndStore(key, otherKeys, destKey, aggregate).awaitSingle()

/**
 * Coroutines variant of [ReactiveZSetOperations.unionAndStore].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend inline fun <reified K : Any, reified V : Any> ReactiveZSetOperations<K, V>.unionAndStoreAndAwait(key: K, otherKeys: Collection<K>, destKey: K, aggregate: RedisZSetCommands.Aggregate, weights: RedisZSetCommands.Weights): Long =
		unionAndStore(key, otherKeys, destKey, aggregate, weights).awaitSingle()

/**
 * Coroutines variant of [ReactiveZSetOperations.intersectAndStore].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend inline fun <reified K : Any, reified V : Any> ReactiveZSetOperations<K, V>.intersectAndStoreAndAwait(key: K, otherKey: K, destKey: K): Long =
		intersectAndStore(key, otherKey, destKey).awaitSingle()

/**
 * Coroutines variant of [ReactiveZSetOperations.intersectAndStore].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend inline fun <reified K : Any, reified V : Any> ReactiveZSetOperations<K, V>.intersectAndStoreAndAwait(key: K, otherKeys: Collection<K>, destKey: K): Long =
		intersectAndStore(key, otherKeys, destKey).awaitSingle()

/**
 * Coroutines variant of [ReactiveZSetOperations.intersectAndStore].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend inline fun <reified K : Any, reified V : Any> ReactiveZSetOperations<K, V>.intersectAndStoreAndAwait(key: K, otherKeys: Collection<K>, destKey: K, aggregate: RedisZSetCommands.Aggregate): Long =
		intersectAndStore(key, otherKeys, destKey, aggregate).awaitSingle()

/**
 * Coroutines variant of [ReactiveZSetOperations.intersectAndStore].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend inline fun <reified K : Any, reified V : Any> ReactiveZSetOperations<K, V>.intersectAndStoreAndAwait(key: K, otherKeys: Collection<K>, destKey: K, aggregate: RedisZSetCommands.Aggregate, weights: RedisZSetCommands.Weights): Long =
		intersectAndStore(key, otherKeys, destKey, aggregate, weights).awaitSingle()

/**
 * Coroutines variant of [ReactiveZSetOperations.delete].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend inline fun <reified K : Any, reified V : Any> ReactiveZSetOperations<K, V>.deleteAndAwait(key: K): Boolean =
		delete(key).awaitSingle()
