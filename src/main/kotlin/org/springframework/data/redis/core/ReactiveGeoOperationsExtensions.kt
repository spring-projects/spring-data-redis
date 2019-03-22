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
import org.springframework.data.geo.Distance
import org.springframework.data.geo.Metric
import org.springframework.data.geo.Point
import org.springframework.data.redis.connection.RedisGeoCommands

/**
 * Coroutines variant of [ReactiveGeoOperations.add].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend inline fun <reified K : Any, reified M : Any> ReactiveGeoOperations<K, M>.addAndAwait(key: K, point: Point, member: M): Long =
		add(key, point, member).awaitSingle()

/**
 * Coroutines variant of [ReactiveGeoOperations.add].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend inline fun <reified K : Any, reified M : Any> ReactiveGeoOperations<K, M>.addAndAwait(key: K, location: RedisGeoCommands.GeoLocation<M>): Long =
		add(key, location).awaitSingle()

/**
 * Coroutines variant of [ReactiveGeoOperations.add].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend inline fun <reified K : Any, reified M : Any> ReactiveGeoOperations<K, M>.addAndAwait(key: K, memberCoordinateMap: Map<M, Point>): Long =
		add(key, memberCoordinateMap).awaitSingle()

/**
 * Coroutines variant of [ReactiveGeoOperations.add].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend inline fun <reified K : Any, reified M : Any> ReactiveGeoOperations<K, M>.addAndAwait(key: K, locations: Iterable<RedisGeoCommands.GeoLocation<M>>): Long =
		add(key, locations).awaitSingle()

/**
 * Coroutines variant of [ReactiveGeoOperations.distance].
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.2
 */
suspend inline fun <reified K : Any, reified M : Any> ReactiveGeoOperations<K, M>.distanceAndAwait(key: K, member1: M, member2: M): Distance? =
		distance(key, member1, member2).awaitFirstOrNull()

/**
 * Coroutines variant of [ReactiveGeoOperations.distance].
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.2
 */
suspend inline fun <reified K : Any, reified M : Any> ReactiveGeoOperations<K, M>.distanceAndAwait(key: K, member1: M, member2: M, metric: Metric): Distance? =
		distance(key, member1, member2, metric).awaitFirstOrNull()

/**
 * Coroutines variant of [ReactiveGeoOperations.hash].
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.2
 */
suspend inline fun <reified K : Any, reified M : Any> ReactiveGeoOperations<K, M>.hashAndAwait(key: K, member: M): String? =
		hash(key, member).awaitFirstOrNull()

/**
 * Coroutines variant of [ReactiveGeoOperations.hash].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend inline fun <reified K : Any, reified M : Any> ReactiveGeoOperations<K, M>.hashAndAwait(key: K, vararg member: M): List<String> =
		hash(key, *member).awaitSingle()

/**
 * Coroutines variant of [ReactiveGeoOperations.position].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend inline fun <reified K : Any, reified M : Any> ReactiveGeoOperations<K, M>.positionAndAwait(key: K, member: M): Point? =
		position(key, member).awaitFirstOrNull()

/**
 * Coroutines variant of [ReactiveGeoOperations.position].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend inline fun <reified K : Any, reified M : Any> ReactiveGeoOperations<K, M>.positionAndAwait(key: K, vararg members: M): List<Point> =
		position(key, *members).awaitSingle()

/**
 * Coroutines variant of [ReactiveGeoOperations.remove].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend inline fun <reified K : Any, reified M : Any> ReactiveGeoOperations<K, M>.removeAndAwait(key: K, vararg member: M): Long =
		remove(key, *member).awaitSingle()

/**
 * Coroutines variant of [ReactiveGeoOperations.delete].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend inline fun <reified K : Any, reified M : Any> ReactiveGeoOperations<K, M>.deleteAndAwait(key: K): Boolean =
		delete(key).awaitSingle()
