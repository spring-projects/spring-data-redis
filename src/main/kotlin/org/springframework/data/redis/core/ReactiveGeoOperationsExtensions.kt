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

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.coroutines.reactive.awaitSingle
import kotlinx.coroutines.reactor.asFlux
import org.springframework.data.geo.Circle
import org.springframework.data.geo.Distance
import org.springframework.data.geo.GeoResult
import org.springframework.data.geo.Metric
import org.springframework.data.geo.Point
import org.springframework.data.redis.connection.RedisGeoCommands.GeoLocation
import org.springframework.data.redis.connection.RedisGeoCommands.GeoRadiusCommandArgs

/**
 * Coroutines variant of [ReactiveGeoOperations.add].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <K : Any, M : Any> ReactiveGeoOperations<K, M>.addAndAwait(key: K, point: Point, member: M): Long =
		add(key, point, member).awaitSingle()

/**
 * Coroutines variant of [ReactiveGeoOperations.add].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <K : Any, M : Any> ReactiveGeoOperations<K, M>.addAndAwait(key: K, location: GeoLocation<M>): Long =
		add(key, location).awaitSingle()

/**
 * Coroutines variant of [ReactiveGeoOperations.add].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <K : Any, M : Any> ReactiveGeoOperations<K, M>.addAndAwait(key: K, memberCoordinateMap: Map<M, Point>): Long =
		add(key, memberCoordinateMap).awaitSingle()

/**
 * Coroutines variant of [ReactiveGeoOperations.add].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <K : Any, M : Any> ReactiveGeoOperations<K, M>.addAndAwait(key: K, locations: Iterable<GeoLocation<M>>): Long =
		add(key, locations).awaitSingle()

/**
 * Coroutines [Flow] variant of [ReactiveGeoOperations.add].
 *
 * @author Sebastien Deleuze
 * @since 2.2
 */
@ExperimentalCoroutinesApi
fun <K : Any, M : Any> ReactiveGeoOperations<K, M>.add(key: K, locations: Flow<Collection<GeoLocation<M>>>): Flow<Long> =
		add(key, locations.asFlux()).asFlow()

/**
 * Coroutines variant of [ReactiveGeoOperations.distance].
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.2
 */
suspend fun <K : Any, M : Any> ReactiveGeoOperations<K, M>.distanceAndAwait(key: K, member1: M, member2: M): Distance? =
		distance(key, member1, member2).awaitFirstOrNull()

/**
 * Coroutines variant of [ReactiveGeoOperations.distance].
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.2
 */
suspend fun <K : Any, M : Any> ReactiveGeoOperations<K, M>.distanceAndAwait(key: K, member1: M, member2: M, metric: Metric): Distance? =
		distance(key, member1, member2, metric).awaitFirstOrNull()

/**
 * Coroutines variant of [ReactiveGeoOperations.hash].
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.2
 */
suspend fun <K : Any, M : Any> ReactiveGeoOperations<K, M>.hashAndAwait(key: K, member: M): String? =
		hash(key, member).awaitFirstOrNull()

/**
 * Coroutines variant of [ReactiveGeoOperations.hash].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <K : Any, M : Any> ReactiveGeoOperations<K, M>.hashAndAwait(key: K, vararg member: M): List<String> =
		hash(key, *member).awaitSingle()

/**
 * Coroutines variant of [ReactiveGeoOperations.position].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <K : Any, M : Any> ReactiveGeoOperations<K, M>.positionAndAwait(key: K, member: M): Point? =
		position(key, member).awaitFirstOrNull()

/**
 * Coroutines variant of [ReactiveGeoOperations.position].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <K : Any, M : Any> ReactiveGeoOperations<K, M>.positionAndAwait(key: K, vararg members: M): List<Point> =
		position(key, *members).awaitSingle()

/**
 * Coroutines [Flow] variant of [ReactiveGeoOperations.radius].
 *
 * @author Sebastien Deleuze
 * @since 2.2
 */
@ExperimentalCoroutinesApi
fun <K : Any, M : Any> ReactiveGeoOperations<K, M>.radiusAsFlow(key: K, within: Circle, args: GeoRadiusCommandArgs? = null): Flow<GeoResult<GeoLocation<M>>> =
		(if (args != null) radius(key, within, args) else radius(key, within)).asFlow()


/**
 * Coroutines [Flow] variant of [ReactiveGeoOperations.radius].
 *
 * @author Sebastien Deleuze
 * @since 2.2
 */
@ExperimentalCoroutinesApi
fun <K : Any, M : Any> ReactiveGeoOperations<K, M>.radiusAsFlow(key: K, member: M, radius: Double): Flow<GeoResult<GeoLocation<M>>> =
		radius(key, member, radius).asFlow()

/**
 * Coroutines [Flow] variant of [ReactiveGeoOperations.radius].
 *
 * @author Sebastien Deleuze
 * @since 2.2
 */
@ExperimentalCoroutinesApi
fun <K : Any, M : Any> ReactiveGeoOperations<K, M>.radiusAsFlow(key: K, member: M, distance: Distance, args: GeoRadiusCommandArgs? = null): Flow<GeoResult<GeoLocation<M>>> =
		(if (args != null) radius(key, member, distance, args) else radius(key, member, distance)).asFlow()

/**
 * Coroutines variant of [ReactiveGeoOperations.remove].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <K : Any, M : Any> ReactiveGeoOperations<K, M>.removeAndAwait(key: K, vararg member: M): Long =
		remove(key, *member).awaitSingle()

/**
 * Coroutines variant of [ReactiveGeoOperations.delete].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <K : Any, M : Any> ReactiveGeoOperations<K, M>.deleteAndAwait(key: K): Boolean =
		delete(key).awaitSingle()
