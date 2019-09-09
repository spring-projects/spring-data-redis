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
 * Coroutines variant of [ReactiveHashOperations.hasKey].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <H : Any, HK : Any, HV : Any> ReactiveHashOperations<H, HK, HV>.hasKeyAndAwait(key: H, hashKey: HK): Boolean =
		hasKey(key, hashKey).awaitSingle()

/**
 * Coroutines variant of [ReactiveHashOperations.get].
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.2
 */
suspend fun <H : Any, HK : Any, HV : Any> ReactiveHashOperations<H, HK, HV>.getAndAwait(key: H, hashKey: HK): HV? =
		get(key, hashKey).awaitFirstOrNull()

/**
 * Coroutines variant of [ReactiveHashOperations.multiGet].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <H : Any, HK : Any, HV : Any> ReactiveHashOperations<H, HK, HV>.multiGetAndAwait(key: H, vararg hashKeys: HK): List<HV?> =
		multiGet(key, hashKeys.toCollection(ArrayList())).awaitSingle()

/**
 * Coroutines variant of [ReactiveHashOperations.increment].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <H : Any, HK : Any, HV : Any> ReactiveHashOperations<H, HK, HV>.incrementAndAwait(key: H, hashKey: HK, delta: Long): Long =
		increment(key, hashKey, delta).awaitSingle()

/**
 * Coroutines variant of [ReactiveHashOperations.increment].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <H : Any, HK : Any, HV : Any> ReactiveHashOperations<H, HK, HV>.incrementAndAwait(key: H, hashKey: HK, delta: Double): Double =
		increment(key, hashKey, delta).awaitSingle()

/**
 * Coroutines variant of [ReactiveHashOperations.size].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <H : Any, HK : Any, HV : Any> ReactiveHashOperations<H, HK, HV>.sizeAndAwait(key: H): Long =
		size(key).awaitSingle()

/**
 * Coroutines variant of [ReactiveHashOperations.putAll].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <H : Any, HK : Any, HV : Any> ReactiveHashOperations<H, HK, HV>.putAllAndAwait(key: H, map: Map<HK, HV>): Boolean =
		putAll(key, map).awaitSingle()

/**
 * Coroutines variant of [ReactiveHashOperations.put].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <H : Any, HK : Any, HV : Any> ReactiveHashOperations<H, HK, HV>.putAndAwait(key: H, hashKey: HK, hashValue: HV): Boolean =
		put(key, hashKey, hashValue).awaitSingle()

/**
 * Coroutines variant of [ReactiveHashOperations.putIfAbsent].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <H : Any, HK : Any, HV : Any> ReactiveHashOperations<H, HK, HV>.putIfAbsentAndAwait(key: H, hashKey: HK, hashValue: HV): Boolean =
		putIfAbsent(key, hashKey, hashValue).awaitSingle()

/**
 * Coroutines variant of [ReactiveHashOperations.remove].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <H : Any, HK : Any, HV : Any> ReactiveHashOperations<H, HK, HV>.removeAndAwait(key: H, vararg hashKeys: Any): Long =
		remove(key, *hashKeys).awaitSingle()

/**
 * Coroutines variant of [ReactiveListOperations.delete].
 *
 * @author Christoph Strobl
 * @since 2.2
 */
suspend fun <H : Any, HK : Any, HV : Any> ReactiveHashOperations<H, HK, HV>.deleteAndAwait(key: H): Boolean =
		delete(key).awaitSingle()
