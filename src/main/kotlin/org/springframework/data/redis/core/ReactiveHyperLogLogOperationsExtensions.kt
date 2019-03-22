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

import kotlinx.coroutines.reactive.awaitSingle

/**
 * Coroutines variant of [ReactiveHyperLogLogOperations.add].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend inline fun <reified K : Any, reified V : Any> ReactiveHyperLogLogOperations<K, V>.addAndAwait(key: K, vararg values: V): Long =
		add(key, *values).awaitSingle()

/**
 * Coroutines variant of [ReactiveHyperLogLogOperations.size].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend inline fun <reified K : Any, reified V : Any> ReactiveHyperLogLogOperations<K, V>.sizeAndAwait(vararg keys: K): Long =
		size(*keys).awaitSingle()

/**
 * Coroutines variant of [ReactiveHyperLogLogOperations.union].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend inline fun <reified K : Any, reified V : Any> ReactiveHyperLogLogOperations<K, V>.unionAndAwait(destination: K, vararg sourceKeys: K): Boolean =
		union(destination, *sourceKeys).awaitSingle()

/**
 * Coroutines variant of [ReactiveHyperLogLogOperations.delete].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend inline fun <reified K : Any, reified V : Any> ReactiveHyperLogLogOperations<K, V>.deleteAndAwait(key: K): Boolean =
		delete(key).awaitSingle()
