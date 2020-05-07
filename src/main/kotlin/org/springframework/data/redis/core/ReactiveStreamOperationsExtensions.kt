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
import kotlinx.coroutines.reactive.awaitSingle
import org.springframework.data.domain.Range
import org.springframework.data.redis.connection.RedisZSetCommands.*
import org.springframework.data.redis.connection.stream.*

/**
 * Coroutines variant of [ReactiveStreamOperations.acknowledge].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <K : Any, HK : Any, HV : Any> ReactiveStreamOperations<K, HK, HV>.acknowledgeAndAwait(key: K, group: String, vararg recordIds: String): Long =
		acknowledge(key, group, *recordIds).awaitSingle()

/**
 * Coroutines variant of [ReactiveStreamOperations.acknowledge].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <K : Any, HK : Any, HV : Any> ReactiveStreamOperations<K, HK, HV>.acknowledgeAndAwait(key: K, group: String, vararg recordIds: RecordId): Long =
		acknowledge(key, group, *recordIds).awaitSingle()

/**
 * Coroutines variant of [ReactiveStreamOperations.acknowledge].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <K : Any, HK : Any, HV : Any> ReactiveStreamOperations<K, HK, HV>.acknowledgeAndAwait(group: String, record: Record<K, *>): Long =
		acknowledge(group, record).awaitSingle()

/**
 * Coroutines variant of [ReactiveStreamOperations.add].
 *
 * @author Sebastien Deleuze
 * @since 2.2
 */
fun <K : Any, HK : Any, HV : Any> ReactiveStreamOperations<K, HK, HV>.add(key: K, bodyFlow: Flow<Map<HK, HV>>): Flow<RecordId> =
		add(key, bodyFlow.asPublisher()).asFlow()

/**
 * Coroutines variant of [ReactiveStreamOperations.add].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <K : Any, HK : Any, HV : Any> ReactiveStreamOperations<K, HK, HV>.addAndAwait(record: MapRecord<K, HK, HV>): RecordId =
		add(record).awaitSingle()

/**
 * Coroutines variant of [ReactiveStreamOperations.add].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <K : Any, HK : Any, HV : Any> ReactiveStreamOperations<K, HK, HV>.addAndAwait(record: Record<K, *>): RecordId =
		add(record).awaitSingle()

/**
 * Coroutines variant of [ReactiveStreamOperations.delete].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <K : Any, HK : Any, HV : Any> ReactiveStreamOperations<K, HK, HV>.deleteAndAwait(key: K, vararg recordIds: String): Long =
		delete(key, *recordIds).awaitSingle()

/**
 * Coroutines variant of [ReactiveStreamOperations.delete].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <K : Any, HK : Any, HV : Any> ReactiveStreamOperations<K, HK, HV>.deleteAndAwait(record: Record<K, *>): Long =
		delete(record).awaitSingle()

/**
 * Coroutines variant of [ReactiveStreamOperations.delete].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <K : Any, HK : Any, HV : Any> ReactiveStreamOperations<K, HK, HV>.deleteAndAwait(key: K, vararg recordIds: RecordId): Long =
		delete(key, *recordIds).awaitSingle()

/**
 * Coroutines variant of [ReactiveStreamOperations.createGroup].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <K : Any, HK : Any, HV : Any> ReactiveStreamOperations<K, HK, HV>.createGroupAndAwait(key: K, group: String): String =
		createGroup(key, group).awaitSingle()

/**
 * Coroutines variant of [ReactiveStreamOperations.createGroup].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <K : Any, HK : Any, HV : Any> ReactiveStreamOperations<K, HK, HV>.createGroupAndAwait(key: K, readOffset: ReadOffset, group: String): String =
		createGroup(key, readOffset, group).awaitSingle()

/**
 * Coroutines variant of [ReactiveStreamOperations.deleteConsumer].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <K : Any, HK : Any, HV : Any> ReactiveStreamOperations<K, HK, HV>.deleteConsumerAndAwait(key: K, consumer: Consumer): String =
		deleteConsumer(key, consumer).awaitSingle()

/**
 * Coroutines variant of [ReactiveStreamOperations.destroyGroup].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <K : Any, HK : Any, HV : Any> ReactiveStreamOperations<K, HK, HV>.destroyGroupAndAwait(key: K, group: String): String =
		destroyGroup(key, group).awaitSingle()

/**
 * Coroutines variant of [ReactiveStreamOperations.size].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <K : Any, HK : Any, HV : Any> ReactiveStreamOperations<K, HK, HV>.sizeAndAwait(key: K): Long =
		size(key).awaitSingle()


/**
 * Coroutines variant of [ReactiveStreamOperations.range].
 *
 * @author Sebastien Deleuze
 * @since 2.2
 */
fun <K : Any, HK : Any, HV : Any> ReactiveStreamOperations<K, HK, HV>.rangeAsFlow(key: K, range: Range<String>, limit: Limit = Limit.unlimited()): Flow<MapRecord<K, HK, HV>>
		= range(key, range, limit).asFlow()

/**
 * Coroutines variant of [ReactiveStreamOperations.range].
 *
 * @author Sebastien Deleuze
 * @since 2.2
 */
inline fun <K : Any, reified V : Any> ReactiveStreamOperations<K, *, *>.rangeWithTypeAsFlow(key: K, range: Range<String>, limit: Limit = Limit.unlimited()): Flow<ObjectRecord<K, V>>
		= range(V::class.java, key, range, limit).asFlow()

/**
 * Coroutines variant of [ReactiveStreamOperations.read].
 *
 * @author Sebastien Deleuze
 * @since 2.2
 */
fun <K : Any, HK : Any, HV : Any> ReactiveStreamOperations<K, HK, HV>.readAsFlow(vararg stream: StreamOffset<K>): Flow<MapRecord<K, HK, HV>> =
		read(*stream).asFlow()

/**
 * Coroutines variant of [ReactiveStreamOperations.read].
 *
 * @author Sebastien Deleuze
 * @since 2.2
 */
fun <K : Any, HK : Any, HV : Any> ReactiveStreamOperations<K, HK, HV>.readAsFlow(readOptions: StreamReadOptions, vararg stream: StreamOffset<K>): Flow<MapRecord<K, HK, HV>> =
		read(readOptions, *stream).asFlow()

/**
 * Coroutines variant of [ReactiveStreamOperations.read].
 *
 * @author Sebastien Deleuze
 * @since 2.2
 */
inline fun <K : Any, reified V : Any> ReactiveStreamOperations<K, *, *>.readWithTypeAsFlow(vararg stream: StreamOffset<K>): Flow<ObjectRecord<K, V>> =
		read(V::class.java, *stream).asFlow()


/**
 * Coroutines variant of [ReactiveStreamOperations.read].
 *
 * @author Sebastien Deleuze
 * @since 2.2
 */
inline fun <K : Any, reified V : Any> ReactiveStreamOperations<K, *, *>.readWithTypeAsFlow(readOptions: StreamReadOptions, vararg stream: StreamOffset<K>): Flow<ObjectRecord<K, V>> =
		read(V::class.java, readOptions, *stream).asFlow()

/**
 * Coroutines variant of [ReactiveStreamOperations.read].
 *
 * @author Sebastien Deleuze
 * @since 2.2
 */
fun <K : Any, HK : Any, HV : Any> ReactiveStreamOperations<K, HK, HV>.readAsFlow(consumer: Consumer, vararg stream: StreamOffset<K>): Flow<MapRecord<K, HK, HV>> =
		read(consumer, *stream).asFlow()

/**
 * Coroutines variant of [ReactiveStreamOperations.read].
 *
 * @author Sebastien Deleuze
 * @since 2.2
 */
fun <K : Any, HK : Any, HV : Any> ReactiveStreamOperations<K, HK, HV>.readAsFlow(consumer: Consumer, readOptions: StreamReadOptions, vararg stream: StreamOffset<K>): Flow<MapRecord<K, HK, HV>> =
		read(consumer, readOptions, *stream).asFlow()

/**
 * Coroutines variant of [ReactiveStreamOperations.read].
 *
 * @author Sebastien Deleuze
 * @since 2.2
 */
inline fun <K : Any, reified V : Any> ReactiveStreamOperations<K, *, *>.readWithTypeAsFlow(consumer: Consumer, vararg stream: StreamOffset<K>): Flow<ObjectRecord<K, V>> =
		read(V::class.java, consumer, *stream).asFlow()

/**
 * Coroutines variant of [ReactiveStreamOperations.read].
 *
 * @author Sebastien Deleuze
 * @since 2.2
 */
inline fun <K : Any, reified V : Any> ReactiveStreamOperations<K, *, *>.readWithTypeAsFlow(consumer: Consumer, readOptions: StreamReadOptions, vararg stream: StreamOffset<K>): Flow<ObjectRecord<K, V>> =
		read(V::class.java, consumer, readOptions, *stream).asFlow()

/**
 * Coroutines variant of [ReactiveStreamOperations.reverseRange].
 *
 * @author Sebastien Deleuze
 * @since 2.2
 */
fun <K : Any, HK : Any, HV : Any> ReactiveStreamOperations<K, HK, HV>.reverseRangeAsFlow(key: K, range: Range<String>, limit: Limit = Limit.unlimited()): Flow<MapRecord<K, HK, HV>>
		= reverseRange(key, range, limit).asFlow()

/**
 * Coroutines variant of [ReactiveStreamOperations.reverseRange].
 *
 * @author Sebastien Deleuze
 * @since 2.2
 */
inline fun <K : Any, reified V : Any> ReactiveStreamOperations<K, *, *>.reverseRangeWithTypeAsFlow(key: K, range: Range<String>, limit: Limit = Limit.unlimited()): Flow<ObjectRecord<K, V>> =
		reverseRange(V::class.java, key, range, limit).asFlow()

/**
 * Coroutines variant of [ReactiveStreamOperations.trim].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <K : Any, HK : Any, HV : Any> ReactiveStreamOperations<K, HK, HV>.trimAndAwait(key: K, count: Long): Long =
		trim(key, count).awaitSingle()
