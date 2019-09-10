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

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import org.reactivestreams.Publisher
import org.springframework.data.domain.Range
import org.springframework.data.redis.connection.RedisZSetCommands.Limit
import org.springframework.data.redis.connection.stream.*
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

/**
 * Unit tests for `ReactiveStreamOperationsExtensions`.
 *
 * @author Mark Paluch
 */
class ReactiveStreamOperationsExtensionsUnitTests {

	@Test // DATAREDIS-937
	fun acknowledge() {

		val operations = mockk<ReactiveStreamOperations<String, String, String>>()
		every { operations.acknowledge("foo", "bar", "0-0") } returns Mono.just(1)

		runBlocking {
			assertThat(operations.acknowledgeAndAwait("foo", "bar", "0-0")).isEqualTo(1)
		}

		verify {
			operations.acknowledge("foo", "bar", "0-0")
		}
	}

	@Test // DATAREDIS-937
	fun acknowledgeRecordId() {

		val operations = mockk<ReactiveStreamOperations<String, String, String>>()
		val recordId = RecordId.of("0-0")
		every { operations.acknowledge("foo", "bar", recordId) } returns Mono.just(1)

		runBlocking {
			assertThat(operations.acknowledgeAndAwait("foo", "bar", recordId)).isEqualTo(1)
		}

		verify {
			operations.acknowledge("foo", "bar", recordId)
		}
	}

	@Test // DATAREDIS-937
	fun acknowledgeRecord() {

		val operations = mockk<ReactiveStreamOperations<String, String, String>>()
		every { operations.acknowledge(any(), any<Record<String, String>>()) } returns Mono.just(1)

		runBlocking {
			assertThat(operations.acknowledgeAndAwait("foo", Record.of("bar"))).isEqualTo(1)
		}

		verify {
			operations.acknowledge("foo", Record.of("bar"))
		}
	}

	@Test // DATAREDIS-937
	fun add() {

		val operations = mockk<ReactiveStreamOperations<String, String, String>>()
		val record = MapRecord.create("foo", mapOf("a" to "b"))
		val recordId = RecordId.of("0-0")
		every { operations.add(record) } returns Mono.just(recordId)

		runBlocking {
			assertThat(operations.addAndAwait(record)).isEqualTo(recordId)
		}

		verify {
			operations.add(record)
		}
	}

	@Test
	@ExperimentalCoroutinesApi
	fun `add as Flow`() {

		val map = mapOf("a" to "b")
		val bodyPublisher = Mono.just(map)
		val operations = mockk<ReactiveStreamOperations<String, String, String>>()
		val recordId = RecordId.of("0-0")
		every { operations.add(any(), any<Publisher<Map<String, String>>>()) } returns Flux.just(recordId)

		runBlocking {
			val bodyFlow = flow { emit(map) }
			assertThat(operations.add("foo", bodyFlow).toList()).contains(recordId)
		}

		verify {
			operations.add("foo", any<Publisher<Map<String, String>>>())
		}
	}

	@Test // DATAREDIS-937
	fun addRecord() {

		val operations = mockk<ReactiveStreamOperations<String, String, String>>()
		val record = Record.of<String, String>("foo").withStreamKey("bar")
		val recordId = RecordId.of("0-0")
		every { operations.add(record) } returns Mono.just(recordId)

		runBlocking {
			assertThat(operations.addAndAwait(record)).isEqualTo(recordId)
		}

		verify {
			operations.add(record)
		}
	}

	@Test // DATAREDIS-937
	fun delete() {

		val operations = mockk<ReactiveStreamOperations<String, String, String>>()
		val recordId = RecordId.of("0-0")
		every { operations.delete("foo", recordId) } returns Mono.just(1)

		runBlocking {
			assertThat(operations.deleteAndAwait("foo", recordId)).isEqualTo(1)
		}

		verify {
			operations.delete("foo", recordId)
		}
	}

	@Test // DATAREDIS-937
	fun deleteRecord() {

		val operations = mockk<ReactiveStreamOperations<String, String, String>>()
		val record = Record.of<String, String>("foo").withStreamKey("bar")
		every { operations.delete(record) } returns Mono.just(1)

		runBlocking {
			assertThat(operations.deleteAndAwait(record)).isEqualTo(1)
		}

		verify {
			operations.delete(record)
		}
	}

	@Test // DATAREDIS-937
	fun deleteRecordIds() {

		val operations = mockk<ReactiveStreamOperations<String, String, String>>()
		every { operations.delete("foo", "0-0") } returns Mono.just(1)

		runBlocking {
			assertThat(operations.deleteAndAwait("foo", "0-0")).isEqualTo(1)
		}

		verify {
			operations.delete("foo", "0-0")
		}
	}

	@Test // DATAREDIS-937
	fun createGroup() {

		val operations = mockk<ReactiveStreamOperations<String, String, String>>()
		every { operations.createGroup(any(), any()) } returns Mono.just("OK")

		runBlocking {
			assertThat(operations.createGroupAndAwait("foo", "bar")).isEqualTo("OK")
		}

		verify {
			operations.createGroup("foo", "bar")
		}
	}

	@Test // DATAREDIS-937
	fun createGroupWithOffset() {

		val operations = mockk<ReactiveStreamOperations<String, String, String>>()
		every { operations.createGroup(any(), ReadOffset.lastConsumed(), any()) } returns Mono.just("OK")

		runBlocking {
			assertThat(operations.createGroupAndAwait("foo", ReadOffset.lastConsumed(), "bar")).isEqualTo("OK")
		}

		verify {
			operations.createGroup("foo", ReadOffset.lastConsumed(), "bar")
		}
	}

	@Test // DATAREDIS-937
	fun deleteConsumer() {

		val operations = mockk<ReactiveStreamOperations<String, String, String>>()
		every { operations.deleteConsumer(any(), any()) } returns Mono.just("OK")

		runBlocking {
			assertThat(operations.deleteConsumerAndAwait("foo", Consumer.from("bar", "baz"))).isEqualTo("OK")
		}

		verify {
			operations.deleteConsumer("foo", Consumer.from("bar", "baz"))
		}
	}

	@Test // DATAREDIS-937
	fun destroyGroup() {

		val operations = mockk<ReactiveStreamOperations<String, String, String>>()
		every { operations.destroyGroup(any(), any()) } returns Mono.just("OK")

		runBlocking {
			assertThat(operations.destroyGroupAndAwait("foo", "bar")).isEqualTo("OK")
		}

		verify {
			operations.destroyGroup("foo", "bar")
		}
	}

	@Test // DATAREDIS-937
	fun size() {

		val operations = mockk<ReactiveStreamOperations<String, String, String>>()
		every { operations.size(any()) } returns Mono.just(1)

		runBlocking {
			assertThat(operations.sizeAndAwait("foo")).isEqualTo(1)
		}

		verify {
			operations.size("foo")
		}
	}

	@Test
	@ExperimentalCoroutinesApi
	fun range() {

		val record = MapRecord.create("foo", mapOf("a" to "b"))
		val range = Range.just("bar")
		val operations = mockk<ReactiveStreamOperations<String, String, String>>()
		every { operations.range(any(), any(), any()) } returns Flux.just(record)

		runBlocking {
			assertThat(operations.rangeAsFlow("foo", range).toList()).contains(record)
		}

		verify {
			operations.range("foo", range, Limit.unlimited())
		}
	}

	@Test
	@ExperimentalCoroutinesApi
	fun rangeWithType() {

		val record = ObjectRecord.create("a", "b")
		val range = Range.just("bar")
		val operations = mockk<ReactiveStreamOperations<String, String, String>>()
		every { operations.range(any<Class<*>>(), any(), any(), any()) } returns Flux.just(record)

		runBlocking {
			assertThat(operations.rangeWithTypeAsFlow<String, String>("foo", range).toList()).contains(record)
		}

		verify {
			operations.range(String::class.java, "foo", range, Limit.unlimited())
		}
	}

	@Test
	@ExperimentalCoroutinesApi
	fun `read with StreamOffset vararg`() {
		val offset1 = StreamOffset.create("foo", ReadOffset.lastConsumed())
		val offset2 = StreamOffset.create("bar", ReadOffset.lastConsumed())
		val record = MapRecord.create("foo", mapOf("a" to "b"))
		val operations = mockk<ReactiveStreamOperations<String, String, String>>()
		every { operations.read(offset1, offset2) } returns Flux.just(record)

		runBlocking {
			assertThat(operations.readAsFlow(offset1, offset2).toList()).contains(record)
		}

		verify {
			operations.read(offset1, offset2)
		}
	}

	@Test
	@ExperimentalCoroutinesApi
	fun `read with options and StreamOffset vararg` () {
		val offset1 = StreamOffset.create("foo", ReadOffset.lastConsumed())
		val offset2 = StreamOffset.create("bar", ReadOffset.lastConsumed())
		val options = StreamReadOptions.empty()
		val record = MapRecord.create("foo", mapOf("a" to "b"))
		val operations = mockk<ReactiveStreamOperations<String, String, String>>()
		every { operations.read(options, offset1, offset2) } returns Flux.just(record)

		runBlocking {
			assertThat(operations.readAsFlow(options, offset1, offset2).toList()).contains(record)
		}

		verify {
			operations.read(options, offset1, offset2)
		}
	}

	@Test
	@ExperimentalCoroutinesApi
	fun `read with type and StreamOffset vararg`() {
		val offset1 = StreamOffset.create("foo", ReadOffset.lastConsumed())
		val offset2 = StreamOffset.create("bar", ReadOffset.lastConsumed())
		val record = ObjectRecord.create("a", "b")
		val operations = mockk<ReactiveStreamOperations<String, String, String>>()
		every { operations.read(String::class.java, offset1, offset2) } returns Flux.just(record)

		runBlocking {
			assertThat(operations.readWithTypeAsFlow<String, String>(offset1, offset2).toList()).contains(record)
		}

		verify {
			operations.read(String::class.java, offset1, offset2)
		}
	}

	@Test
	@ExperimentalCoroutinesApi
	fun `read with type, options and StreamOffset vararg` () {
		val offset1 = StreamOffset.create("foo", ReadOffset.lastConsumed())
		val offset2 = StreamOffset.create("bar", ReadOffset.lastConsumed())
		val options = StreamReadOptions.empty()
		val record = ObjectRecord.create("a", "b")
		val operations = mockk<ReactiveStreamOperations<String, String, String>>()
		every { operations.read(String::class.java, options, offset1, offset2) } returns Flux.just(record)

		runBlocking {
			assertThat(operations.readWithTypeAsFlow<String, String>(options, offset1, offset2).toList()).contains(record)
		}

		verify {
			operations.read(String::class.java, options, offset1, offset2)
		}
	}

	@Test
	@ExperimentalCoroutinesApi
	fun `read with consumer and StreamOffset vararg`() {
		val consumer = Consumer.from("a", "b")
		val offset1 = StreamOffset.create("foo", ReadOffset.lastConsumed())
		val offset2 = StreamOffset.create("bar", ReadOffset.lastConsumed())
		val record = MapRecord.create("foo", mapOf("a" to "b"))
		val operations = mockk<ReactiveStreamOperations<String, String, String>>()
		every { operations.read(consumer, offset1, offset2) } returns Flux.just(record)

		runBlocking {
			assertThat(operations.readAsFlow(consumer, offset1, offset2).toList()).contains(record)
		}

		verify {
			operations.read(consumer, offset1, offset2)
		}
	}

	@Test
	@ExperimentalCoroutinesApi
	fun `read with consumer, options and StreamOffset vararg`() {
		val consumer = Consumer.from("a", "b")
		val offset1 = StreamOffset.create("foo", ReadOffset.lastConsumed())
		val offset2 = StreamOffset.create("bar", ReadOffset.lastConsumed())
		val options = StreamReadOptions.empty()
		val record = MapRecord.create("foo", mapOf("a" to "b"))
		val operations = mockk<ReactiveStreamOperations<String, String, String>>()
		every { operations.read(consumer, options, offset1, offset2) } returns Flux.just(record)

		runBlocking {
			assertThat(operations.readAsFlow(consumer, options, offset1, offset2).toList()).contains(record)
		}

		verify {
			operations.read(consumer, options, offset1, offset2)
		}
	}

	@Test
	@ExperimentalCoroutinesApi
	fun `read with type, consumer and StreamOffset vararg`() {
		val consumer = Consumer.from("a", "b")
		val offset1 = StreamOffset.create("foo", ReadOffset.lastConsumed())
		val offset2 = StreamOffset.create("bar", ReadOffset.lastConsumed())
		val record = ObjectRecord.create("a", "b")
		val operations = mockk<ReactiveStreamOperations<String, String, String>>()
		every { operations.read(String::class.java, consumer, offset1, offset2) } returns Flux.just(record)

		runBlocking {
			assertThat(operations.readWithTypeAsFlow<String, String>(consumer, offset1, offset2).toList()).contains(record)
		}

		verify {
			operations.read(String::class.java, consumer, offset1, offset2)
		}
	}

	@Test
	@ExperimentalCoroutinesApi
	fun `read with type, consumer, options and StreamOffset vararg`() {
		val consumer = Consumer.from("a", "b")
		val offset1 = StreamOffset.create("foo", ReadOffset.lastConsumed())
		val offset2 = StreamOffset.create("bar", ReadOffset.lastConsumed())
		val options = StreamReadOptions.empty()
		val record = ObjectRecord.create("a", "b")
		val operations = mockk<ReactiveStreamOperations<String, String, String>>()
		every { operations.read(String::class.java, consumer, options, offset1, offset2) } returns Flux.just(record)

		runBlocking {
			assertThat(operations.readWithTypeAsFlow<String, String>(consumer, options, offset1, offset2).toList()).contains(record)
		}

		verify {
			operations.read(String::class.java, consumer, options, offset1, offset2)
		}
	}

	@Test
	@ExperimentalCoroutinesApi
	fun reverseRange() {

		val record = MapRecord.create("foo", mapOf("a" to "b"))
		val range = Range.just("bar")
		val operations = mockk<ReactiveStreamOperations<String, String, String>>()
		every { operations.reverseRange(any(), any(), any()) } returns Flux.just(record)

		runBlocking {
			assertThat(operations.reverseRangeAsFlow("foo", range).toList()).contains(record)
		}

		verify {
			operations.reverseRange("foo", range, Limit.unlimited())
		}
	}

	@Test
	@ExperimentalCoroutinesApi
	fun reverseRangeWithType() {

		val record = ObjectRecord.create("a", "b")
		val range = Range.just("bar")
		val operations = mockk<ReactiveStreamOperations<String, String, String>>()
		every { operations.reverseRange(any<Class<*>>(), any(), any(), any()) } returns Flux.just(record)

		runBlocking {
			assertThat(operations.reverseRangeWithTypeAsFlow<String, String>("foo", range).toList()).contains(record)
		}

		verify {
			operations.reverseRange(String::class.java, "foo", range, Limit.unlimited())
		}
	}

	@Test // DATAREDIS-937
	fun trim() {

		val operations = mockk<ReactiveStreamOperations<String, String, String>>()
		every { operations.trim(any(), any()) } returns Mono.just(1)

		runBlocking {
			assertThat(operations.trimAndAwait("foo", 1)).isEqualTo(1)
		}

		verify {
			operations.trim("foo", 1)
		}
	}
}
