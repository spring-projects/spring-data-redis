/*
 * Copyright 2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import org.springframework.data.redis.connection.BitFieldSubCommands
import reactor.core.publisher.Mono
import java.time.Duration

/**
 * Unit tests for [ReactiveValueOperationsExtensions].
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 */
class ReactiveValueOperationsExtensionsUnitTests {

	@Test // DATAREDIS-937
	fun set() {

		val operations = mockk<ReactiveValueOperations<String, String>>()
		every { operations.set(any(), any()) } returns Mono.just(true)

		runBlocking {
			assertThat(operations.setAndAwait("foo", "bar")).isTrue()
		}

		verify {
			operations.set("foo", "bar")
		}
	}

	@Test // DATAREDIS-937
	fun setWithDuration() {

		val operations = mockk<ReactiveValueOperations<String, String>>()
		every { operations.set(any(), any(), any<Duration>()) } returns Mono.just(true)

		runBlocking {
			assertThat(operations.setAndAwait("foo", "bar", Duration.ofDays(1))).isTrue()
		}

		verify {
			operations.set("foo", "bar", Duration.ofDays(1))
		}
	}

	@Test // DATAREDIS-937
	fun setIfAbsent() {

		val operations = mockk<ReactiveValueOperations<String, String>>()
		every { operations.setIfAbsent(any(), any()) } returns Mono.just(true)

		runBlocking {
			assertThat(operations.setIfAbsentAndAwait("foo", "bar")).isTrue()
		}

		verify {
			operations.setIfAbsent("foo", "bar")
		}
	}

	@Test // DATAREDIS-937
	fun setIfAbsentWithDuration() {

		val operations = mockk<ReactiveValueOperations<String, String>>()
		every { operations.setIfAbsent(any(), any(), any<Duration>()) } returns Mono.just(true)

		runBlocking {
			assertThat(operations.setIfAbsentAndAwait("foo", "bar", Duration.ofDays(1))).isTrue()
		}

		verify {
			operations.setIfAbsent("foo", "bar", Duration.ofDays(1))
		}
	}

	@Test // DATAREDIS-937
	fun setIfPresent() {

		val operations = mockk<ReactiveValueOperations<String, String>>()
		every { operations.setIfPresent(any(), any()) } returns Mono.just(true)

		runBlocking {
			assertThat(operations.setIfPresentAndAwait("foo", "bar")).isTrue()
		}

		verify {
			operations.setIfPresent("foo", "bar")
		}
	}

	@Test // DATAREDIS-937
	fun setIfPresentWithDuration() {

		val operations = mockk<ReactiveValueOperations<String, String>>()
		every { operations.setIfPresent(any(), any(), any<Duration>()) } returns Mono.just(true)

		runBlocking {
			assertThat(operations.setIfPresentAndAwait("foo", "bar", Duration.ofDays(1))).isTrue()
		}

		verify {
			operations.setIfPresent("foo", "bar", Duration.ofDays(1))
		}
	}

	@Test // DATAREDIS-937
	fun multiSet() {

		val operations = mockk<ReactiveValueOperations<String, String>>()
		every { operations.multiSet(any()) } returns Mono.just(true)

		runBlocking {
			assertThat(operations.multiSetAndAwait(mapOf("foo" to "bar"))).isTrue()
		}

		verify {
			operations.multiSet(mapOf("foo" to "bar"))
		}
	}

	@Test // DATAREDIS-937
	fun multiSetIfAbsent() {

		val operations = mockk<ReactiveValueOperations<String, String>>()
		every { operations.multiSetIfAbsent(any()) } returns Mono.just(true)

		runBlocking {
			assertThat(operations.multiSetIfAbsentAndAwait(mapOf("foo" to "bar"))).isTrue()
		}

		verify {
			operations.multiSetIfAbsent(mapOf("foo" to "bar"))
		}
	}

	@Test // DATAREDIS-937
	fun get() {

		val operations = mockk<ReactiveValueOperations<String, String>>()
		every { operations.get(any()) } returns Mono.just("bar")

		runBlocking {
			assertThat(operations.getAndAwait("foo")).isEqualTo("bar")
		}

		verify {
			operations.get("foo")
		}
	}

	@Test // DATAREDIS-937
	fun `get returning an empty Mono`() {

		val operations = mockk<ReactiveValueOperations<String, String>>()
		every { operations.get(any()) } returns Mono.empty()

		runBlocking {
			assertThat(operations.getAndAwait("foo")).isNull()
		}

		verify {
			operations.get("foo")
		}
	}

	@Test // DATAREDIS-937
	fun getAndSet() {

		val operations = mockk<ReactiveValueOperations<String, String>>()
		every { operations.getAndSet(any(), any()) } returns Mono.just("baz")

		runBlocking {
			assertThat(operations.getAndSetAndAwait("foo", "bar")).isEqualTo("baz")
		}

		verify {
			operations.getAndSet("foo", "bar")
		}
	}

	@Test // DATAREDIS-937
	fun `getAndSet returning an empty Mono`() {

		val operations = mockk<ReactiveValueOperations<String, String>>()
		every { operations.getAndSet(any(), any()) } returns Mono.empty()

		runBlocking {
			assertThat(operations.getAndSetAndAwait("foo", "bar")).isNull()
		}

		verify {
			operations.getAndSet("foo", "bar")
		}
	}

	@Test // DATAREDIS-937
	fun multiGet() {

		val operations = mockk<ReactiveValueOperations<String, String>>()
		every { operations.multiGet(any()) } returns Mono.just(listOf("baz"))

		runBlocking {
			assertThat(operations.multiGetAndAwait("foo", "bar")).isEqualTo(listOf("baz"))
		}

		verify {
			operations.multiGet(listOf("foo", "bar"))
		}
	}

	@Test // DATAREDIS-937
	fun increment() {

		val operations = mockk<ReactiveValueOperations<String, String>>()
		every { operations.increment(any()) } returns Mono.just(1)

		runBlocking {
			assertThat(operations.incrementAndAwait("foo")).isEqualTo(1)
		}

		verify {
			operations.increment("foo")
		}
	}

	@Test // DATAREDIS-937
	fun incrementWithDelta() {

		val operations = mockk<ReactiveValueOperations<String, String>>()
		every { operations.increment(any(), 2) } returns Mono.just(1)

		runBlocking {
			assertThat(operations.incrementAndAwait("foo", 2)).isEqualTo(1)
		}

		verify {
			operations.increment("foo", 2)
		}
	}

	@Test // DATAREDIS-937
	fun incrementWithDoubleDelta() {

		val operations = mockk<ReactiveValueOperations<String, String>>()
		every { operations.increment(any(), 2.0) } returns Mono.just(1.0)

		runBlocking {
			assertThat(operations.incrementAndAwait("foo", 2.0)).isEqualTo(1.0)
		}

		verify {
			operations.increment("foo", 2.0)
		}
	}

	@Test // DATAREDIS-937
	fun decrement() {

		val operations = mockk<ReactiveValueOperations<String, String>>()
		every { operations.decrement(any()) } returns Mono.just(1)

		runBlocking {
			assertThat(operations.decrementAndAwait("foo")).isEqualTo(1)
		}

		verify {
			operations.decrement("foo")
		}
	}

	@Test // DATAREDIS-937
	fun decrementWithDelta() {

		val operations = mockk<ReactiveValueOperations<String, String>>()
		every { operations.decrement(any(), 2) } returns Mono.just(1)

		runBlocking {
			assertThat(operations.decrementAndAwait("foo", 2)).isEqualTo(1)
		}

		verify {
			operations.decrement("foo", 2)
		}
	}

	@Test // DATAREDIS-937
	fun append() {

		val operations = mockk<ReactiveValueOperations<String, String>>()
		every { operations.append(any(), any()) } returns Mono.just(2)

		runBlocking {
			assertThat(operations.appendAndAwait("foo", "bar")).isEqualTo(2)
		}

		verify {
			operations.append("foo", "bar")
		}
	}

	@Test // DATAREDIS-937
	fun getSubstring() {

		val operations = mockk<ReactiveValueOperations<String, String>>()
		every { operations.get(any(), any(), any()) } returns Mono.just("foo")

		runBlocking {
			assertThat(operations.getAndAwait("foo", 1, 2)).isEqualTo("foo")
		}

		verify {
			operations.get("foo", 1, 2)
		}
	}

	@Test // DATAREDIS-937
	fun `getSubstring returning an empty Mono`() {

		val operations = mockk<ReactiveValueOperations<String, String>>()
		every { operations.get(any(), any(), any()) } returns Mono.empty()

		runBlocking {
			assertThat(operations.getAndAwait("foo", 1, 2)).isNull()
		}

		verify {
			operations.get("foo", 1, 2)
		}
	}

	@Test // DATAREDIS-937
	fun setSubstring() {

		val operations = mockk<ReactiveValueOperations<String, String>>()
		every { operations.set(any(), any(), any<Long>()) } returns Mono.just(1)

		runBlocking {
			assertThat(operations.setAndAwait("foo", "bar", 2)).isEqualTo(1)
		}

		verify {
			operations.set("foo", "bar", 2)
		}
	}

	@Test // DATAREDIS-937
	fun size() {

		val operations = mockk<ReactiveValueOperations<String, String>>()
		every { operations.size(any()) } returns Mono.just(1)

		runBlocking {
			assertThat(operations.sizeAndAwait("foo")).isEqualTo(1)
		}

		verify {
			operations.size("foo")
		}
	}

	@Test // DATAREDIS-937
	fun setBit() {

		val operations = mockk<ReactiveValueOperations<String, String>>()
		every { operations.setBit(any(), any(), any()) } returns Mono.just(true)

		runBlocking {
			assertThat(operations.setBitAndAwait("foo", 1, true)).isTrue()
		}

		verify {
			operations.setBit("foo", 1, true)
		}
	}

	@Test // DATAREDIS-937
	fun getBit() {

		val operations = mockk<ReactiveValueOperations<String, String>>()
		every { operations.getBit(any(), any()) } returns Mono.just(true)

		runBlocking {
			assertThat(operations.getBitAndAwait("foo", 1)).isTrue()
		}

		verify {
			operations.getBit("foo", 1)
		}
	}

	@Test // DATAREDIS-937
	fun bitField() {

		val operations = mockk<ReactiveValueOperations<String, String>>()
		val commands = BitFieldSubCommands.create();
		every { operations.bitField(any(), any()) } returns Mono.just(listOf(1L))

		runBlocking {
			assertThat(operations.bitFieldAndAwait("foo", commands)).isEqualTo(listOf(1L))
		}

		verify {
			operations.bitField("foo", commands)
		}
	}

	@Test // DATAREDIS-937
	fun delete() {

		val operations = mockk<ReactiveValueOperations<String, String>>()
		every { operations.delete(any()) } returns Mono.just(true)

		runBlocking {
			assertThat(operations.deleteAndAwait("foo")).isTrue()
		}

		verify {
			operations.delete("foo")
		}
	}
}
