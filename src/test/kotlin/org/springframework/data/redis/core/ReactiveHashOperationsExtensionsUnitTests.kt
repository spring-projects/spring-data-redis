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
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import reactor.core.publisher.Mono

/**
 * Unit tests for [ReactiveHashOperationsExtensions].
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 */
class ReactiveHashOperationsExtensionsUnitTests {

	@Test // DATAREDIS-937
	fun hasKey() {

		val operations = mockk<ReactiveHashOperations<String, String, String>>()
		every { operations.hasKey(any(), any()) } returns Mono.just(true)

		runBlocking {
			assertThat(operations.hasKeyAndAwait("foo", "bar")).isTrue()
		}

		verify {
			operations.hasKey("foo", "bar")
		}
	}

	@Test // DATAREDIS-937
	fun get() {

		val operations = mockk<ReactiveHashOperations<String, String, String>>()
		every { operations.get(any(), any()) } returns Mono.just("baz")

		runBlocking {
			assertThat(operations.getAndAwait("foo", "bar")).isEqualTo("baz")
		}

		verify {
			operations.get("foo", "bar")
		}
	}

	@Test // DATAREDIS-937
	fun `get returning an empty Mono`() {

		val operations = mockk<ReactiveHashOperations<String, String, String>>()
		every { operations.get(any(), any()) } returns Mono.empty()

		runBlocking {
			assertThat(operations.getAndAwait("foo", "bar")).isNull()
		}

		verify {
			operations.get("foo", "bar")
		}
	}

	@Test // DATAREDIS-937
	fun multiGet() {

		val operations = mockk<ReactiveHashOperations<String, String, String>>()
		every { operations.multiGet(any(), any()) } returns Mono.just(listOf("baz1", "baz2"))

		runBlocking {
			assertThat(operations.multiGetAndAwait("foo", "bar", "joe")).isEqualTo(listOf("baz1", "baz2"))
		}

		verify {
			operations.multiGet("foo", listOf("bar", "joe"))
		}
	}

	@Test // DATAREDIS-937
	fun increment() {

		val operations = mockk<ReactiveHashOperations<String, String, String>>()
		every { operations.increment(any(), any(), 1) } returns Mono.just(2)

		runBlocking {
			assertThat(operations.incrementAndAwait("foo", "bar", 1)).isEqualTo(2)
		}

		verify {
			operations.increment("foo", "bar", 1)
		}
	}

	@Test // DATAREDIS-937
	fun incrementDouble() {

		val operations = mockk<ReactiveHashOperations<String, String, String>>()
		every { operations.increment(any(), any(), 1.0) } returns Mono.just(2.0)

		runBlocking {
			assertThat(operations.incrementAndAwait("foo", "bar", 1.0)).isEqualTo(2.0)
		}

		verify {
			operations.increment("foo", "bar", 1.0)
		}
	}

	@Test // DATAREDIS-937
	fun size() {

		val operations = mockk<ReactiveHashOperations<String, String, String>>()
		every { operations.size(any()) } returns Mono.just(1)

		runBlocking {
			assertThat(operations.sizeAndAwait("foo")).isEqualTo(1)
		}

		verify {
			operations.size("foo")
		}
	}

	@Test // DATAREDIS-937
	fun put() {

		val operations = mockk<ReactiveHashOperations<String, String, String>>()
		every { operations.put(any(), any(), any()) } returns Mono.just(true)

		runBlocking {
			assertThat(operations.putAndAwait("foo", "bar", "baz")).isTrue()
		}

		verify {
			operations.put("foo", "bar", "baz")
		}
	}

	@Test // DATAREDIS-937
	fun putAll() {

		val operations = mockk<ReactiveHashOperations<String, String, String>>()
		every { operations.putAll(any(), any()) } returns Mono.just(true)

		runBlocking {
			assertThat(operations.putAllAndAwait("foo", mapOf("bar" to "baz"))).isTrue()
		}

		verify {
			operations.putAll("foo", mapOf("bar" to "baz"))
		}
	}

	@Test // DATAREDIS-937
	fun putIfAbsent() {

		val operations = mockk<ReactiveHashOperations<String, String, String>>()
		every { operations.putIfAbsent(any(), any(), any()) } returns Mono.just(true)

		runBlocking {
			assertThat(operations.putIfAbsentAndAwait("foo", "bar", "baz")).isTrue()
		}

		verify {
			operations.putIfAbsent("foo", "bar", "baz")
		}
	}

	@Test // DATAREDIS-937
	fun remove() {

		val operations = mockk<ReactiveHashOperations<String, String, String>>()
		every { operations.remove(any(), any()) } returns Mono.just(1)

		runBlocking {
			assertThat(operations.removeAndAwait("foo", "bar")).isEqualTo(1)
		}

		verify {
			operations.remove("foo", "bar")
		}
	}

	@Test // DATAREDIS-937
	fun delete() {

		val operations = mockk<ReactiveHashOperations<String, String, String>>()
		every { operations.delete(any()) } returns Mono.just(true)

		runBlocking {
			assertThat(operations.deleteAndAwait("foo")).isTrue()
		}

		verify {
			operations.delete("foo")
		}
	}
}
