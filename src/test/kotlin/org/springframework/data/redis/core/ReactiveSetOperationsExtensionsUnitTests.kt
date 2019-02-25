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
import reactor.core.publisher.Mono

/**
 * Unit tests for [ReactiveSetOperationsExtensions].
 *
 * @author Mark Paluch
 */
class ReactiveSetOperationsExtensionsUnitTests {

	@Test // DATAREDIS-937
	fun add() {

		val operations = mockk<ReactiveSetOperations<String, String>>()
		every { operations.add("foo", "bar", "baz") } returns Mono.just(1)

		runBlocking {
			assertThat(operations.addAndAwait("foo", "bar", "baz")).isEqualTo(1)
		}

		verify {
			operations.add("foo", "bar", "baz")
		}
	}

	@Test // DATAREDIS-937
	fun remove() {

		val operations = mockk<ReactiveSetOperations<String, String>>()
		every { operations.remove("foo", "bar", "baz") } returns Mono.just(1)

		runBlocking {
			assertThat(operations.removeAndAwait("foo", "bar", "baz")).isEqualTo(1)
		}

		verify {
			operations.remove("foo", "bar", "baz")
		}
	}

	@Test // DATAREDIS-937
	fun pop() {

		val operations = mockk<ReactiveSetOperations<String, String>>()
		every { operations.pop(any()) } returns Mono.just("bar")

		runBlocking {
			assertThat(operations.popAndAwait("foo")).isEqualTo("bar")
		}

		verify {
			operations.pop("foo")
		}
	}

	@Test // DATAREDIS-937
	fun move() {

		val operations = mockk<ReactiveSetOperations<String, String>>()
		every { operations.move(any(), any(), any()) } returns Mono.just(true)

		runBlocking {
			assertThat(operations.moveAndAwait("foo", "from", "to")).isTrue()
		}

		verify {
			operations.move("foo", "from", "to")
		}
	}

	@Test // DATAREDIS-937
	fun size() {

		val operations = mockk<ReactiveSetOperations<String, String>>()
		every { operations.size(any()) } returns Mono.just(1)

		runBlocking {
			assertThat(operations.sizeAndAwait("foo")).isEqualTo(1)
		}

		verify {
			operations.size("foo")
		}
	}

	@Test // DATAREDIS-937
	fun isMember() {

		val operations = mockk<ReactiveSetOperations<String, String>>()
		every { operations.isMember("foo", "bar") } returns Mono.just(true)

		runBlocking {
			assertThat(operations.isMemberAndAwait("foo", "bar")).isTrue()
		}

		verify {
			operations.isMember("foo", "bar")
		}
	}

	@Test // DATAREDIS-937
	fun intersectAndStore() {

		val operations = mockk<ReactiveSetOperations<String, String>>()
		every { operations.intersectAndStore("foo", "bar", "baz") } returns Mono.just(3)

		runBlocking {
			assertThat(operations.intersectAndStoreAndAwait("foo", "bar", "baz")).isEqualTo(3)
		}

		verify {
			operations.intersectAndStore("foo", "bar", "baz")
		}
	}

	@Test // DATAREDIS-937
	fun intersectAndStoreCollection() {

		val operations = mockk<ReactiveSetOperations<String, String>>()
		every { operations.intersectAndStore(listOf("foo", "bar"), "baz") } returns Mono.just(3)

		runBlocking {
			assertThat(operations.intersectAndStoreAndAwait(listOf("foo", "bar"), "baz")).isEqualTo(3)
		}

		verify {
			operations.intersectAndStore(listOf("foo", "bar"), "baz")
		}
	}

	@Test // DATAREDIS-937
	fun unionAndStore() {

		val operations = mockk<ReactiveSetOperations<String, String>>()
		every { operations.unionAndStore("foo", "bar", "baz") } returns Mono.just(3)

		runBlocking {
			assertThat(operations.unionAndStoreAndAwait("foo", "bar", "baz")).isEqualTo(3)
		}

		verify {
			operations.unionAndStore("foo", "bar", "baz")
		}
	}

	@Test // DATAREDIS-937
	fun unionAndStoreCollection() {

		val operations = mockk<ReactiveSetOperations<String, String>>()
		every { operations.unionAndStore(listOf("foo", "bar"), "baz") } returns Mono.just(3)

		runBlocking {
			assertThat(operations.unionAndStoreAndAwait(listOf("foo", "bar"), "baz")).isEqualTo(3)
		}

		verify {
			operations.unionAndStore(listOf("foo", "bar"), "baz")
		}
	}

	@Test // DATAREDIS-937
	fun differenceAndStore() {

		val operations = mockk<ReactiveSetOperations<String, String>>()
		every { operations.differenceAndStore("foo", "bar", "baz") } returns Mono.just(3)

		runBlocking {
			assertThat(operations.differenceAndStoreAndAwait("foo", "bar", "baz")).isEqualTo(3)
		}

		verify {
			operations.differenceAndStore("foo", "bar", "baz")
		}
	}

	@Test // DATAREDIS-937
	fun differenceAndStoreCollection() {

		val operations = mockk<ReactiveSetOperations<String, String>>()
		every { operations.differenceAndStore(listOf("foo", "bar"), "baz") } returns Mono.just(3)

		runBlocking {
			assertThat(operations.differenceAndStoreAndAwait(listOf("foo", "bar"), "baz")).isEqualTo(3)
		}

		verify {
			operations.differenceAndStore(listOf("foo", "bar"), "baz")
		}
	}

	@Test // DATAREDIS-937
	fun randomMember() {

		val operations = mockk<ReactiveSetOperations<String, String>>()
		every { operations.randomMember(any()) } returns Mono.just("bar")

		runBlocking {
			assertThat(operations.randomMemberAndAwait("foo")).isEqualTo("bar")
		}

		verify {
			operations.randomMember("foo")
		}
	}

	@Test // DATAREDIS-937
	fun delete() {

		val operations = mockk<ReactiveSetOperations<String, String>>()
		every { operations.delete(any()) } returns Mono.just(true)

		runBlocking {
			assertThat(operations.deleteAndAwait("foo")).isTrue()
		}

		verify {
			operations.delete("foo")
		}
	}
}
