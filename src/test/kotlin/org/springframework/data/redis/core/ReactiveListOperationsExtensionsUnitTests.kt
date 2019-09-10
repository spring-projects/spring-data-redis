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
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.Duration

/**
 * Unit tests for `ReactiveListOperationsExtensions`
 *
 * @author Mark Paluch
 */
class ReactiveListOperationsExtensionsUnitTests {

	@Test
	@ExperimentalCoroutinesApi
	fun range() {

		val operations = mockk<ReactiveListOperations<String, String>>()
		every { operations.range(any(), any(), any()) } returns Flux.just("foo", "bar")

		runBlocking {
			assertThat(operations.rangeAsFlow("foo", 2, 3).toList()).contains("foo", "bar")
		}

		verify {
			operations.range("foo", 2, 3)
		}
	}

	@Test // DATAREDIS-937
	fun trim() {

		val operations = mockk<ReactiveListOperations<String, String>>()
		every { operations.trim(any(), any(), any()) } returns Mono.just(true)

		runBlocking {
			assertThat(operations.trimAndAwait("foo", 2, 3)).isTrue()
		}

		verify {
			operations.trim("foo", 2, 3)
		}
	}

	@Test // DATAREDIS-937
	fun size() {

		val operations = mockk<ReactiveListOperations<String, String>>()
		every { operations.size(any()) } returns Mono.just(2)

		runBlocking {
			assertThat(operations.sizeAndAwait("foo")).isEqualTo(2)
		}

		verify {
			operations.size("foo")
		}
	}

	@Test // DATAREDIS-937
	fun leftPush() {

		val operations = mockk<ReactiveListOperations<String, String>>()
		every { operations.leftPush(any(), any()) } returns Mono.just(2)

		runBlocking {
			assertThat(operations.leftPushAndAwait("foo", "bar")).isEqualTo(2)
		}

		verify {
			operations.leftPush("foo", "bar")
		}
	}

	@Test // DATAREDIS-937
	fun leftPushAll() {

		val operations = mockk<ReactiveListOperations<String, String>>()
		every { operations.leftPushAll("foo", "bar", "baz") } returns Mono.just(2)

		runBlocking {
			assertThat(operations.leftPushAllAndAwait("foo", "bar", "baz")).isEqualTo(2)
		}

		verify {
			operations.leftPushAll("foo", "bar", "baz")
		}
	}

	@Test // DATAREDIS-937
	fun leftPushAllCollection() {

		val operations = mockk<ReactiveListOperations<String, String>>()
		every { operations.leftPushAll("foo", listOf("bar", "baz")) } returns Mono.just(2)

		runBlocking {
			assertThat(operations.leftPushAllAndAwait("foo", listOf("bar", "baz"))).isEqualTo(2)
		}

		verify {
			operations.leftPushAll("foo", listOf("bar", "baz"))
		}
	}

	@Test // DATAREDIS-937
	fun leftPushIfPresent() {

		val operations = mockk<ReactiveListOperations<String, String>>()
		every { operations.leftPushIfPresent(any(), any()) } returns Mono.just(2)

		runBlocking {
			assertThat(operations.leftPushIfPresentAndAwait("foo", "bar")).isEqualTo(2)
		}

		verify {
			operations.leftPushIfPresent("foo", "bar")
		}
	}

	@Test // DATAREDIS-937
	fun leftPushPivot() {

		val operations = mockk<ReactiveListOperations<String, String>>()
		every { operations.leftPush("foo", "bar", "baz") } returns Mono.just(2)

		runBlocking {
			assertThat(operations.leftPushAndAwait("foo", "bar", "baz")).isEqualTo(2)
		}

		verify {
			operations.leftPush("foo", "bar", "baz")
		}
	}

	@Test // DATAREDIS-937
	fun rightPush() {

		val operations = mockk<ReactiveListOperations<String, String>>()
		every { operations.rightPush(any(), any()) } returns Mono.just(2)

		runBlocking {
			assertThat(operations.rightPushAndAwait("foo", "bar")).isEqualTo(2)
		}

		verify {
			operations.rightPush("foo", "bar")
		}
	}

	@Test // DATAREDIS-937
	fun rightPushAll() {

		val operations = mockk<ReactiveListOperations<String, String>>()
		every { operations.rightPushAll("foo", "bar", "baz") } returns Mono.just(2)

		runBlocking {
			assertThat(operations.rightPushAllAndAwait("foo", "bar", "baz")).isEqualTo(2)
		}

		verify {
			operations.rightPushAll("foo", "bar", "baz")
		}
	}

	@Test // DATAREDIS-937
	fun rightPushAllCollection() {

		val operations = mockk<ReactiveListOperations<String, String>>()
		every { operations.rightPushAll("foo", listOf("bar", "baz")) } returns Mono.just(2)

		runBlocking {
			assertThat(operations.rightPushAllAndAwait("foo", listOf("bar", "baz"))).isEqualTo(2)
		}

		verify {
			operations.rightPushAll("foo", listOf("bar", "baz"))
		}
	}

	@Test // DATAREDIS-937
	fun rightPushIfPresent() {

		val operations = mockk<ReactiveListOperations<String, String>>()
		every { operations.rightPushIfPresent(any(), any()) } returns Mono.just(2)

		runBlocking {
			assertThat(operations.rightPushIfPresentAndAwait("foo", "bar")).isEqualTo(2)
		}

		verify {
			operations.rightPushIfPresent("foo", "bar")
		}
	}

	@Test // DATAREDIS-937
	fun rightPushPivot() {

		val operations = mockk<ReactiveListOperations<String, String>>()
		every { operations.rightPush("foo", "bar", "baz") } returns Mono.just(2)

		runBlocking {
			assertThat(operations.rightPushAndAwait("foo", "bar", "baz")).isEqualTo(2)
		}

		verify {
			operations.rightPush("foo", "bar", "baz")
		}
	}

	@Test // DATAREDIS-937
	fun set() {

		val operations = mockk<ReactiveListOperations<String, String>>()
		every { operations.set(any(), any(), any()) } returns Mono.just(true)

		runBlocking {
			assertThat(operations.setAndAwait("foo", 1, "baz")).isTrue()
		}

		verify {
			operations.set("foo", 1, "baz")
		}
	}

	@Test // DATAREDIS-937
	fun remove() {

		val operations = mockk<ReactiveListOperations<String, String>>()
		every { operations.remove(any(), any(), any()) } returns Mono.just(1)

		runBlocking {
			assertThat(operations.removeAndAwait("foo", 1, "baz")).isEqualTo(1)
		}

		verify {
			operations.remove("foo", 1, "baz")
		}
	}

	@Test // DATAREDIS-937
	fun index() {

		val operations = mockk<ReactiveListOperations<String, String>>()
		every { operations.index(any(), any()) } returns Mono.just("foo")

		runBlocking {
			assertThat(operations.indexAndAwait("foo", 1)).isEqualTo("foo")
		}

		verify {
			operations.index("foo", 1)
		}
	}

	@Test // DATAREDIS-937
	fun leftPop() {

		val operations = mockk<ReactiveListOperations<String, String>>()
		every { operations.leftPop(any()) } returns Mono.just("foo")

		runBlocking {
			assertThat(operations.leftPopAndAwait("foo")).isEqualTo("foo")
		}

		verify {
			operations.leftPop("foo")
		}
	}

	@Test // DATAREDIS-937
	fun blockingLeftPop() {

		val operations = mockk<ReactiveListOperations<String, String>>()
		every { operations.leftPop(any(), any()) } returns Mono.just("foo")

		runBlocking {
			assertThat(operations.leftPopAndAwait("foo", Duration.ofDays(1))).isEqualTo("foo")
		}

		verify {
			operations.leftPop("foo", Duration.ofDays(1))
		}
	}

	@Test // DATAREDIS-937
	fun rightPop() {

		val operations = mockk<ReactiveListOperations<String, String>>()
		every { operations.rightPop(any()) } returns Mono.just("foo")

		runBlocking {
			assertThat(operations.rightPopAndAwait("foo")).isEqualTo("foo")
		}

		verify {
			operations.rightPop("foo")
		}
	}

	@Test // DATAREDIS-937
	fun blockingRightPop() {

		val operations = mockk<ReactiveListOperations<String, String>>()
		every { operations.rightPop(any(), any()) } returns Mono.just("foo")

		runBlocking {
			assertThat(operations.rightPopAndAwait("foo", Duration.ofDays(1))).isEqualTo("foo")
		}

		verify {
			operations.rightPop("foo", Duration.ofDays(1))
		}
	}

	@Test // DATAREDIS-937
	fun delete() {

		val operations = mockk<ReactiveListOperations<String, String>>()
		every { operations.delete(any()) } returns Mono.just(true)

		runBlocking {
			assertThat(operations.deleteAndAwait("foo")).isTrue()
		}

		verify {
			operations.delete("foo")
		}
	}

}
