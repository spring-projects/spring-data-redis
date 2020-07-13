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
 * Unit tests for `ReactiveHyperLogLogOperationsExtensions`
 *
 * @author Mark Paluch
 */
class ReactiveHyperLogLogOperationsExtensionsUnitTests {

	@Test // DATAREDIS-937
	fun add() {

		val operations = mockk<ReactiveHyperLogLogOperations<String, String>>()
		every { operations.add(any(), any()) } returns Mono.just(1)

		runBlocking {
			assertThat(operations.addAndAwait("foo", "bar")).isEqualTo(1)
		}

		verify {
			operations.add("foo", "bar")
		}
	}

	@Test // DATAREDIS-937
	fun size() {

		val operations = mockk<ReactiveHyperLogLogOperations<String, String>>()
		every { operations.size("foo") } returns Mono.just(1)

		runBlocking {
			assertThat(operations.sizeAndAwait("foo")).isEqualTo(1)
		}

		verify {
			operations.size("foo")
		}
	}

	@Test // DATAREDIS-937
	fun union() {

		val operations = mockk<ReactiveHyperLogLogOperations<String, String>>()
		every { operations.union("foo", "bar", "baz") } returns Mono.just(true)

		runBlocking {
			assertThat(operations.unionAndAwait("foo", "bar", "baz")).isTrue()
		}

		verify {
			operations.union("foo", "bar", "baz")
		}
	}

	@Test // DATAREDIS-937
	fun remove() {

		val operations = mockk<ReactiveHyperLogLogOperations<String, String>>()
		every { operations.delete(any()) } returns Mono.just(true)

		runBlocking {
			assertThat(operations.deleteAndAwait("foo")).isTrue()
		}

		verify {
			operations.delete("foo")
		}
	}
}
