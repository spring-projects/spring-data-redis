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
import org.springframework.data.domain.Range
import org.springframework.data.redis.connection.RedisZSetCommands.Aggregate
import org.springframework.data.redis.connection.RedisZSetCommands.Weights
import reactor.core.publisher.Mono

/**
 * Unit tests for [ReactiveZSetOperationsExtensions].
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 */
class ReactiveZSetOperationsExtensionsUnitTests {

	@Test // DATAREDIS-937
	fun add() {

		val operations = mockk<ReactiveZSetOperations<String, String>>()
		every { operations.add(any(), any(), 1.0) } returns Mono.just(true)

		runBlocking {
			assertThat(operations.addAndAwait("foo", "bar", 1.0)).isTrue()
		}

		verify {
			operations.add("foo", "bar", 1.0)
		}
	}

	@Test // DATAREDIS-937
	fun addAll() {

		val operations = mockk<ReactiveZSetOperations<String, String>>()
		every { operations.addAll(any(), any()) } returns Mono.just(1)

		runBlocking {
			assertThat(operations.addAllAndAwait("foo", listOf(DefaultTypedTuple("v", 1.0)))).isEqualTo(1)
		}

		verify {
			operations.addAll("foo", listOf(DefaultTypedTuple("v", 1.0)))
		}
	}

	@Test // DATAREDIS-937
	fun remove() {

		val operations = mockk<ReactiveZSetOperations<String, String>>()
		every { operations.remove("foo", "bar") } returns Mono.just(1)

		runBlocking {
			assertThat(operations.removeAndAwait("foo", "bar")).isEqualTo(1)
		}

		verify {
			operations.remove("foo", "bar")
		}
	}

	@Test // DATAREDIS-937
	fun incrementScore() {

		val operations = mockk<ReactiveZSetOperations<String, String>>()
		every { operations.incrementScore(any(), any(), 1.0) } returns Mono.just(1.0)

		runBlocking {
			assertThat(operations.incrementScoreAndAwait("foo", "bar", 1.0)).isEqualTo(1.0)
		}

		verify {
			operations.incrementScore("foo", "bar", 1.0)
		}
	}

	@Test // DATAREDIS-937
	fun rank() {

		val operations = mockk<ReactiveZSetOperations<String, String>>()
		every { operations.rank(any(), any()) } returns Mono.just(1)

		runBlocking {
			assertThat(operations.rankAndAwait("foo", "bar")).isEqualTo(1)
		}

		verify {
			operations.rank("foo", "bar")
		}
	}

	@Test // DATAREDIS-937
	fun `rank returning an empty Mono`() {

		val operations = mockk<ReactiveZSetOperations<String, String>>()
		every { operations.rank(any(), any()) } returns Mono.empty();

		runBlocking {
			assertThat(operations.rankAndAwait("foo", "bar")).isNull()
		}

		verify {
			operations.rank("foo", "bar")
		}
	}

	@Test // DATAREDIS-937
	fun reverseRank() {

		val operations = mockk<ReactiveZSetOperations<String, String>>()
		every { operations.reverseRank(any(), any()) } returns Mono.just(1)

		runBlocking {
			assertThat(operations.reverseRankAndAwait("foo", "bar")).isEqualTo(1)
		}

		verify {
			operations.reverseRank("foo", "bar")
		}
	}

	@Test // DATAREDIS-937
	fun `reverseRank returning an enpty Mono`() {

		val operations = mockk<ReactiveZSetOperations<String, String>>()
		every { operations.reverseRank(any(), any()) } returns Mono.empty()

		runBlocking {
			assertThat(operations.reverseRankAndAwait("foo", "bar")).isNull()
		}

		verify {
			operations.reverseRank("foo", "bar")
		}
	}

	@Test // DATAREDIS-937
	fun count() {

		val operations = mockk<ReactiveZSetOperations<String, String>>()
		every { operations.count(any(), any()) } returns Mono.just(1)

		runBlocking {
			assertThat(operations.countAndAwait("foo", Range.unbounded())).isEqualTo(1)
		}

		verify {
			operations.count("foo", Range.unbounded())
		}
	}

	@Test // DATAREDIS-937
	fun score() {

		val operations = mockk<ReactiveZSetOperations<String, String>>()
		every { operations.score(any(), any()) } returns Mono.just(1.0)

		runBlocking {
			assertThat(operations.scoreAndAwait("foo", "bar")).isEqualTo(1.0)
		}

		verify {
			operations.score("foo", "bar")
		}
	}

	@Test // DATAREDIS-937
	fun `score returning an empty Mono`() {

		val operations = mockk<ReactiveZSetOperations<String, String>>()
		every { operations.score(any(), any()) } returns Mono.empty()

		runBlocking {
			assertThat(operations.scoreAndAwait("foo", "bar")).isNull()
		}

		verify {
			operations.score("foo", "bar")
		}
	}

	@Test // DATAREDIS-937
	fun removeRange() {

		val operations = mockk<ReactiveZSetOperations<String, String>>()
		every { operations.removeRange(any(), any()) } returns Mono.just(1)

		runBlocking {
			assertThat(operations.removeRangeAndAwait("foo", Range.unbounded())).isEqualTo(1)
		}

		verify {
			operations.removeRange("foo", Range.unbounded())
		}
	}

	@Test // DATAREDIS-937
	fun removeRangeByScore() {

		val operations = mockk<ReactiveZSetOperations<String, String>>()
		every { operations.removeRangeByScore(any(), any()) } returns Mono.just(1)

		runBlocking {
			assertThat(operations.removeRangeByScoreAndAwait("foo", Range.unbounded())).isEqualTo(1)
		}

		verify {
			operations.removeRangeByScore("foo", Range.unbounded())
		}
	}

	@Test // DATAREDIS-937
	fun unionAndStore() {

		val operations = mockk<ReactiveZSetOperations<String, String>>()
		every { operations.unionAndStore("foo", "bar", "baz") } returns Mono.just(1)

		runBlocking {
			assertThat(operations.unionAndStoreAndAwait("foo", "bar", "baz")).isEqualTo(1)
		}

		verify {
			operations.unionAndStore("foo", "bar", "baz")
		}
	}

	@Test // DATAREDIS-937
	fun unionAndStoreListOfKeys() {

		val operations = mockk<ReactiveZSetOperations<String, String>>()
		every { operations.unionAndStore("foo", listOf("bar"), "baz") } returns Mono.just(1)

		runBlocking {
			assertThat(operations.unionAndStoreAndAwait("foo", listOf("bar"), "baz")).isEqualTo(1)
		}

		verify {
			operations.unionAndStore("foo", listOf("bar"), "baz")
		}
	}

	@Test // DATAREDIS-937
	fun unionAndStoreAggregate() {

		val operations = mockk<ReactiveZSetOperations<String, String>>()
		every { operations.unionAndStore(any(), any(), any(), any()) } returns Mono.just(1)

		runBlocking {
			assertThat(operations.unionAndStoreAndAwait("foo", listOf("bar"), "baz", Aggregate.MAX)).isEqualTo(1)
		}

		verify {
			operations.unionAndStore("foo", listOf("bar"), "baz", Aggregate.MAX)
		}
	}

	@Test // DATAREDIS-937
	fun unionAndStoreWeights() {

		val operations = mockk<ReactiveZSetOperations<String, String>>()
		every { operations.unionAndStore(any(), any(), any(), any(), any()) } returns Mono.just(1)

		runBlocking {
			assertThat(operations.unionAndStoreAndAwait("foo", listOf("bar"), "baz", Aggregate.MAX, Weights.fromSetCount(1))).isEqualTo(1)
		}

		verify {
			operations.unionAndStore("foo", listOf("bar"), "baz", Aggregate.MAX, Weights.fromSetCount(1))
		}
	}

	@Test // DATAREDIS-937
	fun intersectAndStore() {

		val operations = mockk<ReactiveZSetOperations<String, String>>()
		every { operations.intersectAndStore("foo", "bar", "baz") } returns Mono.just(1)

		runBlocking {
			assertThat(operations.intersectAndStoreAndAwait("foo", "bar", "baz")).isEqualTo(1)
		}

		verify {
			operations.intersectAndStore("foo", "bar", "baz")
		}
	}

	@Test // DATAREDIS-937
	fun intersectAndStoreListOfKeys() {

		val operations = mockk<ReactiveZSetOperations<String, String>>()
		every { operations.intersectAndStore("foo", listOf("bar"), "baz") } returns Mono.just(1)

		runBlocking {
			assertThat(operations.intersectAndStoreAndAwait("foo", listOf("bar"), "baz")).isEqualTo(1)
		}

		verify {
			operations.intersectAndStore("foo", listOf("bar"), "baz")
		}
	}

	@Test // DATAREDIS-937
	fun intersectAndStoreAggregate() {

		val operations = mockk<ReactiveZSetOperations<String, String>>()
		every { operations.intersectAndStore(any(), any(), any(), any()) } returns Mono.just(1)

		runBlocking {
			assertThat(operations.intersectAndStoreAndAwait("foo", listOf("bar"), "baz", Aggregate.MAX)).isEqualTo(1)
		}

		verify {
			operations.intersectAndStore("foo", listOf("bar"), "baz", Aggregate.MAX)
		}
	}

	@Test // DATAREDIS-937
	fun intersectAndStoreWeights() {

		val operations = mockk<ReactiveZSetOperations<String, String>>()
		every { operations.intersectAndStore(any(), any(), any(), any(), any()) } returns Mono.just(1)

		runBlocking {
			assertThat(operations.intersectAndStoreAndAwait("foo", listOf("bar"), "baz", Aggregate.MAX, Weights.fromSetCount(1))).isEqualTo(1)
		}

		verify {
			operations.intersectAndStore("foo", listOf("bar"), "baz", Aggregate.MAX, Weights.fromSetCount(1))
		}
	}
}
