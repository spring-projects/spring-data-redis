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
import org.springframework.data.redis.connection.DataType
import reactor.core.publisher.Mono
import java.time.Duration
import java.time.Instant

/**
 * Unit tests for [ReactiveRedisOperationsExtensions].
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 */
class ReactiveRedisOperationsExtensionsUnitTests {

	@Test // DATAREDIS-937
	fun convertAndSend() {

		val operations = mockk<ReactiveRedisOperations<String, String>>()
		every { operations.convertAndSend(any(), any()) } returns Mono.just(1)

		runBlocking {
			assertThat(operations.sendAndAwait("foo", "bar")).isEqualTo(1)
		}

		verify {
			operations.convertAndSend("foo", "bar")
		}
	}

	@Test // DATAREDIS-937
	fun hasKey() {

		val operations = mockk<ReactiveRedisOperations<String, String>>()
		every { operations.hasKey(any()) } returns Mono.just(true)

		runBlocking {
			assertThat(operations.hasKeyAndAwait("foo")).isTrue()
		}

		verify {
			operations.hasKey("foo")
		}
	}

	@Test // DATAREDIS-937
	fun type() {

		val operations = mockk<ReactiveRedisOperations<String, String>>()
		every { operations.type(any()) } returns Mono.just(DataType.HASH)

		runBlocking {
			assertThat(operations.typeAndAwait("foo")).isEqualTo(DataType.HASH)
		}

		verify {
			operations.type("foo")
		}
	}

	@Test // DATAREDIS-937
	fun randomKey() {

		val operations = mockk<ReactiveRedisOperations<String, String>>()
		every { operations.randomKey() } returns Mono.just("foo")

		runBlocking {
			assertThat(operations.randomKeyAndAwait()).isEqualTo("foo")
		}

		verify {
			operations.randomKey()
		}
	}

	@Test // DATAREDIS-937
	fun `randomKey returning an empty Mono`() {

		val operations = mockk<ReactiveRedisOperations<String, String>>()
		every { operations.randomKey() } returns Mono.empty()

		runBlocking {
			assertThat(operations.randomKeyAndAwait()).isNull();
		}

		verify {
			operations.randomKey()
		}
	}

	@Test // DATAREDIS-937
	fun rename() {

		val operations = mockk<ReactiveRedisOperations<String, String>>()
		every { operations.rename(any(), any()) } returns Mono.just(true)

		runBlocking {
			assertThat(operations.renameAndAwait("foo", "bar")).isTrue()
		}

		verify {
			operations.rename("foo", "bar")
		}
	}

	@Test // DATAREDIS-937
	fun renameIfAbsent() {

		val operations = mockk<ReactiveRedisOperations<String, String>>()
		every { operations.renameIfAbsent(any(), any()) } returns Mono.just(true)

		runBlocking {
			assertThat(operations.renameIfAbsentAndAwait("foo", "bar")).isTrue()
		}

		verify {
			operations.renameIfAbsent("foo", "bar")
		}
	}

	@Test // DATAREDIS-937
	fun delete() {

		val operations = mockk<ReactiveRedisOperations<String, String>>()
		every { operations.delete("foo", "bar") } returns Mono.just(2)

		runBlocking {
			assertThat(operations.deleteAndAwait("foo", "bar")).isEqualTo(2)
		}

		verify {
			operations.delete("foo", "bar")
		}
	}

	@Test // DATAREDIS-937
	fun unlink() {

		val operations = mockk<ReactiveRedisOperations<String, String>>()
		every { operations.unlink("foo", "bar") } returns Mono.just(2)

		runBlocking {
			assertThat(operations.unlinkAndAwait("foo", "bar")).isEqualTo(2)
		}

		verify {
			operations.unlink("foo", "bar")
		}
	}

	@Test // DATAREDIS-937
	fun expire() {

		val operations = mockk<ReactiveRedisOperations<String, String>>()
		every { operations.expire(any(), any()) } returns Mono.just(true)

		runBlocking {
			assertThat(operations.expireAndAwait("foo", Duration.ofDays(1))).isTrue()
		}

		verify {
			operations.expire("foo", Duration.ofDays(1))
		}
	}

	@Test // DATAREDIS-937
	fun expireAt() {

		val operations = mockk<ReactiveRedisOperations<String, String>>()
		every { operations.expireAt(any(), any()) } returns Mono.just(true)

		runBlocking {
			assertThat(operations.expireAtAndAwait("foo", Instant.ofEpochSecond(2))).isTrue()
		}

		verify {
			operations.expireAt("foo", Instant.ofEpochSecond(2))
		}
	}

	@Test // DATAREDIS-937
	fun persist() {

		val operations = mockk<ReactiveRedisOperations<String, String>>()
		every { operations.persist(any()) } returns Mono.just(true)

		runBlocking {
			assertThat(operations.persistAndAwait("foo")).isTrue()
		}

		verify {
			operations.persist("foo")
		}
	}


	@Test // DATAREDIS-937
	fun move() {

		val operations = mockk<ReactiveRedisOperations<String, String>>()
		every { operations.move(any(), any()) } returns Mono.just(true)

		runBlocking {
			assertThat(operations.moveAndAwait("foo", 2)).isTrue()
		}

		verify {
			operations.move("foo", 2)
		}
	}

	@Test // DATAREDIS-937
	fun getExpire() {

		val operations = mockk<ReactiveRedisOperations<String, String>>()
		every { operations.getExpire(any()) } returns Mono.just(Duration.ofDays(1))

		runBlocking {
			assertThat(operations.getExpireAndAwait("foo")).isEqualTo(Duration.ofDays(1))
		}

		verify {
			operations.getExpire("foo")
		}
	}
}
