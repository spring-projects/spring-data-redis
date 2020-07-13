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
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import org.springframework.data.redis.connection.DataType
import org.springframework.data.redis.connection.ReactiveSubscription
import org.springframework.data.redis.core.script.RedisScript
import org.springframework.data.redis.listener.ChannelTopic
import org.springframework.data.redis.serializer.RedisElementReader
import org.springframework.data.redis.serializer.RedisElementWriter
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.Duration
import java.time.Instant

/**
 * Unit tests for `ReactiveRedisOperationsExtensions`.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @author Sebastien Deleuze
 */
class ReactiveRedisOperationsExtensionsUnitTests {

	@Test // DATAREDIS-1033
	fun `execute with calllback`() {

		val operations = mockk<ReactiveRedisOperations<String, String>>()
		every { operations.execute(any<ReactiveRedisCallback<*>>()) } returns Flux.just("foo")

		runBlocking {
			assertThat(operations.executeAsFlow { flow { emit("foo")} }.toList()).contains("foo")
		}

		verify {
			operations.execute(any<ReactiveRedisCallback<*>>())
		}
	}

	@Test // DATAREDIS-1033
	fun `execute with script`() {

		val script = RedisScript.of<String>("foo")
		val operations = mockk<ReactiveRedisOperations<String, String>>()
		every { operations.execute(any<RedisScript<*>>(), any(), any()) } returns Flux.just("foo")

		runBlocking {
			assertThat(operations.executeAsFlow(script).toList()).contains("foo")
		}

		verify {
			operations.execute(script, any(), any())
		}
	}

	@Test // DATAREDIS-1033
	fun `execute with script, argsWriter and resultReader`() {

		val script = RedisScript.of<String>("foo")
		val argsWriter = mockk<RedisElementWriter<Any>>(relaxed = true)
		val resultReader = mockk<RedisElementReader<String>>(relaxed = true)
		val operations = mockk<ReactiveRedisOperations<String, String>>()
		every { operations.execute(any<RedisScript<String>>(), any(), any(), any(), any()) } returns Flux.just("foo")

		runBlocking {
			assertThat(operations.executeAsFlow(script, argsWriter = argsWriter, resultReader = resultReader).toList()).contains("foo")
		}

		verify {
			operations.execute(script, any(), any(), argsWriter, resultReader)
		}
	}

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

	@Test // DATAREDIS-1033
	fun listenToChannel() {

		val message = ReactiveSubscription.ChannelMessage("a", "b")
		val operations = mockk<ReactiveRedisOperations<String, String>>()
		every { operations.listenToChannel(any(), any()) } returns Flux.just(message)

		runBlocking {
			assertThat(operations.listenToChannelAsFlow("foo", "bar").toList()).contains(message)
		}

		verify {
			operations.listenToChannel("foo", "bar")
		}
	}

	@Test // DATAREDIS-1033
	fun listenToPattern() {

		val message = ReactiveSubscription.ChannelMessage("a", "b")
		val operations = mockk<ReactiveRedisOperations<String, String>>()
		every { operations.listenToPattern(any(), any()) } returns Flux.just(message)

		runBlocking {
			assertThat(operations.listenToPatternAsFlow("foo", "bar").toList()).contains(message)
		}

		verify {
			operations.listenToPattern("foo", "bar")
		}
	}

	@Test // DATAREDIS-1033
	fun listenTo() {

		val topic1 = ChannelTopic.of("foo")
		val topic2 = ChannelTopic.of("bar")
		val message = ReactiveSubscription.ChannelMessage("a", "b")
		val operations = mockk<ReactiveRedisOperations<String, String>>()
		every { operations.listenTo(any(), any()) } returns Flux.just(message)

		runBlocking {
			assertThat(operations.listenToAsFlow(topic1, topic2).toList()).contains(message)
		}

		verify {
			operations.listenTo(topic1, topic2)
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

	@Test // DATAREDIS-1033
	fun keys() {

		val operations = mockk<ReactiveRedisOperations<String, String>>()
		every { operations.keys(any()) } returns Flux.just("bar")

		runBlocking {
			assertThat(operations.keysAsFlow("foo").toList()).contains("bar")
		}

		verify {
			operations.keys("foo")
		}
	}

	@Test // DATAREDIS-1033
	fun scan() {

		val operations = mockk<ReactiveRedisOperations<String, String>>()
		every { operations.scan(ScanOptions.NONE) } returns Flux.just("foo")

		runBlocking {
			assertThat(operations.scanAsFlow().toList()).contains("foo")
		}

		verify {
			operations.scan(ScanOptions.NONE)
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
