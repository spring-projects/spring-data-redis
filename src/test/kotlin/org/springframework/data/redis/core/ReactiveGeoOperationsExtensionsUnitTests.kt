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
import org.springframework.data.geo.Distance
import org.springframework.data.geo.Metrics
import org.springframework.data.geo.Point
import org.springframework.data.redis.connection.RedisGeoCommands
import reactor.core.publisher.Mono

/**
 * Unit tests for [ReactiveGeoOperationsExtensions].
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 */
class ReactiveGeoOperationsExtensionsUnitTests {

	@Test // DATAREDIS-937
	fun addPoint() {

		val operations = mockk<ReactiveGeoOperations<String, String>>()
		every { operations.add(any(), any(), any()) } returns Mono.just(1)

		runBlocking {
			assertThat(operations.addAndAwait("foo", Point(1.0, 2.0), "bar")).isEqualTo(1)
		}

		verify {
			operations.add("foo", Point(1.0, 2.0), "bar")
		}
	}

	@Test // DATAREDIS-937
	fun addGeoLocation() {

		val operations = mockk<ReactiveGeoOperations<String, String>>()
		every { operations.add(any(), any<RedisGeoCommands.GeoLocation<String>>()) } returns Mono.just(1)

		runBlocking {
			assertThat(operations.addAndAwait("foo", RedisGeoCommands.GeoLocation("bar", Point(1.0, 2.0)))).isEqualTo(1)
		}

		verify {
			operations.add("foo", RedisGeoCommands.GeoLocation("bar", Point(1.0, 2.0)))
		}
	}

	@Test // DATAREDIS-937
	fun addLocationMap() {

		val operations = mockk<ReactiveGeoOperations<String, String>>()
		every { operations.add(any(), any<Map<String, Point>>()) } returns Mono.just(1)

		runBlocking {
			assertThat(operations.addAndAwait("foo", mapOf("foo" to Point(1.0, 2.0)))).isEqualTo(1)
		}

		verify {
			operations.add("foo", mapOf("foo" to Point(1.0, 2.0)))
		}
	}

	@Test // DATAREDIS-937
	fun addGeoLocationList() {

		val operations = mockk<ReactiveGeoOperations<String, String>>()
		every { operations.add(any(), any<List<RedisGeoCommands.GeoLocation<String>>>()) } returns Mono.just(1)

		runBlocking {
			assertThat(operations.addAndAwait("foo", listOf(RedisGeoCommands.GeoLocation("bar", Point(1.0, 2.0))))).isEqualTo(1)
		}

		verify {
			operations.add("foo", listOf(RedisGeoCommands.GeoLocation("bar", Point(1.0, 2.0))))
		}
	}

	@Test // DATAREDIS-937
	fun distance() {

		val operations = mockk<ReactiveGeoOperations<String, String>>()
		every { operations.distance(any(), any(), any()) } returns Mono.just(Distance(2.0))

		runBlocking {
			assertThat(operations.distanceAndAwait("foo", "from", "to")).isEqualTo(Distance(2.0))
		}

		verify {
			operations.distance("foo", "from", "to")
		}
	}

	@Test // DATAREDIS-937
	fun `distance returning an empty Mono`() {

		val operations = mockk<ReactiveGeoOperations<String, String>>()
		every { operations.distance(any(), any(), any()) } returns Mono.empty()

		runBlocking {
			assertThat(operations.distanceAndAwait("foo", "from", "to")).isNull()
		}

		verify {
			operations.distance("foo", "from", "to")
		}
	}

	@Test // DATAREDIS-937
	fun distanceWithMetric() {

		val operations = mockk<ReactiveGeoOperations<String, String>>()
		every { operations.distance(any(), any(), any(), any()) } returns Mono.just(Distance(2.0))

		runBlocking {
			assertThat(operations.distanceAndAwait("foo", "from", "to", Metrics.KILOMETERS)).isEqualTo(Distance(2.0))
		}

		verify {
			operations.distance("foo", "from", "to", Metrics.KILOMETERS)
		}
	}

	@Test // DATAREDIS-937
	fun `distance with Metric returning an empty Mono`() {

		val operations = mockk<ReactiveGeoOperations<String, String>>()
		every { operations.distance(any(), any(), any(), any()) } returns Mono.empty()

		runBlocking {
			assertThat(operations.distanceAndAwait("foo", "from", "to", Metrics.KILOMETERS)).isNull()
		}

		verify {
			operations.distance("foo", "from", "to", Metrics.KILOMETERS)
		}
	}

	@Test // DATAREDIS-937
	fun hash() {

		val operations = mockk<ReactiveGeoOperations<String, String>>()
		every { operations.hash(any(), any()) } returns Mono.just("baz")

		runBlocking {
			assertThat(operations.hashAndAwait("foo", "bar")).isEqualTo("baz")
		}

		verify {
			operations.hash("foo", "bar")
		}
	}

	@Test // DATAREDIS-937
	fun `hash returning an empty Mono`() {

		val operations = mockk<ReactiveGeoOperations<String, String>>()
		every { operations.hash(any(), any()) } returns Mono.empty()

		runBlocking {
			assertThat(operations.hashAndAwait("foo", "bar")).isNull()
		}

		verify {
			operations.hash("foo", "bar")
		}
	}

	@Test // DATAREDIS-937
	fun hashVararg() {

		val operations = mockk<ReactiveGeoOperations<String, String>>()
		every { operations.hash(any(), any(), any()) } returns Mono.just(listOf("baz1", "baz2"))

		runBlocking {
			assertThat(operations.hashAndAwait("foo", "bar", "baz")).isEqualTo(listOf("baz1", "baz2"))
		}

		verify {
			operations.hash("foo", "bar", "baz")
		}
	}


	@Test // DATAREDIS-937
	fun position() {

		val operations = mockk<ReactiveGeoOperations<String, String>>()
		every { operations.position(any(), any()) } returns Mono.just(Point(1.0, 2.0))

		runBlocking {
			assertThat(operations.positionAndAwait("foo", "bar")).isEqualTo(Point(1.0, 2.0))
		}

		verify {
			operations.position("foo", "bar")
		}
	}

	@Test // DATAREDIS-937
	fun `position returning an empty Mono`() {

		val operations = mockk<ReactiveGeoOperations<String, String>>()
		every { operations.position(any(), any()) } returns Mono.empty()

		runBlocking {
			assertThat(operations.positionAndAwait("foo", "bar")).isNull()
		}

		verify {
			operations.position("foo", "bar")
		}
	}

	@Test // DATAREDIS-937
	fun positionVararg() {

		val operations = mockk<ReactiveGeoOperations<String, String>>()
		every { operations.position(any(), any(), any()) } returns Mono.just(listOf(Point(1.0, 2.0), Point(2.0, 3.0)))

		runBlocking {
			assertThat(operations.positionAndAwait("foo", "bar", "baz")).isEqualTo(listOf(Point(1.0, 2.0), Point(2.0, 3.0)))
		}

		verify {
			operations.position("foo", "bar", "baz")
		}
	}

	@Test // DATAREDIS-937
	fun remove() {

		val operations = mockk<ReactiveGeoOperations<String, String>>()
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

		val operations = mockk<ReactiveGeoOperations<String, String>>()
		every { operations.delete(any()) } returns Mono.just(true)

		runBlocking {
			assertThat(operations.deleteAndAwait("foo")).isTrue()
		}

		verify {
			operations.delete("foo")
		}
	}
}
