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
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import org.reactivestreams.Publisher
import org.springframework.data.geo.Circle
import org.springframework.data.geo.Distance
import org.springframework.data.geo.GeoResult
import org.springframework.data.geo.Metrics
import org.springframework.data.geo.Point
import org.springframework.data.redis.connection.RedisGeoCommands
import org.springframework.data.redis.connection.RedisGeoCommands.*
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

/**
 * Unit tests for `ReactiveGeoOperationsExtensions`.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @author Sebastien Deleuze
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
		every { operations.add(any(), any<GeoLocation<String>>()) } returns Mono.just(1)

		runBlocking {
			assertThat(operations.addAndAwait("foo", GeoLocation("bar", Point(1.0, 2.0)))).isEqualTo(1)
		}

		verify {
			operations.add("foo", GeoLocation("bar", Point(1.0, 2.0)))
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
		every { operations.add(any(), any<List<GeoLocation<String>>>()) } returns Mono.just(1)

		runBlocking {
			assertThat(operations.addAndAwait("foo", listOf(GeoLocation("bar", Point(1.0, 2.0))))).isEqualTo(1)
		}

		verify {
			operations.add("foo", listOf(GeoLocation("bar", Point(1.0, 2.0))))
		}
	}

	@Test // DATAREDIS-1033
	fun addGeoLocationFlow() {

		val operations = mockk<ReactiveGeoOperations<String, String>>()
		every { operations.add(any(), any<Publisher<List<GeoLocation<String>>>>()) } returns Flux.just(1)
		val flow = flow { emit(listOf(GeoLocation("bar", Point(1.0, 2.0)))) }

		runBlocking {
			assertThat(operations.add("foo", flow).toList()).contains(1)
		}

		verify {
			operations.add("foo", any<Publisher<List<GeoLocation<String>>>>())
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

	@Test // DATAREDIS-1033
	fun radiusAsFlowCircle() {

		val operations = mockk<ReactiveGeoOperations<String, String>>()
		val result = GeoResult(GeoLocation("bar", Point(1.0, 2.0)), Distance(1.0))
		val circle = Circle(1.0, 2.0, 3.0)
		every { operations.radius(any(), any()) } returns Flux.just(result)

		runBlocking {
			assertThat(operations.radiusAsFlow("foo", circle).toList()).contains(result)
		}

		verify {
			operations.radius("foo", circle)
		}
	}

	@Test // DATAREDIS-1033
	fun radiusAsFlowCircleAndArgs() {

		val operations = mockk<ReactiveGeoOperations<String, String>>()
		val result = GeoResult(GeoLocation("bar", Point(1.0, 2.0)), Distance(1.0))
		val circle = Circle(1.0, 2.0, 3.0)
		val args = GeoRadiusCommandArgs.newGeoRadiusArgs()
		every { operations.radius(any(), any(), args) } returns Flux.just(result)

		runBlocking {
			assertThat(operations.radiusAsFlow("foo", circle, args).toList()).contains(result)
		}

		verify {
			operations.radius("foo", circle, args)
		}
	}

	@Test // DATAREDIS-1033
	fun radiusAsFlowMemberAndRadius() {

		val operations = mockk<ReactiveGeoOperations<String, String>>()
		val result = GeoResult(GeoLocation("bar", Point(1.0, 2.0)), Distance(1.0))

		every { operations.radius(any(), any(), any<Double>()) } returns Flux.just(result)

		runBlocking {
			assertThat(operations.radiusAsFlow("foo", "bar", 1.0).toList()).contains(result)
		}

		verify {
			operations.radius("foo", "bar", 1.0)
		}
	}

	@Test // DATAREDIS-1033
	fun radiusAsFlowDistance() {

		val operations = mockk<ReactiveGeoOperations<String, String>>()
		val result = GeoResult(GeoLocation("bar", Point(1.0, 2.0)), Distance(1.0))
		val distance = Distance(2.0)

		every { operations.radius(any(), any(), any<Distance>()) } returns Flux.just(result)

		runBlocking {
			assertThat(operations.radiusAsFlow("foo", "bar", distance).toList()).contains(result)
		}

		verify {
			operations.radius("foo", "bar", distance)
		}
	}

	@Test // DATAREDIS-1033
	fun radiusAsFlowDistanceAndArgs() {

		val operations = mockk<ReactiveGeoOperations<String, String>>()
		val result = GeoResult(GeoLocation("bar", Point(1.0, 2.0)), Distance(1.0))
		val distance = Distance(2.0)
		val args = GeoRadiusCommandArgs.newGeoRadiusArgs().limit(1)

		every { operations.radius(any(), any(), any(), any()) } returns Flux.just(result)

		runBlocking {
			assertThat(operations.radiusAsFlow("foo", "bar", distance, args).toList()).contains(result)
		}

		verify {
			operations.radius("foo", "bar", distance, args)
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
