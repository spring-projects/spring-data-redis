/*
 * Copyright 2026-present the original author or authors.
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
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.core.ParameterizedTypeReference

/**
 * Unit tests for `JsonOperationsExtensions`.
 *
 * @author Yordan Tsintsov
 */
class JsonOperationsExtensionsUnitTests {

	data class Person(val name: String)

	@Test // GH-3390
	fun `asType on JsonResult delegates to as with a ParameterizedTypeReference`() {

		val result = mockk<JsonOperations.JsonResult>()
		val person = Person("Rand al'Thor")
		every { result.`as`(any<ParameterizedTypeReference<Person>>()) } returns person

		assertThat(result.asType<Person>()).isEqualTo(person)

		verify { result.`as`(any<ParameterizedTypeReference<Person>>()) }
	}

	@Test // GH-3390
	fun `asType on JsonResults delegates to as and preserves null elements`() {

		val results = mockk<JsonOperations.JsonResults>()
		val person = Person("Rand al'Thor")
		every { results.`as`(any<ParameterizedTypeReference<Person>>()) } returns listOf(person, null)

		assertThat(results.asType<Person>()).containsExactly(person, null)

		verify { results.`as`(any<ParameterizedTypeReference<Person>>()) }
	}

	@Test // GH-3390
	fun `asTypeOrNull returns the decoded value when the result is not null`() {

		val result = mockk<JsonOperations.JsonResult>()
		val person = Person("Rand al'Thor")
		every { result.isNull } returns false
		every { result.`as`(any<ParameterizedTypeReference<Person>>()) } returns person

		assertThat(result.asTypeOrNull<Person>()).isEqualTo(person)

		verify { result.`as`(any<ParameterizedTypeReference<Person>>()) }
	}

	@Test // GH-3390
	fun `asTypeOrNull returns null without decoding when the result is null`() {

		val result = mockk<JsonOperations.JsonResult>()
		every { result.isNull } returns true

		assertThat(result.asTypeOrNull<Person>()).isNull()

		verify(exactly = 0) { result.`as`(any<ParameterizedTypeReference<Person>>()) }
	}
}
