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

import org.springframework.core.ParameterizedTypeReference

/**
 * Decode this [JsonOperations.JsonResult] into the target type [T] using reified type information.
 *
 * @author Yordan Tsintsov
 * @since 4.2
 */
inline fun <reified T : Any> JsonOperations.JsonResult.asType(): T =
    `as`(object : ParameterizedTypeReference<T>() {})

/**
 * Decode all values of this [JsonOperations.JsonResults] into the target type [T] using reified type information.
 *
 * @author Yordan Tsintsov
 * @since 4.2
 */
inline fun <reified T : Any> JsonOperations.JsonResults.asType(): List<T?> =
    `as`(object : ParameterizedTypeReference<T>() {})

/**
 * Decode this [JsonOperations.JsonResult] into the target type [T], returning `null` when the result is absent or
 * represents JSON `null` instead of decoding it.
 *
 * @author Yordan Tsintsov
 * @since 4.2
 */
inline fun <reified T : Any> JsonOperations.JsonResult.asTypeOrNull(): T? =
    if (this.isNull) null else asType()
