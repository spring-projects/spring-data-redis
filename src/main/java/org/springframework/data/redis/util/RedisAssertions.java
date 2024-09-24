/*
 * Copyright 2017-2024 the original author or authors.
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
 *  limitations under the License.
 */
package org.springframework.data.redis.util;

import java.util.function.Supplier;

import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Abstract utility class for common assertions used in Spring Data Redis.
 *
 * @author John Blum
 * @since 3.1.0
 * @deprecated since 3.3, will be removed in a future revision in favor of Spring's {@link Assert} utility.
 */
@Deprecated(since = "3.3", forRemoval = true)
public abstract class RedisAssertions {

	/**
	 * Asserts the given {@link Object} is not {@literal null}.
	 *
	 * @param <T> {@link Class type} of {@link Object} being asserted.
	 * @param target {@link Object} to evaluate.
	 * @param message {@link String} containing the message for the thrown exception.
	 * @param arguments array of {@link Object} arguments used to format the {@link String message}.
	 * @return the given {@link Object}.
	 * @throws IllegalArgumentException if the {@link Object target} is {@literal null}.
	 * @see #requireNonNull(Object, Supplier)
	 */
	public static <T> T requireNonNull(@Nullable T target, String message, Object... arguments) {
		return requireNonNull(target, () -> message.formatted(arguments));
	}

	/**
	 * Asserts the given {@link Object} is not {@literal null}.
	 *
	 * @param <T> {@link Class type} of {@link Object} being asserted.
	 * @param target {@link Object} to evaluate.
	 * @param message {@link Supplier} supplying the message for the thrown exception.
	 * @return the given {@link Object}.
	 * @throws IllegalArgumentException if the {@link Object target} is {@literal null}.
	 */
	public static <T> T requireNonNull(@Nullable T target, Supplier<String> message) {
		Assert.notNull(target, message);
		return target;
	}

	/**
	 * Asserts the given {@link Object} is not {@literal null} throwing the given {@link RuntimeException}
	 * if {@link Object} is {@literal null}.
	 *
	 * @param <T> {@link Class type} of {@link Object} being asserted.
	 * @param target {@link Object} to evaluate.
	 * @param cause {@link Supplier} of a {@link RuntimeException} to throw
	 * if the given {@link Object} is {@literal null}.
	 * @return the given {@link Object}.
	 */
	public static <T> T requireNonNull(@Nullable T target, RuntimeExceptionSupplier cause) {

		if (target == null) {
			throw cause.get();
		}

		return target;
	}

	/**
	 * Asserts the given {@link Object} is not {@literal null}.
	 *
	 * @param <T> {@link Class type} of {@link Object} being asserted.
	 * @param target {@link Object} to evaluate.
	 * @param message {@link String} containing the message for the thrown exception.
	 * @param arguments array of {@link Object} arguments used to format the {@link String message}.
	 * @return the given {@link Object}.
	 * @throws IllegalArgumentException if the {@link Object target} is {@literal null}.
	 * @see #requireNonNull(Object, Supplier)
	 */
	public static <T> T requireState(@Nullable T target, String message, Object... arguments) {
		return requireState(target, () -> message.formatted(arguments));
	}

	/**
	 * Asserts the given {@link Object} is not {@literal null}.
	 *
	 * @param <T> {@link Class type} of {@link Object} being asserted.
	 * @param target {@link Object} to evaluate.
	 * @param message {@link Supplier} supplying the message for the thrown exception.
	 * @return the given {@link Object}.
	 * @throws IllegalArgumentException if the {@link Object target} is {@literal null}.
	 */
	public static <T> T requireState(@Nullable T target, Supplier<String> message) {
		Assert.state(target != null, message);
		return target;
	}

	public interface RuntimeExceptionSupplier extends Supplier<RuntimeException> { }

}
