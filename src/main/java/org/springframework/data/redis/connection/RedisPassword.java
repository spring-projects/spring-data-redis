/*
 * Copyright 2017-2018 the original author or authors.
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
package org.springframework.data.redis.connection;

import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;

import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.function.Function;

import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

/**
 * Value object which may or may not contain a Redis password.
 * <p/>
 * If a password is present, {@code isPresent()} will return {@code true} and {@code get()} will return the value.
 * <p/>
 * The password is stored as character array.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.0
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
@EqualsAndHashCode
public class RedisPassword {

	private static final RedisPassword NONE = new RedisPassword(new char[] {});

	private final char[] thePassword;

	/**
	 * Create a {@link RedisPassword} from a {@link String}.
	 *
	 * @param passwordAsString the password as string.
	 * @return the {@link RedisPassword} for {@code passwordAsString}.
	 */
	public static RedisPassword of(@Nullable String passwordAsString) {

		return Optional.ofNullable(passwordAsString) //
				.filter(StringUtils::hasText) //
				.map(it -> new RedisPassword(it.toCharArray())) //
				.orElseGet(RedisPassword::none);
	}

	/**
	 * Create a {@link RedisPassword} from a {@code char} array.
	 *
	 * @param passwordAsChars the password as char array.
	 * @return the {@link RedisPassword} for {@code passwordAsChars}.
	 */
	public static RedisPassword of(@Nullable char[] passwordAsChars) {

		return Optional.ofNullable(passwordAsChars) //
				.filter(it -> !ObjectUtils.isEmpty(passwordAsChars)) //
				.map(it -> new RedisPassword(Arrays.copyOf(it, it.length))) //
				.orElseGet(RedisPassword::none);
	}

	/**
	 * Create an absent {@link RedisPassword}.
	 *
	 * @return the absent {@link RedisPassword}.
	 */
	public static RedisPassword none() {
		return NONE;
	}

	/**
	 * Return {@code true} if there is a password present, otherwise {@code false}.
	 *
	 * @return {@code true} if there is a password present, otherwise {@code false}
	 */
	public boolean isPresent() {
		return !ObjectUtils.isEmpty(thePassword);
	}

	/**
	 * Return the password value if present. Throws {@link NoSuchElementException} if the password is absent.
	 *
	 * @return the password.
	 * @throws NoSuchElementException if the password is absent.
	 */
	public char[] get() throws NoSuchElementException {

		if (isPresent()) {
			return Arrays.copyOf(thePassword, thePassword.length);
		}

		throw new NoSuchElementException("No password present.");
	}

	/**
	 * Map the password using a {@link Function} and return a {@link Optional} containing the mapped value.
	 * <p/>
	 * Absent passwords return a {@link Optional#empty()}.
	 *
	 * @param mapper must not be {@literal null}.
	 * @return the mapped result.
	 */
	public <R> Optional<R> map(Function<char[], R> mapper) {

		Assert.notNull(mapper, "Mapper function must not be null!");

		return toOptional().map(mapper);
	}

	/**
	 * Adopt the password to {@link Optional} containing the password value.
	 * <p/>
	 * Absent passwords return a {@link Optional#empty()}.
	 *
	 * @return the {@link Optional} containing the password value.
	 */
	public Optional<char[]> toOptional() {

		if (isPresent()) {
			return Optional.of(get());
		}

		return Optional.empty();
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return String.format("%s[%s]", getClass().getSimpleName(), isPresent() ? "*****" : "<none>");
	}
}
