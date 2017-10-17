/*
 * Copyright 2011-2017 the original author or authors.
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
package org.springframework.data.redis.serializer;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Simple {@literal} to {@literal byte[]} (and back) serializer. Converts Strings into bytes and vice-versa using the
 * specified charset (by default UTF-8).
 * <p>
 * Useful when the interaction with the Redis happens mainly through Strings.
 * <p>
 * Does not perform any {@literal null} conversion since empty strings are valid keys/values.
 *
 * @author Costin Leau
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public class StringRedisSerializer implements RedisSerializer<String> {

	private final Charset charset;

	/**
	 * Serializer to use 7 bit ASCII, a.k.a. ISO646-US, a.k.a. the Basic Latin block of the Unicode character set.
	 * 
	 * @since 2.1
	 */
	public static final StringRedisSerializer US_ASCII = new StringRedisSerializer(StandardCharsets.US_ASCII);

	/**
	 * Serializer to use ISO Latin Alphabet No. 1, a.k.a. ISO-LATIN-1.
	 * 
	 * @since 2.1
	 */
	public static final StringRedisSerializer ISO_8859_1 = new StringRedisSerializer(StandardCharsets.ISO_8859_1);

	/**
	 * Serializer to use 8 bit UCS Transformation Format.
	 * 
	 * @since 2.1
	 */
	public static final StringRedisSerializer UTF_8 = new StringRedisSerializer(StandardCharsets.UTF_8);

	/**
	 * Creates a new {@link StringRedisSerializer} using {@link StandardCharsets#UTF_8 UTF-8}.
	 */
	public StringRedisSerializer() {
		this(StandardCharsets.UTF_8);
	}

	/**
	 * Creates a new {@link StringRedisSerializer} using the given {@link Charset} to encode and decode strings.
	 *
	 * @param charset must not be {@literal null}.
	 */
	public StringRedisSerializer(Charset charset) {

		Assert.notNull(charset, "Charset must not be null!");
		this.charset = charset;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.serializer.RedisSerializer#deserialize(byte[])
	 */
	@Override
	public String deserialize(@Nullable byte[] bytes) {
		return (bytes == null ? null : new String(bytes, charset));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.serializer.RedisSerializer#serialize(java.lang.Object)
	 */
	@Override
	public byte[] serialize(@Nullable String string) {
		return (string == null ? null : string.getBytes(charset));
	}
}
