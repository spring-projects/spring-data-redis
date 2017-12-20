/*
 * Copyright 2017 the original author or authors.
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
package org.springframework.data.redis.core.convert;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;

import org.springframework.lang.Nullable;
import org.springframework.util.StringUtils;

/**
 * Value object representing a path within a {@link Bucket}. Paths can be either top-level (if the {@code prefix} is
 * {@literal null} or empty) or nested with a given {@code prefix}.
 *
 * @author Mark Paluch
 * @since 2.1
 */
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Getter
public class BucketPropertyPath {

	private final @NonNull Bucket bucket;
	private final @Nullable String prefix;

	/**
	 * Creates a top-level {@link BucketPropertyPath} given {@link Bucket}.
	 *
	 * @param bucket the bucket, must not be {@literal null}.
	 * @return {@link BucketPropertyPath} within the given {@link Bucket}.
	 */
	public static BucketPropertyPath from(Bucket bucket) {
		return new BucketPropertyPath(bucket, null);
	}

	/**
	 * Creates a {@link BucketPropertyPath} given {@link Bucket} and {@code prefix}. The resulting path is top-level if
	 * {@code prefix} is empty or nested, if {@code prefix} is not empty.
	 *
	 * @param bucket the bucket, must not be {@literal null}.
	 * @param prefix the prefix. Property path is top-level if {@code prefix} is {@literal null} or empty.
	 * @return {@link BucketPropertyPath} within the given {@link Bucket} using {@code prefix}.
	 */
	public static BucketPropertyPath from(Bucket bucket, @Nullable String prefix) {
		return new BucketPropertyPath(bucket, prefix);
	}

	/**
	 * Retrieve a value at {@code key} considering top-level/nesting.
	 *
	 * @param key must not be {@literal null} or empty.
	 * @return the resulting value, may be {@literal null}.
	 */
	@Nullable
	public byte[] get(String key) {
		return bucket.get(getPath(key));
	}

	/**
	 * Write a {@code value} at {@code key} considering top-level/nesting.
	 *
	 * @param key must not be {@literal null} or empty.
	 * @param value the value.
	 */
	public void put(String key, byte[] value) {
		bucket.put(getPath(key), value);
	}

	private String getPath(String key) {
		return StringUtils.hasText(prefix) ? prefix + "." + key : key;
	}
}
