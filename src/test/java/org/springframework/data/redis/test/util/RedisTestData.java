/*
 * Copyright 2019-2020 the original author or authors.
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
package org.springframework.data.redis.test.util;

import static org.assertj.core.error.ShouldContain.*;

import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

import org.assertj.core.api.AssertProvider;
import org.assertj.core.api.MapAssert;
import org.assertj.core.internal.Failures;

import org.springframework.data.redis.core.convert.Bucket;
import org.springframework.data.redis.core.convert.RedisData;

/**
 * {@link AssertProvider} for {@link RedisTestData}.
 *
 * @author Mark Paluch
 */
public class RedisTestData implements AssertProvider<RedisTestData.RedisBucketAssert> {

	private final RedisData redisData;

	RedisTestData(RedisData redisData) {
		this.redisData = redisData;
	}

	public static RedisTestData from(RedisData data) {
		return new RedisTestData(data);
	}

	@Override
	public RedisBucketAssert assertThat() {
		return new RedisBucketAssert(redisData);
	}

	public Bucket getBucket() {
		return redisData.getBucket();
	}

	public String getId() {
		return redisData.getId();
	}

	public RedisData getRedisData() {
		return redisData;
	}

	public static class RedisBucketAssert extends MapAssert<String, String> {

		private final Failures failures = Failures.instance();

		private final RedisData redisData;

		RedisBucketAssert(RedisData redisData) {

			super(toStringMap(redisData.getBucket().asMap()));
			this.redisData = redisData;
		}

		/**
		 * Checks for presence of type hint at given path.
		 *
		 * @param path
		 * @param type
		 * @return
		 */
		public RedisBucketAssert containingTypeHint(String path, Class<?> type) {

			isNotNull();

			String hint = String.format("Type hint for <%s> at <%s>", type.getName(), path);

			if (!actual.containsKey(path)) {

				throw failures.failure(info, shouldContain(actual, hint, hint));
			}

			String string = getString(path);

			if (!string.equals(type.getName())) {
				throw failures.failure(info, shouldContain(actual, hint, hint));
			}

			return this;
		}

		/**
		 * Checks for presence of equivalent String value at path.
		 *
		 * @param path
		 * @param value
		 * @return
		 */
		public RedisBucketAssert containsEntry(String path, String value) {
			super.containsEntry(path, value);
			return this;
		}

		/**
		 * Checks for presence of equivalent time in msec value at path.
		 *
		 * @param path
		 * @param date
		 * @return
		 */
		public RedisBucketAssert containsEntry(String path, Date date) {
			return containsEntry(path, "" + date.getTime());
		}

		/**
		 * Checks given path is not present.
		 *
		 * @param path
		 * @return
		 */
		public RedisBucketAssert without(String path) {

			return this;
		}

		private String getString(String path) {
			return actual.get(path);
		}

	}


	private static Map<String, String> toStringMap(Map<String, byte[]> source) {

		Map<String, String> converted = new LinkedHashMap<>();

		source.forEach((k, v) -> converted.put(k, new String(v, StandardCharsets.UTF_8)));

		return converted;
	}

	@Override
	public String toString() {
		return toStringMap(getBucket().asMap()).toString();
	}
}
