/*
 * Copyright 2015-2018 the original author or authors.
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
package org.springframework.data.redis.test.util;

import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.springframework.data.redis.core.convert.Bucket;

/**
 * {@link TypeSafeMatcher} implementation for checking contents of {@link Bucket}.
 *
 * @author Christoph Strobl
 * @since 1.7
 */
public class IsBucketMatcher extends TypeSafeMatcher<Bucket> {

	Map<String, Object> expected = new LinkedHashMap<>();
	Set<String> without = new LinkedHashSet<>();

	/*
	 * (non-Javadoc)
	 * @see org.hamcrest.SelfDescribing#describeTo(org.hamcrest.Description)
	 */
	@Override
	public void describeTo(Description description) {

		if (!expected.isEmpty()) {
			description.appendValueList("Expected Bucket content [{", "},{", "}].", expected.entrySet());
		}
		if (!without.isEmpty()) {
			description.appendValueList("Expected Bucket to not include [", ",", "].", without);
		}

	}

	/*
	 * (non-Javadoc)
	 * @see org.hamcrest.TypeSafeMatcher#matchesSafely(java.lang.Object)
	 */
	@Override
	protected boolean matchesSafely(Bucket bucket) {

		if (bucket == null) {
			return false;
		}

		if (bucket.isEmpty() && expected.isEmpty()) {
			return true;
		}

		for (String notContained : without) {
			byte[] value = bucket.get(notContained);
			if (value != null || (value != null && value.length > 0)) {
				return false;
			}
		}

		for (Map.Entry<String, Object> entry : expected.entrySet()) {

			byte[] actualValue = bucket.get(entry.getKey());
			Object expectedValue = entry.getValue();
			if (expectedValue == null && actualValue != null) {
				return false;
			}

			if (expectedValue != null && actualValue == null) {
				return false;
			}

			if (expectedValue instanceof byte[]) {
				if (!Arrays.equals((byte[]) expectedValue, actualValue)) {
					return false;
				}
			} else if (expectedValue instanceof String) {
				if (!((String) expectedValue).equals(new String(actualValue, Bucket.CHARSET))) {
					return false;
				}
			} else if (expectedValue instanceof Class<?>) {
				if (!((Class<?>) expectedValue).getName().equals(new String(actualValue, Bucket.CHARSET))) {
					return false;
				}
			} else if (expectedValue instanceof Date) {
				if (((Date) expectedValue).getTime() != Long.valueOf(new String(actualValue, Bucket.CHARSET)).longValue()) {
					return false;
				}
			}

			else if (expectedValue instanceof Matcher<?>) {
				if (!((Matcher<byte[]>) expectedValue).matches(actualValue)) {
					return false;
				}
			} else {
				if (!(expectedValue.toString()).equals(new String(actualValue, Bucket.CHARSET))) {
					return false;
				}
			}
		}

		return true;
	}

	/**
	 * Creates new {@link IsBucketMatcher}.
	 *
	 * @return
	 */
	public static IsBucketMatcher isBucket() {
		return new IsBucketMatcher();
	}

	/**
	 * Checks for presence of type hint at given path.
	 *
	 * @param path
	 * @param type
	 * @return
	 */
	public IsBucketMatcher containingTypeHint(String path, Class<?> type) {

		this.expected.put(path, type);
		return this;
	}

	/**
	 * Checks for presence of equivalent String value at path.
	 *
	 * @param path
	 * @param value
	 * @return
	 */
	public IsBucketMatcher containingUtf8String(String path, String value) {

		this.expected.put(path, value);
		return this;
	}

	/**
	 * Checks for presence of given value at path.
	 *
	 * @param path
	 * @param value
	 * @return
	 */
	public IsBucketMatcher containing(String path, byte[] value) {

		this.expected.put(path, value);
		return this;
	}

	public IsBucketMatcher matchingPath(String path, Matcher<byte[]> matcher) {

		this.expected.put(path, matcher);
		return this;
	}

	/**
	 * Checks for presence of equivalent time in msec value at path.
	 *
	 * @param path
	 * @param date
	 * @return
	 */
	public IsBucketMatcher containingDateAsMsec(String path, Date date) {

		this.expected.put(path, date);
		return this;
	}

	/**
	 * Checks given path is not present.
	 *
	 * @param path
	 * @return
	 */
	public IsBucketMatcher without(String path) {
		this.without.add(path);
		return this;
	}

}
