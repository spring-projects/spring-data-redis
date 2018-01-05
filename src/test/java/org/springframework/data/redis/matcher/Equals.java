/*
 * Copyright 2013-2018 the original author or authors.
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
package org.springframework.data.redis.matcher;

import static org.hamcrest.CoreMatchers.hasItems;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;

/**
 * Custom JUnit {@link Matcher} that exists to properly compare byte arrays, either as individual results or members of
 * a {@link Collection} or {@link LinkedHashMap}. Works the same as assertEquals for all other types of Objects. Make
 * some assumptions about structure of data based on what we typically get back from Redis commands, i.e that Sets or
 * Maps don't contain Collections, but a List might contain a bit of everything (when it comes back from exec() or
 * closePipeline())
 *
 * @author Jennifer Hickey
 */
public class Equals extends BaseMatcher<Object> {

	private Object expected;

	public Equals(Object expected) {
		this.expected = expected;
	}

	public void describeTo(Description description) {
		description.appendValue(expected);
	}

	@SuppressWarnings("rawtypes")
	public boolean matches(Object actual) {
		if (expected instanceof List || expected instanceof LinkedHashSet) {
			return collectionEquals((Collection) expected, (Collection) actual);
		} else if (expected instanceof Set) {
			return setEquals((Set) expected, (Set) actual);
		} else if (expected instanceof LinkedHashMap) {
			return mapEquals((Map) expected, (Map) actual);
		} else {
			return equals(expected, actual);
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private boolean collectionEquals(Collection expected, Collection actual) {
		if (expected.size() != actual.size()) {
			return false;
		}
		Iterator<Object> actualItr = actual.iterator();
		Iterator<Object> expectedItr = expected.iterator();
		while (expectedItr.hasNext()) {
			Object expectedObj = expectedItr.next();
			if (expectedObj instanceof List || expectedObj instanceof LinkedHashSet) {
				if (!(collectionEquals((Collection) expectedObj, (Collection) actualItr.next()))) {
					return false;
				}
			} else if (expectedObj instanceof Set) {
				if (!(setEquals((Set) expectedObj, (Set) actualItr.next()))) {
					return false;
				}
			} else if (expectedObj instanceof Map) {
				if (!(mapEquals((Map) expectedObj, (Map) actualItr.next()))) {
					return false;
				}
			} else if (!equals(expectedObj, actualItr.next())) {
				return false;
			}
		}
		return true;
	}

	@SuppressWarnings("rawtypes")
	private boolean setEquals(Set expected, Set actual) {
		if (expected.size() != actual.size()) {
			return false;
		}
		return hasItems(actual.toArray()).matches(expected);
	}

	@SuppressWarnings("rawtypes")
	private boolean mapEquals(Map expected, Map actual) {
		if (expected.size() != actual.size()) {
			return false;
		}
		Iterator actualItr = actual.entrySet().iterator();
		Iterator expectedItr = expected.entrySet().iterator();
		while (expectedItr.hasNext()) {
			Map.Entry expectedEntry = (Map.Entry) expectedItr.next();
			Map.Entry actualEntry = (Map.Entry) actualItr.next();
			if (!(equals(expectedEntry.getKey(), actualEntry.getKey()))
					|| !(equals(expectedEntry.getValue(), actualEntry.getValue()))) {
				return false;
			}
		}
		return true;
	}

	private boolean equals(Object expected, Object actual) {
		if (expected == null) {
			return actual == null;
		}
		if (expected instanceof byte[]) {
			return Arrays.equals((byte[]) expected, (byte[]) actual);
		}
		return expected.equals(actual);
	}
}
