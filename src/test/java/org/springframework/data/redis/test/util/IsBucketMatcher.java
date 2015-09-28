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

public class IsBucketMatcher extends TypeSafeMatcher<Bucket> {

	Map<String, Object> expected = new LinkedHashMap<String, Object>();
	Set<String> without = new LinkedHashSet<String>();

	@Override
	public void describeTo(Description description) {

		if (!expected.isEmpty()) {
			description.appendValueList("Expected Bucket content [{", "},{", "}].", expected.entrySet());
		}
		if (!without.isEmpty()) {
			description.appendValueList("Expected Bucket to not include [", ",", "].", without);
		}

	}

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

	public static IsBucketMatcher isBucket() {
		return new IsBucketMatcher();
	}

	public IsBucketMatcher containingTypeHint(String path, Class<?> type) {

		this.expected.put(path, type);
		return this;
	}

	public IsBucketMatcher containingUtf8String(String path, String value) {

		this.expected.put(path, value);
		return this;
	}

	public IsBucketMatcher containing(String path, byte[] value) {

		this.expected.put(path, value);
		return this;
	}

	public IsBucketMatcher matchingPath(String path, Matcher<byte[]> matcher) {

		this.expected.put(path, matcher);
		return this;
	}

	public IsBucketMatcher containingDateAsMsec(String path, Date date) {

		this.expected.put(path, date);
		return this;
	}

	public IsBucketMatcher without(String path) {
		this.without.add(path);
		return this;
	}

}
