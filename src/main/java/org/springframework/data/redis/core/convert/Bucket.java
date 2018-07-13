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
package org.springframework.data.redis.core.convert;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Bucket is the data bag for Redis hash structures to be used with {@link RedisData}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 1.7
 */
public class Bucket {

	/**
	 * Encoding used for converting {@link Byte} to and from {@link String}.
	 */
	public static final Charset CHARSET = StandardCharsets.UTF_8;

	private final Map<String, byte[]> data;

	/**
	 * Creates new empty bucket
	 */
	public Bucket() {
		data = new LinkedHashMap<>();
	}

	Bucket(Map<String, byte[]> data) {

		Assert.notNull(data, "Inital data must not be null!");
		this.data = new LinkedHashMap<>(data.size());
		this.data.putAll(data);
	}

	/**
	 * Add {@link String} representation of property dot path with given value.
	 *
	 * @param path must not be {@literal null} or {@link String#isEmpty()}.
	 * @param value can be {@literal null}.
	 */
	public void put(String path, byte[] value) {

		Assert.hasText(path, "Path to property must not be null or empty.");
		data.put(path, value);
	}

	/**
	 * Remove the property at property dot {@code path}.
	 *
	 * @param path must not be {@literal null} or {@link String#isEmpty()}.
	 */
	public void remove(String path) {

		Assert.hasText(path, "Path to property must not be null or empty.");
		data.remove(path);
	}

	/**
	 * Get value assigned with path.
	 *
	 * @param path path must not be {@literal null} or {@link String#isEmpty()}.
	 * @return {@literal null} if not set.
	 */
	@Nullable
	public byte[] get(String path) {

		Assert.hasText(path, "Path to property must not be null or empty.");
		return data.get(path);
	}

	/**
	 * A set view of the mappings contained in this bucket.
	 *
	 * @return never {@literal null}.
	 */
	public Set<Entry<String, byte[]>> entrySet() {
		return data.entrySet();
	}

	/**
	 * @return {@literal true} when no data present in {@link Bucket}.
	 */
	public boolean isEmpty() {
		return data.isEmpty();
	}

	/**
	 * @return the number of key-value mappings of the {@link Bucket}.
	 */
	public int size() {
		return data.size();
	}

	/**
	 * @return never {@literal null}.
	 */
	public Collection<byte[]> values() {
		return data.values();
	}

	/**
	 * @return never {@literal null}.
	 */
	public Set<String> keySet() {
		return data.keySet();
	}

	/**
	 * Key/value pairs contained in the {@link Bucket}.
	 *
	 * @return never {@literal null}.
	 */
	public Map<String, byte[]> asMap() {
		return Collections.unmodifiableMap(this.data);
	}

	/**
	 * Extracts a bucket containing key/value pairs with the {@code prefix}.
	 *
	 * @param prefix
	 * @return
	 */
	public Bucket extract(String prefix) {

		Bucket partial = new Bucket();
		for (Map.Entry<String, byte[]> entry : data.entrySet()) {
			if (entry.getKey().startsWith(prefix)) {
				partial.put(entry.getKey(), entry.getValue());
			}
		}

		return partial;
	}

	/**
	 * Get all the keys matching a given path.
	 *
	 * @param path the path to look for. Can be {@literal null}.
	 * @return all keys if path is {@null} or empty.
	 */
	public Set<String> extractAllKeysFor(String path) {

		if (!StringUtils.hasText(path)) {
			return keySet();
		}

		Pattern pattern = Pattern.compile("(" + Pattern.quote(path) + ")\\.\\[.*?\\]");

		Set<String> keys = new LinkedHashSet<>();
		for (Map.Entry<String, byte[]> entry : data.entrySet()) {

			Matcher matcher = pattern.matcher(entry.getKey());
			if (matcher.find()) {
				keys.add(matcher.group());
			}
		}

		return keys;
	}

	/**
	 * Get keys and values in binary format.
	 *
	 * @return never {@literal null}.
	 */
	public Map<byte[], byte[]> rawMap() {

		Map<byte[], byte[]> raw = new LinkedHashMap<>(data.size());
		for (Map.Entry<String, byte[]> entry : data.entrySet()) {
			if (entry.getValue() != null) {
				raw.put(entry.getKey().getBytes(CHARSET), entry.getValue());
			}
		}
		return raw;
	}

	/**
	 * Get the {@link BucketPropertyPath} leading to the current {@link Bucket}.
	 *
	 * @return new instance of {@link BucketPropertyPath}.
	 * @since 2.1
	 */
	public BucketPropertyPath getPath() {
		return BucketPropertyPath.from(this);
	}

	/**
	 * Get the {@link BucketPropertyPath} for a given property within the current {@link Bucket}.
	 *
	 * @param property the property to look up.
	 * @return new instance of {@link BucketPropertyPath}.
	 * @since 2.1
	 */
	public BucketPropertyPath getPropertyPath(String property) {
		return BucketPropertyPath.from(this, property);
	}

	/**
	 * Creates a new Bucket from a given raw map.
	 *
	 * @param source can be {@literal null}.
	 * @return never {@literal null}.
	 */
	public static Bucket newBucketFromRawMap(Map<byte[], byte[]> source) {

		Bucket bucket = new Bucket();

		for (Map.Entry<byte[], byte[]> entry : source.entrySet()) {
			bucket.put(new String(entry.getKey(), CHARSET), entry.getValue());
		}
		return bucket;
	}

	/**
	 * Creates a new Bucket from a given {@link String} map.
	 *
	 * @param source can be {@literal null}.
	 * @return never {@literal null}.
	 */
	public static Bucket newBucketFromStringMap(Map<String, String> source) {

		Bucket bucket = new Bucket();

		for (Map.Entry<String, String> entry : source.entrySet()) {
			bucket.put(entry.getKey(),
					StringUtils.hasText(entry.getValue()) ? entry.getValue().getBytes(CHARSET) : new byte[] {});
		}
		return bucket;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "Bucket [data=" + safeToString() + "]";
	}

	private String safeToString() {

		Map<String, String> serialized = new LinkedHashMap<>();
		for (Map.Entry<String, byte[]> entry : data.entrySet()) {
			if (entry.getValue() != null) {
				serialized.put(entry.getKey(), toUtf8String(entry.getValue()));
			} else {
				serialized.put(entry.getKey(), null);
			}
		}
		return serialized.toString();
	}

	@Nullable
	private String toUtf8String(byte[] raw) {

		try {
			return new String(raw, CHARSET);
		} catch (Exception e) {
			// Ignore this one
		}
		return null;
	}

	/**
	 * Value object representing a path within a {@link Bucket}. Paths can be either top-level (if the {@code prefix} is
	 * {@literal null} or empty) or nested with a given {@code prefix}.
	 *
	 * @author Mark Paluch
	 * @since 2.1
	 */
	@AllArgsConstructor(access = AccessLevel.PRIVATE)
	@Getter
	public static class BucketPropertyPath {

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
}
