/*
 * Copyright 2018-2019 the original author or authors.
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
package org.springframework.data.redis.connection.stream;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import org.springframework.util.Assert;

/**
 * Value object representing a Stream Id with its offset.
 * 
 * @author Mark Paluch
 * @see 2.2
 */
@EqualsAndHashCode
@ToString
@Getter
public final class StreamOffset<K> {

	private final K key;
	private final ReadOffset offset;

	private StreamOffset(K key, ReadOffset offset) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(offset, "ReadOffset must not be null");

		this.key = key;
		this.offset = offset;
	}

	/**
	 * Create a {@link StreamOffset} given {@code key} and {@link ReadOffset}.
	 *
	 * @param stream the stream key.
	 * @param readOffset the {@link ReadOffset} to use.
	 * @return new instance of {@link StreamOffset}.
	 */
	public static <K> StreamOffset<K> create(K stream, ReadOffset readOffset) {
		return new StreamOffset<>(stream, readOffset);
	}

	/**
	 * Create a {@link StreamOffset} given {@code key} starting at {@link ReadOffset#latest()}.
	 *
	 * @param stream the stream key.
	 * @param <K>
	 * @return new instance of {@link StreamOffset}.
	 */
	public static <K> StreamOffset<K> latest(K stream) {
		return new StreamOffset<>(stream, ReadOffset.latest());
	}

	/**
	 * Create a {@link StreamOffset} given {@code stream} starting at {@link ReadOffset#from(String)
	 * ReadOffset#from("0-0")}.
	 *
	 * @param stream the stream key.
	 * @param <K>
	 * @return new instance of {@link StreamOffset}.
	 */
	public static <K> StreamOffset<K> fromStart(K stream) {
		return new StreamOffset<>(stream, ReadOffset.from("0-0"));
	}

	/**
	 * Create a {@link StreamOffset} from the given {@link Record#getId() record id} as reference to create the
	 * {@link ReadOffset#from(String)}.
	 * 
	 * @param reference the record to be used as reference point.
	 * @param <K>
	 * @return new instance of {@link StreamOffset}.
	 */
	public static <K> StreamOffset<K> from(Record<K, ?> reference) {

		Assert.notNull(reference, "Reference record must not be null");

		return create(reference.getStream(), ReadOffset.from(reference.getId()));
	}
}
