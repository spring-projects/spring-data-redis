/*
 * Copyright 2011-present the original author or authors.
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
package org.springframework.data.redis.connection.util;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.jspecify.annotations.Nullable;
import org.springframework.data.redis.util.ByteUtils;

/**
 * Simple wrapper class used for wrapping arrays so they can be used as keys inside maps.
 *
 * @author Costin Leau
 */
public class ByteArrayWrapper implements Comparable<ByteArrayWrapper> {

	private final byte[] array;
	private final int hashCode;

	public ByteArrayWrapper(ByteBuffer buffer) {
		this(ByteUtils.getBytes(buffer.asReadOnlyBuffer()));
	}

	public ByteArrayWrapper(byte[] array) {
		this.array = array;
		this.hashCode = Arrays.hashCode(array);
	}

	public boolean equals(@Nullable Object obj) {
		if (obj instanceof ByteArrayWrapper other) {
			return Arrays.equals(array, other.array);
		}

		return false;
	}

	public int hashCode() {
		return hashCode;
	}

	/**
	 * Returns the array.
	 *
	 * @return Returns the array
	 */
	public byte[] getArray() {
		return array;
	}

	@Override
	public String toString() {
		return ByteUtils.toString(array);
	}

	@Override
	public int compareTo(ByteArrayWrapper o) {
		return Arrays.compare(this.array, o.array);
	}
}
