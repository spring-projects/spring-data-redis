/*
 * Copyright 2011-2018 the original author or authors.
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
package org.springframework.data.redis.connection.util;

import java.util.Arrays;

/**
 * Simple wrapper class used for wrapping arrays so they can be used as keys inside maps.
 *
 * @author Costin Leau
 */
public class ByteArrayWrapper {

	private final byte[] array;
	private final int hashCode;

	public ByteArrayWrapper(byte[] array) {
		this.array = array;
		this.hashCode = Arrays.hashCode(array);
	}

	public boolean equals(Object obj) {
		if (obj instanceof ByteArrayWrapper) {
			return Arrays.equals(array, ((ByteArrayWrapper) obj).array);
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
}
