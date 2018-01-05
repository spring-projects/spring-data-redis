/*
 * Copyright 2014-2018 the original author or authors.
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
package org.springframework.data.redis.core;

import org.springframework.lang.Nullable;

/**
 * @author Christoph Strobl
 * @param <T>
 * @since 1.4
 */
public abstract class KeyBoundCursor<T> extends ScanCursor<T> {

	private byte[] key;

	/**
	 * Crates new {@link ScanCursor}
	 *
	 * @param cursorId
	 * @param options Defaulted to {@link ScanOptions#NONE} if nulled.
	 */
	public KeyBoundCursor(byte[] key, long cursorId, @Nullable ScanOptions options) {
		super(cursorId, options != null ? options : ScanOptions.NONE);
		this.key = key;
	}

	protected ScanIteration<T> doScan(long cursorId, ScanOptions options) {
		return doScan(this.key, cursorId, options);
	}

	protected abstract ScanIteration<T> doScan(byte[] key, long cursorId, ScanOptions options);

	public byte[] getKey() {
		return key;
	}

}
