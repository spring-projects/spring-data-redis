/*
 * Copyright 2014 the original author or authors.
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.springframework.util.CollectionUtils;

/**
 * Redis client agnostic {@link Cursor} implementation continuously loading additional results from Redis server until
 * reaching its starting point {@code zero}.
 * 
 * @author Christoph Strobl
 * @param <T>
 * @since 1.3
 */
public abstract class ScanCursor<T> implements Cursor<T> {

	private int currentIndex = 0;
	private long cursorId;
	private List<T> items = new ArrayList<T>();
	private final ScanOptions scanOptions;

	/**
	 * Crates new {@link ScanCursor} with {@code id=0} and {@link ScanOptions#NONE}
	 */
	public ScanCursor() {
		this(ScanOptions.NONE);
	}

	/**
	 * Crates new {@link ScanCursor} with {@code id=0}.
	 * 
	 * @param options
	 */
	public ScanCursor(ScanOptions options) {
		this(0, options);
	}

	/**
	 * Crates new {@link ScanCursor} with {@link ScanOptions#NONE}
	 * 
	 * @param cursorId
	 */
	public ScanCursor(long cursorId) {
		this(cursorId, ScanOptions.NONE);
	}

	/**
	 * Crates new {@link ScanCursor}
	 * 
	 * @param cursorId
	 * @param options Defaulted to {@link ScanOptions#NONE} if nulled.
	 */
	public ScanCursor(long cursorId, ScanOptions options) {

		this.scanOptions = options != null ? options : ScanOptions.NONE;
		scan(cursorId);
	}

	private void scan(long cursorId) {

		ScanIteration<T> result = doScan(cursorId, this.scanOptions);
		processScanResult(result);
	}

	/**
	 * Performs the actual scan command using the native client implementation. The given {@literal options} are never
	 * {@code null}.
	 * 
	 * @param cursorId
	 * @param options
	 * @return
	 */
	protected abstract ScanIteration<T> doScan(long cursorId, ScanOptions options);

	private void processScanResult(ScanIteration<T> result) {

		cursorId = Long.valueOf(result.getCursorId());
		if (cursorId == 0) {
			cursorId = -1;
		}

		if (!CollectionUtils.isEmpty(result.getItems())) {
			items.addAll(result.getItems());
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.Cursor#getCursorId()
	 */
	@Override
	public long getCursorId() {
		return this.cursorId;
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Iterator#hasNext()
	 */
	@Override
	public boolean hasNext() {

		if (currentIndex < items.size()) {
			return true;
		}
		if (cursorId > 0) {
			return true;
		}
		return false;
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Iterator#next()
	 */
	@Override
	public T next() {

		if (cursorId > -1 && (currentIndex + 1 > items.size())) {
			scan(cursorId);
		}

		return items.get(currentIndex++);
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Iterator#remove()
	 */
	@Override
	public void remove() {
		throw new UnsupportedOperationException("Remove is not supported");
	}

	@Override
	public void close() throws IOException {
		// nothing to do
	}

}
