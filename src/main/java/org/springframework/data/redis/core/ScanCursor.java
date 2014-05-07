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
import java.util.NoSuchElementException;

import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.util.CollectionUtils;

/**
 * Redis client agnostic {@link Cursor} implementation continuously loading additional results from Redis server until
 * reaching its starting point {@code zero}. <br />
 * <strong>Note:</strong> Please note that the {@link ScanCursor} has to be initialized ({@link #init()} prior to usage.
 * 
 * @author Christoph Strobl
 * @param <T>
 * @since 1.4
 */
public abstract class ScanCursor<T> implements Cursor<T> {

	private boolean initialized = false;
	private boolean finished = false;
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
		this.cursorId = cursorId;
	}

	private void scan(long cursorId) {

		if (!initialized) {
			initialized = true;
		}

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

	/**
	 * Initialize the {@link Cursor} prior to usage.
	 */
	public ScanCursor<T> init() {
		if (!initialized) {
			scan(cursorId);
		}
		return this;
	}

	private void processScanResult(ScanIteration<T> result) {

		if (result == null) {

			finished = true;
			return;
		}

		cursorId = Long.valueOf(result.getCursorId());
		if (cursorId == 0) {
			this.finished = true;
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

		verifyProperInitialization();

		if (currentIndex < items.size()) {
			return true;
		}
		if (cursorId > 0) {
			return true;
		}
		return false;
	}

	private void verifyProperInitialization() {
		if (!initialized) {
			throw new InvalidDataAccessApiUsageException("Cursor not properly initialized. Did you forget to call init().");
		}
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Iterator#next()
	 */
	@Override
	public T next() {

		verifyProperInitialization();

		if (!finished && (currentIndex + 1 > items.size())) {
			scan(cursorId);
		}

		if (currentIndex + 1 > items.size()) {
			throw new NoSuchElementException("No more elements available for cursor " + this.cursorId + ".");
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
