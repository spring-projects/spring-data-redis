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
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.util.CollectionUtils;

/**
 * Redis client agnostic {@link Cursor} implementation continuously loading additional results from Redis server until
 * reaching its starting point {@code zero}. <br />
 * <strong>Note:</strong> Please note that the {@link ScanCursor} has to be initialized ({@link #open()} prior to usage.
 * 
 * @author Christoph Strobl
 * @param <T>
 * @since 1.4
 */
public abstract class ScanCursor<T> implements Cursor<T> {

	private boolean finished = false;
	private boolean closed = true;
	private long cursorId;
	private Iterator<T> delegate = Collections.emptyIterator();
	private final ScanOptions scanOptions;
	private long position = 0;

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
	public final ScanCursor<T> open() {

		if (isClosed()) {
			doOpen(cursorId);
			closed = false;
		}
		return this;
	}

	/**
	 * Customization hook when calling {@link #open()}.
	 * 
	 * @param cursorId
	 */
	protected void doOpen(long cursorId) {
		scan(cursorId);
	}

	private void processScanResult(ScanIteration<T> result) {

		if (result == null) {

			resetDelegate();
			finished = true;
			return;
		}

		cursorId = Long.valueOf(result.getCursorId());

		if (cursorId == 0) {
			this.finished = true;
		}

		if (!CollectionUtils.isEmpty(result.getItems())) {
			delegate = result.iterator();
		} else {
			resetDelegate();
		}
	}

	private void resetDelegate() {
		delegate = Collections.emptyIterator();
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

		if (delegate.hasNext()) {
			return true;
		}
		if (cursorId > 0) {
			return true;
		}
		return false;
	}

	private void verifyProperInitialization() {
		if (isClosed()) {
			throw new InvalidDataAccessApiUsageException("Cannot access closed cursor. Did you forget to call open().");
		}
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Iterator#next()
	 */
	@Override
	public T next() {

		verifyProperInitialization();

		if (!finished && !delegate.hasNext()) {
			scan(cursorId);
		}

		if (!delegate.hasNext()) {
			throw new NoSuchElementException("No more elements available for cursor " + this.cursorId + ".");
		}

		T next = doNext(delegate);
		position++;
		return next;
	}

	/**
	 * Fetch the next item from the underlying {@link Iterable}.
	 * 
	 * @param source
	 * @return
	 */
	protected T doNext(Iterator<T> source) {
		return source.next();
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Iterator#remove()
	 */
	@Override
	public void remove() {
		throw new UnsupportedOperationException("Remove is not supported");
	}

	/*
	 * (non-Javadoc)
	 * @see java.io.Closeable#close()
	 */
	@Override
	public final void close() throws IOException {

		closed = true;
		doClose();
	}

	/**
	 * Customization hook for cleaning up resources on when calling {@link #close()}.
	 */
	protected void doClose() {
		resetDelegate();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.Cursor#isClosed()
	 */
	@Override
	public boolean isClosed() {
		return closed;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.Cursor#getPosition()
	 */
	@Override
	public long getPosition() {
		return position;
	}
}
