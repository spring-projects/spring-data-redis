/*
 * Copyright 2014-2021 the original author or authors.
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
package org.springframework.data.redis.core;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

/**
 * Redis client agnostic {@link Cursor} implementation continuously loading additional results from Redis server until
 * reaching its starting point {@code zero}. <br />
 * <strong>Note:</strong> Please note that the {@link ScanCursor} has to be initialized ({@link #open()} prior to usage.
 *
 * @author Christoph Strobl
 * @author Thomas Darimont
 * @author Duobiao Ou
 * @author Marl Paluch
 * @param <T>
 * @since 1.4
 */
public abstract class ScanCursor<T> implements Cursor<T> {

	private @Nullable CursorState state;
	private long cursorId;
	private @Nullable Iterator<T> delegate;
	private @Nullable final ScanOptions scanOptions;
	private long position;
	private final long limit;

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
		this.state = CursorState.READY;
		this.delegate = Collections.<T> emptyList().iterator();
		this.limit = -1;
	}

	/**
	 * Crates a new {@link ScanCursor}.
	 *
	 * @param source
	 * @param limit
	 * @since 2.5
	 */
	private ScanCursor(ScanCursor<T> source, long limit) {

		this.scanOptions = source.scanOptions;
		this.cursorId = source.cursorId;
		this.state = source.state;
		this.delegate = source.delegate;
		this.limit = limit;
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

		if (!isReady()) {
			throw new InvalidDataAccessApiUsageException("Cursor already " + state + ". Cannot (re)open it.");
		}

		state = CursorState.OPEN;
		doOpen(cursorId);

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
			state = CursorState.FINISHED;
			return;
		}

		cursorId = Long.valueOf(result.getCursorId());

		if (isFinished(cursorId)) {
			state = CursorState.FINISHED;
		}

		if (!CollectionUtils.isEmpty(result.getItems())) {
			delegate = result.iterator();
		} else {
			resetDelegate();
		}
	}

	/**
	 * Check whether {@code cursorId} is finished.
	 *
	 * @param cursorId the cursor Id
	 * @return {@literal true} if the cursor is considered finished, {@literal false} otherwise.s
	 * @since 2.1
	 */
	protected boolean isFinished(long cursorId) {
		return cursorId == 0;
	}

	private void resetDelegate() {
		delegate = Collections.<T> emptyList().iterator();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.Cursor#getCursorId()
	 */
	@Override
	public long getCursorId() {
		return cursorId;
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Iterator#hasNext()
	 */
	@Override
	public boolean hasNext() {

		assertCursorIsOpen();

		if (limit != -1 && getPosition() > limit - 1) {
			return false;
		}

		while (!delegate.hasNext() && !CursorState.FINISHED.equals(state)) {
			scan(cursorId);
		}

		if (delegate.hasNext()) {
			return true;
		}

		if (cursorId > 0) {
			return true;
		}

		return false;
	}

	private void assertCursorIsOpen() {

		if (isReady() || isClosed()) {
			throw new InvalidDataAccessApiUsageException("Cannot access closed cursor. Did you forget to call open()?");
		}
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Iterator#next()
	 */
	@Override
	public T next() {

		assertCursorIsOpen();

		if (!hasNext()) {
			throw new NoSuchElementException("No more elements available for cursor " + cursorId + ".");
		}

		T next = moveNext(delegate);
		position++;

		return next;
	}

	/**
	 * Fetch the next item from the underlying {@link Iterable}.
	 *
	 * @param source
	 * @return
	 */
	protected T moveNext(Iterator<T> source) {
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

		try {
			doClose();
		} finally {
			state = CursorState.CLOSED;
		}
	}

	/**
	 * Customization hook for cleaning up resources on when calling {@link #close()}.
	 */
	protected void doClose() {}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.Cursor#isClosed()
	 */
	@Override
	public boolean isClosed() {
		return state == CursorState.CLOSED;
	}

	protected final boolean isReady() {
		return state == CursorState.READY;
	}

	protected final boolean isOpen() {
		return state == CursorState.OPEN;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.Cursor#getPosition()
	 */
	@Override
	public long getPosition() {
		return position;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.Cursor#limit(long)
	 */
	@Override
	public ScanCursor<T> limit(long count) {

		Assert.isTrue(count >= 0, "Count must be greater or equal to zero");

		return new ScanCursorWrapper<>(this, count);
	}

	/**
	 * @author Thomas Darimont
	 */
	enum CursorState {
		READY, OPEN, FINISHED, CLOSED;
	}

	/**
	 * Wrapper for a concrete {@link ScanCursor} forwarding {@link #doScan(long, ScanOptions)}.
	 *
	 * @param <T>
	 * @since 2.5
	 */
	private static class ScanCursorWrapper<T> extends ScanCursor<T> {

		private final ScanCursor<T> delegate;

		public ScanCursorWrapper(ScanCursor<T> delegate, long limit) {
			super(delegate, limit);
			this.delegate = delegate;
		}

		@Override
		protected ScanIteration<T> doScan(long cursorId, ScanOptions options) {
			return delegate.doScan(cursorId, options);
		}
	}
}
