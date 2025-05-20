/*
 * Copyright 2014-2025 the original author or authors.
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

import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.jspecify.annotations.Nullable;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.util.CollectionUtils;

/**
 * Redis client agnostic {@link Cursor} implementation continuously loading additional results from Redis server until
 * reaching its starting point {@code zero}. <br />
 * <strong>Note:</strong> Please note that the {@link ScanCursor} has to be initialized ({@link #open()} prior to usage.
 * Any failures during scanning will {@link #close() close} the cursor and release any associated resources such as
 * connections.
 *
 * @author Christoph Strobl
 * @author Thomas Darimont
 * @author Duobiao Ou
 * @author Marl Paluch
 * @param <T>
 * @since 1.4
 */
public abstract class ScanCursor<T> implements Cursor<T> {

	private CursorState state;
	private CursorId id;
	private Iterator<T> delegate;
	private final ScanOptions scanOptions;
	private long position;

	/**
	 * Crates new {@link ScanCursor} with an initial cursor and {@link ScanOptions#NONE}
	 */
	public ScanCursor() {
		this(ScanOptions.NONE);
	}

	/**
	 * Crates new {@link ScanCursor} with an initial cursor.
	 *
	 * @param options the scan options to apply.
	 */
	public ScanCursor(ScanOptions options) {
		this(CursorId.initial(), options);
	}

	/**
	 * Crates new {@link ScanCursor} with {@link ScanOptions#NONE}
	 *
	 * @param cursorId the cursor Id.
	 * @deprecated since 3.3.0 - Use {@link ScanCursor#ScanCursor(CursorId)} instead.
	 */
	@Deprecated(since = "3.3.0")
	public ScanCursor(long cursorId) {
		this(cursorId, ScanOptions.NONE);
	}

	/**
	 * Crates new {@link ScanCursor} with {@link ScanOptions#NONE}
	 *
	 * @param cursorId the cursor Id.
	 * @since 3.3.0
	 */
	public ScanCursor(CursorId cursorId) {
		this(cursorId, ScanOptions.NONE);
	}

	/**
	 * Crates new {@link ScanCursor}
	 *
	 * @param cursorId the cursor Id.
	 * @param options Defaulted to {@link ScanOptions#NONE} if {@literal null}.
	 * @deprecated since 3.3.0 - Use {@link ScanCursor#ScanCursor(CursorId, ScanOptions)} instead.
	 */
	@Deprecated(since = "3.3.0")
	public ScanCursor(long cursorId, @Nullable ScanOptions options) {
		this(CursorId.of(cursorId), options);
	}

	/**
	 * Crates new {@link ScanCursor}
	 *
	 * @param cursorId the cursor Id.
	 * @param options Defaulted to {@link ScanOptions#NONE} if {@literal null}.
	 * @since 3.3.0
	 */
	public ScanCursor(CursorId cursorId, @Nullable ScanOptions options) {

		this.scanOptions = options != null ? options : ScanOptions.NONE;
		this.id = cursorId;
		this.state = CursorState.READY;
		this.delegate = Collections.emptyIterator();
	}

	private void scan(CursorId cursorId) {

		try {
			processScanResult(doScan(cursorId, this.scanOptions));
		} catch (RuntimeException ex) {
			try {
				close();
			} catch (RuntimeException nested) {
				ex.addSuppressed(nested);
			}
			throw ex;
		}
	}

	/**
	 * Performs the actual scan command using the native client implementation. The given {@literal options} are never
	 * {@literal null}.
	 *
	 * @param cursorId
	 * @param options
	 * @return
	 * @deprecated since 3.3.0, cursorId, can exceed {@link Long#MAX_VALUE}.
	 */
	@Deprecated(since = "3.3.0")
	protected ScanIteration<T> doScan(long cursorId, ScanOptions options) {
		return doScan(CursorId.of(cursorId), scanOptions);
	}

	/**
	 * Performs the actual scan command using the native client implementation. The given {@literal options} are never
	 * {@literal null}.
	 *
	 * @param cursorId
	 * @param options
	 * @return
	 * @since 3.3.0
	 */
	protected ScanIteration<T> doScan(CursorId cursorId, ScanOptions options) {
		return doScan(Long.parseLong(cursorId.getCursorId()), scanOptions);
	}

	/**
	 * Initialize the {@link Cursor} prior to usage.
	 */
	public final ScanCursor<T> open() {

		if (!isReady()) {
			throw new InvalidDataAccessApiUsageException("Cursor already " + state + "; Cannot (re)open it");
		}

		state = CursorState.OPEN;
		doOpen(getId());

		return this;
	}

	/**
	 * Customization hook when calling {@link #open()}.
	 *
	 * @param cursorId
	 * @deprecated since 3.3.0, use {@link #doOpen(CursorId)} instead.
	 */
	@Deprecated(since = "3.3.0", forRemoval = true)
	protected void doOpen(long cursorId) {
		doOpen(CursorId.of(cursorId));
	}

	/**
	 * Customization hook when calling {@link #open()}.
	 *
	 * @param cursorId
	 */
	protected void doOpen(CursorId cursorId) {
		scan(cursorId);
	}

	private void processScanResult(ScanIteration<T> result) {

		id = result.getId();

		if (isFinished(id)) {
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
	@Deprecated(since = "3.3.0", forRemoval = true)
	protected boolean isFinished(long cursorId) {
		return cursorId == 0;
	}

	/**
	 * Check whether {@code cursorId} is finished.
	 *
	 * @param cursorId the cursor Id
	 * @return {@literal true} if the cursor is considered finished, {@literal false} otherwise.s
	 * @since 3.3.0
	 */
	protected boolean isFinished(CursorId cursorId) {
		return CursorId.isInitial(cursorId.getCursorId());
	}

	private void resetDelegate() {
		delegate = Collections.emptyIterator();
	}

	@Override
	public CursorId getId() {
		return id;
	}

	@Override
	public long getCursorId() {
		return Long.parseUnsignedLong(getId().getCursorId());
	}

	@Override
	public boolean hasNext() {

		assertCursorIsOpen();

		while (!delegate.hasNext() && !CursorState.FINISHED.equals(state)) {
			scan(getId());
		}

		if (delegate.hasNext()) {
			return true;
		}

		return !isFinished(id);
	}

	private void assertCursorIsOpen() {

		if (isReady() || isClosed()) {
			throw new InvalidDataAccessApiUsageException("Cannot access closed cursor; Did you forget to call open()");
		}
	}

	@Override
	public T next() {

		assertCursorIsOpen();

		if (!hasNext()) {
			throw new NoSuchElementException("No more elements available for cursor " + id);
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

	@Override
	public void remove() {
		throw new UnsupportedOperationException("Remove is not supported");
	}

	@Override
	public final void close() {

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

	@Override
	public long getPosition() {
		return position;
	}

	/**
	 * @author Thomas Darimont
	 */
	enum CursorState {
		READY, OPEN, FINISHED, CLOSED;
	}
}
