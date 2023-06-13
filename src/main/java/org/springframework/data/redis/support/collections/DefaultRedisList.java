/*
 * Copyright 2011-2023 the original author or authors.
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
package org.springframework.data.redis.support.collections;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;

import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.connection.RedisListCommands;
import org.springframework.data.redis.core.BoundListOperations;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Default implementation for {@link RedisList}. Suitable for not just lists, but also queues (FIFO ordering) or stacks
 * (LIFO ordering) and deques (or double ended queues). Allows the maximum size (or the cap) to be specified to prevent
 * the list from over growing. Note that all write operations will execute immediately, whether a cap is specified or
 * not - the list will always accept new items (trimming the tail after each insert in case of capped collections).
 *
 * @author Costin Leau
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public class DefaultRedisList<E> extends AbstractRedisCollection<E> implements RedisList<E> {

	private final BoundListOperations<String, E> listOps;

	private volatile int maxSize = 0;
	private volatile boolean capped = false;

	private class DefaultRedisListIterator extends RedisIterator<E> {

		public DefaultRedisListIterator(Iterator<E> delegate) {
			super(delegate);
		}

		@Override
		protected void removeFromRedisStorage(E item) {
			DefaultRedisList.this.remove(item);
		}
	}

	/**
	 * Constructs a new, uncapped {@link DefaultRedisList} instance.
	 *
	 * @param key Redis key of this list.
	 * @param operations {@link RedisOperations} for the value type of this list.
	 */
	public DefaultRedisList(String key, RedisOperations<String, E> operations) {
		this(operations.boundListOps(key));
	}

	/**
	 * Constructs a new {@link DefaultRedisList} instance.
	 *
	 * @param key Redis key of this list.
	 * @param operations {@link RedisOperations} for the value type of this list.
	 * @param maxSize
	 * @since 2.6
	 */
	public DefaultRedisList(String key, RedisOperations<String, E> operations, int maxSize) {
		this(operations.boundListOps(key), maxSize);
	}

	/**
	 * Constructs a new, uncapped {@link DefaultRedisList} instance.
	 *
	 * @param boundOps {@link BoundListOperations} for the value type of this list.
	 */
	public DefaultRedisList(BoundListOperations<String, E> boundOps) {
		this(boundOps, 0);
	}

	/**
	 * Constructs a new {@link DefaultRedisList} instance.
	 *
	 * @param boundOps {@link BoundListOperations} for the value type of this list.
	 * @param maxSize
	 */
	public DefaultRedisList(BoundListOperations<String, E> boundOps, int maxSize) {

		super(boundOps.getKey(), boundOps.getOperations());

		listOps = boundOps;
		setMaxSize(maxSize);
	}

	/**
	 * Sets the maximum size of the (capped) list. A value of 0 means unlimited.
	 *
	 * @param maxSize list maximum size
	 */
	public void setMaxSize(int maxSize) {

		this.maxSize = maxSize;
		capped = (maxSize > 0);
	}

	@Override
	public E moveFirstTo(RedisList<E> destination, RedisListCommands.Direction destinationPosition) {

		Assert.notNull(destination, "Destination must not be null");
		Assert.notNull(destinationPosition, "Destination position must not be null");

		E result = listOps.move(RedisListCommands.Direction.first(), destination.getKey(), destinationPosition);
		potentiallyCap(destination);
		return result;
	}

	@Override
	public E moveFirstTo(RedisList<E> destination, RedisListCommands.Direction destinationPosition, long timeout,
			TimeUnit unit) {

		Assert.notNull(destination, "Destination must not be null");
		Assert.notNull(destinationPosition, "Destination position must not be null");
		Assert.notNull(unit, "TimeUnit must not be null");

		E result = listOps.move(RedisListCommands.Direction.first(), destination.getKey(), destinationPosition, timeout,
				unit);
		potentiallyCap(destination);
		return result;
	}

	@Override
	public E moveLastTo(RedisList<E> destination, RedisListCommands.Direction destinationPosition) {

		Assert.notNull(destination, "Destination must not be null");
		Assert.notNull(destinationPosition, "Destination position must not be null");

		E result = listOps.move(RedisListCommands.Direction.last(), destination.getKey(), destinationPosition);
		potentiallyCap(destination);
		return result;
	}

	@Override
	public E moveLastTo(RedisList<E> destination, RedisListCommands.Direction destinationPosition, long timeout,
			TimeUnit unit) {

		Assert.notNull(destination, "Destination must not be null");
		Assert.notNull(destinationPosition, "Destination position must not be null");
		Assert.notNull(unit, "TimeUnit must not be null");

		E result = listOps.move(RedisListCommands.Direction.last(), destination.getKey(), destinationPosition, timeout,
				unit);
		potentiallyCap(destination);
		return result;
	}

	private void potentiallyCap(RedisList<E> destination) {
		if (destination instanceof DefaultRedisList) {
			((DefaultRedisList<Object>) destination).cap();
		}
	}

	@Override
	public List<E> range(long start, long end) {
		return listOps.range(start, end);
	}

	@Override
	public RedisList<E> trim(int start, int end) {
		listOps.trim(start, end);
		return this;
	}

	@Override
	public RedisList<E> trim(long start, long end) {
		listOps.trim(start, end);
		return this;
	}

	@Override
	public Iterator<E> iterator() {
		List<E> list = content();
		checkResult(list);
		return new DefaultRedisListIterator(list.iterator());
	}

	@Override
	public int size() {
		Long size = listOps.size();
		checkResult(size);
		return size.intValue();
	}

	@Override
	public boolean add(E value) {
		listOps.rightPush(value);
		cap();
		return true;
	}

	@Override
	public void clear() {
		listOps.trim(size() + 1, 0);
	}

	@Override
	public boolean remove(Object o) {
		Long result = listOps.remove(1, o);
		return (result != null && result.longValue() > 0);
	}

	@Override
	public void add(int index, E element) {
		if (index == 0) {
			listOps.leftPush(element);
			cap();
			return;
		}

		int size = size();

		if (index == size()) {
			listOps.rightPush(element);
			cap();
			return;
		}

		if (index < 0 || index > size) {
			throw new IndexOutOfBoundsException();
		}

		throw new IllegalArgumentException("Redis supports insertion only at the beginning or the end of the list");
	}

	@Override
	public boolean addAll(int index, Collection<? extends E> c) {
		// insert collection in reverse
		if (index == 0) {
			Collection<? extends E> reverseC = CollectionUtils.reverse(c);

			for (E e : reverseC) {
				listOps.leftPush(e);
				cap();
			}
			return true;
		}

		int size = size();

		if (index == size()) {
			for (E e : c) {
				listOps.rightPush(e);
				cap();
			}
			return true;
		}

		if (index < 0 || index > size) {
			throw new IndexOutOfBoundsException();
		}

		throw new IllegalArgumentException("Redis supports insertion only at the beginning or the end of the list");
	}

	@Override
	public E get(int index) {
		if (index < 0 || index > size()) {
			throw new IndexOutOfBoundsException();
		}
		return listOps.index(index);
	}

	@Override
	public int indexOf(Object o) {

		Long index = listOps.indexOf((E) o);
		return index != null ? index.intValue() : -1;
	}

	@Override
	public int lastIndexOf(Object o) {

		Long index = listOps.lastIndexOf((E) o);
		return index != null ? index.intValue() : -1;
	}

	@Override
	public ListIterator<E> listIterator() {
		throw new UnsupportedOperationException();
	}

	@Override
	public ListIterator<E> listIterator(int index) {
		throw new UnsupportedOperationException();
	}

	@Override
	public E remove(int index) {
		throw new UnsupportedOperationException();
	}

	@Override
	public E set(int index, E e) {

		E object = get(index);
		listOps.set(index, e);
		return object;
	}

	@Override
	public List<E> subList(int fromIndex, int toIndex) {
		throw new UnsupportedOperationException();
	}

	//
	// Queue methods
	//

	@Override
	public E element() {
		E value = peek();
		if (value == null) {
			throw new NoSuchElementException();
		}

		return value;
	}

	@Override
	public boolean offer(E e) {
		listOps.rightPush(e);
		cap();
		return true;
	}

	@Override
	@Nullable
	public E peek() {
		return listOps.index(0);
	}

	@Override
	@Nullable
	public E poll() {
		return listOps.leftPop();
	}

	@Override
	public E remove() {

		E value = poll();
		if (value == null) {
			throw new NoSuchElementException();
		}

		return value;
	}

	//
	// Dequeue
	//

	@Override
	public void addFirst(E e) {
		listOps.leftPush(e);
		cap();
	}

	@Override
	public void addLast(E e) {
		add(e);
	}

	@Override
	public Iterator<E> descendingIterator() {
		List<E> content = content();
		Collections.reverse(content);
		return new DefaultRedisListIterator(content.iterator());
	}

	@Override
	public E getFirst() {
		return element();
	}

	@Override
	public E getLast() {
		E e = peekLast();
		if (e == null) {
			throw new NoSuchElementException();
		}
		return e;
	}

	@Override
	public boolean offerFirst(E e) {
		addFirst(e);
		return true;
	}

	@Override
	public boolean offerLast(E e) {
		addLast(e);
		return true;
	}

	@Override
	@Nullable
	public E peekFirst() {
		return peek();
	}

	@Override
	@Nullable
	public E peekLast() {
		return listOps.index(-1);
	}

	@Override
	@Nullable
	public E pollFirst() {
		return poll();
	}

	@Override
	@Nullable
	public E pollLast() {
		return listOps.rightPop();
	}

	@Override
	public E pop() {

		E e = poll();
		if (e == null) {
			throw new NoSuchElementException();
		}
		return e;
	}

	@Override
	public void push(E e) {
		addFirst(e);
	}

	@Override
	public E removeFirst() {
		return pop();
	}

	@Override
	public boolean removeFirstOccurrence(Object o) {
		return remove(o);
	}

	@Override
	public E removeLast() {
		E e = pollLast();
		if (e == null) {
			throw new NoSuchElementException();
		}
		return e;
	}

	@Override
	public boolean removeLastOccurrence(Object o) {
		Long result = listOps.remove(-1, o);
		return (result != null && result.longValue() > 0);
	}

	//
	// BlockingQueue
	//
	@Override
	public int drainTo(Collection<? super E> c, int maxElements) {
		if (this.equals(c)) {
			throw new IllegalArgumentException("Cannot drain a queue to itself");
		}

		int size = size();
		int loop = (size >= maxElements ? maxElements : size);

		for (int index = 0; index < loop; index++) {
			c.add(poll());
		}

		return loop;
	}

	@Override
	public int drainTo(Collection<? super E> c) {
		return drainTo(c, size());
	}

	@Override
	public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {
		return offer(e);
	}

	@Override
	@Nullable
	public E poll(long timeout, TimeUnit unit) throws InterruptedException {
		return listOps.leftPop(timeout, unit);
	}

	@Override
	public void put(E e) throws InterruptedException {
		offer(e);
	}

	@Override
	public int remainingCapacity() {
		return Integer.MAX_VALUE;
	}

	@Override
	@Nullable
	public E take() throws InterruptedException {
		return poll(0, TimeUnit.SECONDS);
	}

	//
	// BlockingDeque
	//

	@Override
	public boolean offerFirst(E e, long timeout, TimeUnit unit) throws InterruptedException {
		return offerFirst(e);
	}

	@Override
	public boolean offerLast(E e, long timeout, TimeUnit unit) throws InterruptedException {
		return offerLast(e);
	}

	@Override
	@Nullable
	public E pollFirst(long timeout, TimeUnit unit) throws InterruptedException {
		return poll(timeout, unit);
	}

	@Override
	@Nullable
	public E pollLast(long timeout, TimeUnit unit) throws InterruptedException {
		return listOps.rightPop(timeout, unit);
	}

	@Override
	public void putFirst(E e) throws InterruptedException {
		add(e);
	}

	@Override
	public void putLast(E e) throws InterruptedException {
		put(e);
	}

	@Override
	@Nullable
	public E takeFirst() throws InterruptedException {
		return take();
	}

	@Override
	@Nullable
	public E takeLast() throws InterruptedException {
		return pollLast(0, TimeUnit.SECONDS);
	}

	@Override
	public DataType getType() {
		return DataType.LIST;
	}

	private List<E> content() {
		return listOps.range(0, -1);
	}

	private void cap() {
		if (capped) {
			listOps.trim(0, maxSize - 1);
		}
	}

	@Override
	public RedisList<E> reversed() {

		DefaultRedisList<E> redisList = new DefaultRedisList<>(getKey(), getOperations(), this.maxSize);

		StreamSupport.stream(this.spliterator(), false)
			.forEach(redisList::addFirst);

		return redisList;
	}
}
