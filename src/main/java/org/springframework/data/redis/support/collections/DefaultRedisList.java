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
package org.springframework.data.redis.support.collections;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.core.BoundListOperations;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.lang.Nullable;

/**
 * Default implementation for {@link RedisList}. Suitable for not just lists, but also queues (FIFO ordering) or stacks
 * (LIFO ordering) and deques (or double ended queues). Allows the maximum size (or the cap) to be specified to prevent
 * the list from over growing. Note that all write operations will execute immediately, whether a cap is specified or
 * not - the list will always accept new items (trimming the tail after each insert in case of capped collections).
 *
 * @author Costin Leau
 * @author Christoph Strobl
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

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.support.collections.RedisList#range(long, long)
	 */
	@Override
	public List<E> range(long start, long end) {
		return listOps.range(start, end);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.support.collections.RedisList#trim(int, int)
	 */
	@Override
	public RedisList<E> trim(int start, int end) {
		listOps.trim(start, end);
		return this;
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.AbstractCollection#iterator()
	 */
	@Override
	public Iterator<E> iterator() {
		List<E> list = content();
		checkResult(list);
		return new DefaultRedisListIterator(list.iterator());
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.AbstractCollection#size()
	 */
	@Override
	public int size() {
		Long size = listOps.size();
		checkResult(size);
		return size.intValue();
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.AbstractCollection#add(java.lang.Object)
	 */
	@Override
	public boolean add(E value) {
		listOps.rightPush(value);
		cap();
		return true;
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.AbstractCollection#clear()
	 */
	@Override
	public void clear() {
		listOps.trim(size() + 1, 0);
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.AbstractCollection#remove(java.lang.Object)
	 */
	@Override
	public boolean remove(Object o) {
		Long result = listOps.remove(1, o);
		return (result != null && result.longValue() > 0);
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.List#add(int, java.lang.Object)
	 */
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

	/*
	 * (non-Javadoc)
	 * @see java.util.List#addAll(int, java.util.Collection)
	 */
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

	/*
	 * (non-Javadoc)
	 * @see java.util.List#get(int)
	 */
	@Override
	public E get(int index) {
		if (index < 0 || index > size()) {
			throw new IndexOutOfBoundsException();
		}
		return listOps.index(index);
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.List#indexOf(java.lang.Object)
	 */
	@Override
	public int indexOf(Object o) {
		throw new UnsupportedOperationException();
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.List#lastIndexOf(java.lang.Object)
	 */
	@Override
	public int lastIndexOf(Object o) {
		throw new UnsupportedOperationException();
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.List#listIterator()
	 */
	@Override
	public ListIterator<E> listIterator() {
		throw new UnsupportedOperationException();
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.List#listIterator(int)
	 */
	@Override
	public ListIterator<E> listIterator(int index) {
		throw new UnsupportedOperationException();
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.List#remove(int)
	 */
	@Override
	public E remove(int index) {
		throw new UnsupportedOperationException();
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.List#set(int, java.lang.Object)
	 */
	@Override
	public E set(int index, E e) {

		E object = get(index);
		listOps.set(index, e);
		return object;
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.List#subList(int, int)
	 */
	@Override
	public List<E> subList(int fromIndex, int toIndex) {
		throw new UnsupportedOperationException();
	}

	//
	// Queue methods
	//

	/*
	 * (non-Javadoc)
	 * @see java.util.concurrent.BlockingDeque#element()
	 */
	@Override
	public E element() {
		E value = peek();
		if (value == null) {
			throw new NoSuchElementException();
		}

		return value;
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.concurrent.BlockingDeque#offer(java.lang.Object)
	 */
	@Override
	public boolean offer(E e) {
		listOps.rightPush(e);
		cap();
		return true;
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.concurrent.BlockingDeque#peek()
	 */
	@Override
	@Nullable
	public E peek() {
		return listOps.index(0);
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.concurrent.BlockingDeque#poll()
	 */
	@Override
	@Nullable
	public E poll() {
		return listOps.leftPop();
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.concurrent.BlockingDeque#remove()
	 */
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

	/*
	 * (non-Javadoc)
	 * @see java.util.concurrent.BlockingDeque#addFirst(java.lang.Object)
	 */
	@Override
	public void addFirst(E e) {
		listOps.leftPush(e);
		cap();
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.concurrent.BlockingDeque#addLast(java.lang.Object)
	 */
	@Override
	public void addLast(E e) {
		add(e);
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Deque#descendingIterator()
	 */
	@Override
	public Iterator<E> descendingIterator() {
		List<E> content = content();
		Collections.reverse(content);
		return new DefaultRedisListIterator(content.iterator());
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Deque#getFirst()
	 */
	@Override
	public E getFirst() {
		return element();
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Deque#getLast()
	 */
	@Override
	public E getLast() {
		E e = peekLast();
		if (e == null) {
			throw new NoSuchElementException();
		}
		return e;
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.concurrent.BlockingDeque#offerFirst(java.lang.Object)
	 */
	@Override
	public boolean offerFirst(E e) {
		addFirst(e);
		return true;
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.concurrent.BlockingDeque#offerLast(java.lang.Object)
	 */
	@Override
	public boolean offerLast(E e) {
		addLast(e);
		return true;
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Deque#peekFirst()
	 */
	@Override
	@Nullable
	public E peekFirst() {
		return peek();
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Deque#peekLast()
	 */
	@Override
	@Nullable
	public E peekLast() {
		return listOps.index(-1);
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Deque#pollFirst()
	 */
	@Override
	@Nullable
	public E pollFirst() {
		return poll();
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Deque#pollLast()
	 */
	@Override
	@Nullable
	public E pollLast() {
		return listOps.rightPop();
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Deque#pop()
	 */
	@Override
	public E pop() {

		E e = poll();
		if (e == null) {
			throw new NoSuchElementException();
		}
		return e;
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.concurrent.BlockingDeque#push(java.lang.Object)
	 */
	@Override
	public void push(E e) {
		addFirst(e);
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Deque#removeFirst()
	 */
	@Override
	public E removeFirst() {
		return pop();
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.concurrent.BlockingDeque#removeFirstOccurrence(java.lang.Object)
	 */
	@Override
	public boolean removeFirstOccurrence(Object o) {
		return remove(o);
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Deque#removeLast()
	 */
	@Override
	public E removeLast() {
		E e = pollLast();
		if (e == null) {
			throw new NoSuchElementException();
		}
		return e;
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.concurrent.BlockingDeque#removeLastOccurrence(java.lang.Object)
	 */
	@Override
	public boolean removeLastOccurrence(Object o) {
		Long result = listOps.remove(-1, o);
		return (result != null && result.longValue() > 0);
	}

	//
	// BlockingQueue
	//
	/*
	 * (non-Javadoc)
	 * @see java.util.concurrent.BlockingQueue#drainTo(java.util.Collection, int)
	 */
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

	/*
	 * (non-Javadoc)
	 * @see java.util.concurrent.BlockingQueue#drainTo(java.util.Collection)
	 */
	@Override
	public int drainTo(Collection<? super E> c) {
		return drainTo(c, size());
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.concurrent.BlockingDeque#offer(java.lang.Object, long, java.util.concurrent.TimeUnit)
	 */
	@Override
	public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {
		return offer(e);
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.concurrent.BlockingDeque#poll(long, java.util.concurrent.TimeUnit)
	 */
	@Override
	@Nullable
	public E poll(long timeout, TimeUnit unit) throws InterruptedException {
		return listOps.leftPop(timeout, unit);
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.concurrent.BlockingDeque#put(java.lang.Object)
	 */
	@Override
	public void put(E e) throws InterruptedException {
		offer(e);
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.concurrent.BlockingQueue#remainingCapacity()
	 */
	@Override
	public int remainingCapacity() {
		return Integer.MAX_VALUE;
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.concurrent.BlockingDeque#take()
	 */
	@Override
	@Nullable
	public E take() throws InterruptedException {
		return poll(0, TimeUnit.SECONDS);
	}

	//
	// BlockingDeque
	//

	/*
	 * (non-Javadoc)
	 * @see java.util.concurrent.BlockingDeque#offerFirst(java.lang.Object, long, java.util.concurrent.TimeUnit)
	 */
	@Override
	public boolean offerFirst(E e, long timeout, TimeUnit unit) throws InterruptedException {
		return offerFirst(e);
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.concurrent.BlockingDeque#offerLast(java.lang.Object, long, java.util.concurrent.TimeUnit)
	 */
	@Override
	public boolean offerLast(E e, long timeout, TimeUnit unit) throws InterruptedException {
		return offerLast(e);
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.concurrent.BlockingDeque#pollFirst(long, java.util.concurrent.TimeUnit)
	 */
	@Override
	@Nullable
	public E pollFirst(long timeout, TimeUnit unit) throws InterruptedException {
		return poll(timeout, unit);
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.concurrent.BlockingDeque#pollLast(long, java.util.concurrent.TimeUnit)
	 */
	@Override
	@Nullable
	public E pollLast(long timeout, TimeUnit unit) throws InterruptedException {
		return listOps.rightPop(timeout, unit);
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.concurrent.BlockingDeque#putFirst(java.lang.Object)
	 */
	@Override
	public void putFirst(E e) throws InterruptedException {
		add(e);
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.concurrent.BlockingDeque#putLast(java.lang.Object)
	 */
	@Override
	public void putLast(E e) throws InterruptedException {
		put(e);
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.concurrent.BlockingDeque#takeFirst()
	 */
	@Override
	@Nullable
	public E takeFirst() throws InterruptedException {
		return take();
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.concurrent.BlockingDeque#takeLast()
	 */
	@Override
	@Nullable
	public E takeLast() throws InterruptedException {
		return pollLast(0, TimeUnit.SECONDS);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundKeyOperations#getType()
	 */
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
}
