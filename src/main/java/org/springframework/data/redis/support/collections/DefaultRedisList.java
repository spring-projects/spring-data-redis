/*
 * Copyright 2011-2019 the original author or authors.
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

import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.core.BoundListOperations;
import org.springframework.data.redis.core.RedisOperations;

/**
 * Default implementation for {@link RedisList}. Suitable for not just lists, but also queues (FIFO ordering) or stacks
 * (LIFO ordering) and deques (or double ended queues). Allows the maximum size (or the cap) to be specified to prevent
 * the list from over growing. Note that all write operations will execute immediately, whether a cap is specified or
 * not - the list will always accept new items (trimming the tail after each insert in case of capped collections).
 * 
 * @author Costin Leau
 */
public class DefaultRedisList<E> extends AbstractRedisCollection<E> implements RedisList<E> {

	private final BoundListOperations<String, E> listOps;

	private volatile int maxSize = 0;

	private volatile boolean capped = false;

	private class DefaultRedisListIterator extends RedisIterator<E> {

		public DefaultRedisListIterator(Iterator<E> delegate) {
			super(delegate);
		}

		protected void removeFromRedisStorage(E item) {
			DefaultRedisList.this.remove(item);
		}
	}

	/**
	 * Constructs a new, uncapped <code>DefaultRedisList</code> instance.
	 * 
	 * @param key
	 * @param operations
	 */
	public DefaultRedisList(String key, RedisOperations<String, E> operations) {
		this(operations.boundListOps(key));
	}

	/**
	 * Constructs a new, uncapped <code>DefaultRedisList</code> instance.
	 * 
	 * @param boundOps
	 */
	public DefaultRedisList(BoundListOperations<String, E> boundOps) {
		this(boundOps, 0);
	}

	/**
	 * Constructs a new <code>DefaultRedisList</code> instance.
	 * 
	 * @param boundOps
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

	public List<E> range(long start, long end) {
		return listOps.range(start, end);
	}

	public RedisList<E> trim(int start, int end) {
		listOps.trim(start, end);
		return this;
	}

	private List<E> content() {
		return listOps.range(0, -1);
	}

	private void cap() {
		if (capped) {
			listOps.trim(0, maxSize - 1);
		}
	}

	public Iterator<E> iterator() {
		List<E> list = content();
		checkResult(list);
		return new DefaultRedisListIterator(list.iterator());
	}

	public int size() {
		Long size = listOps.size();
		checkResult(size);
		return size.intValue();
	}

	public boolean add(E value) {
		listOps.rightPush(value);
		cap();
		return true;
	}

	public void clear() {
		listOps.trim(size() + 1, 0);
	}

	public boolean remove(Object o) {
		Long result = listOps.remove(1, o);
		return (result != null && result.longValue() > 0);
	}

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

	public E get(int index) {
		if (index < 0 || index > size()) {
			throw new IndexOutOfBoundsException();
		}
		return listOps.index(index);
	}

	public int indexOf(Object o) {
		throw new UnsupportedOperationException();
	}

	public int lastIndexOf(Object o) {
		throw new UnsupportedOperationException();
	}

	public ListIterator<E> listIterator() {
		throw new UnsupportedOperationException();
	}

	public ListIterator<E> listIterator(int index) {
		throw new UnsupportedOperationException();
	}

	public E remove(int index) {
		throw new UnsupportedOperationException();
	}

	public E set(int index, E e) {
		E object = get(index);
		listOps.set(index, e);
		return object;
	}

	public List<E> subList(int fromIndex, int toIndex) {
		throw new UnsupportedOperationException();
	}

	//
	// Queue methods
	//

	public E element() {
		E value = peek();
		if (value == null)
			throw new NoSuchElementException();

		return value;
	}

	public boolean offer(E e) {
		listOps.rightPush(e);
		cap();
		return true;
	}

	public E peek() {
		return listOps.index(0);
	}

	public E poll() {
		return listOps.leftPop();
	}

	public E remove() {
		E value = poll();
		if (value == null)
			throw new NoSuchElementException();

		return value;
	}

	//
	// Dequeue
	//

	public void addFirst(E e) {
		listOps.leftPush(e);
		cap();
	}

	public void addLast(E e) {
		add(e);
	}

	public Iterator<E> descendingIterator() {
		List<E> content = content();
		Collections.reverse(content);
		return new DefaultRedisListIterator(content.iterator());
	}

	public E getFirst() {
		return element();
	}

	public E getLast() {
		E e = peekLast();
		if (e == null) {
			throw new NoSuchElementException();
		}
		return e;
	}

	public boolean offerFirst(E e) {
		addFirst(e);
		return true;
	}

	public boolean offerLast(E e) {
		addLast(e);
		return true;
	}

	public E peekFirst() {
		return peek();
	}

	public E peekLast() {
		return listOps.index(-1);
	}

	public E pollFirst() {
		return poll();
	}

	public E pollLast() {
		return listOps.rightPop();
	}

	public E pop() {
		E e = poll();
		if (e == null) {
			throw new NoSuchElementException();
		}
		return e;
	}

	public void push(E e) {
		addFirst(e);
	}

	public E removeFirst() {
		return pop();
	}

	public boolean removeFirstOccurrence(Object o) {
		return remove(o);
	}

	public E removeLast() {
		E e = pollLast();
		if (e == null) {
			throw new NoSuchElementException();
		}
		return e;
	}

	public boolean removeLastOccurrence(Object o) {
		Long result = listOps.remove(-1, o);
		return (result != null && result.longValue() > 0);
	}

	//
	// BlockingQueue
	//

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

	public int drainTo(Collection<? super E> c) {
		return drainTo(c, size());
	}

	public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {
		return offer(e);
	}

	public E poll(long timeout, TimeUnit unit) throws InterruptedException {
		E element = listOps.leftPop(timeout, unit);
		return (element == null ? null : element);
	}

	public void put(E e) throws InterruptedException {
		offer(e);
	}

	public int remainingCapacity() {
		return Integer.MAX_VALUE;
	}

	public E take() throws InterruptedException {
		return poll(0, TimeUnit.SECONDS);
	}

	//
	// BlockingDeque
	//

	public boolean offerFirst(E e, long timeout, TimeUnit unit) throws InterruptedException {
		return offerFirst(e);
	}

	public boolean offerLast(E e, long timeout, TimeUnit unit) throws InterruptedException {
		return offerLast(e);
	}

	public E pollFirst(long timeout, TimeUnit unit) throws InterruptedException {
		return poll(timeout, unit);
	}

	public E pollLast(long timeout, TimeUnit unit) throws InterruptedException {
		E element = listOps.rightPop(timeout, unit);
		return (element == null ? null : element);
	}

	public void putFirst(E e) throws InterruptedException {
		add(e);
	}

	public void putLast(E e) throws InterruptedException {
		put(e);
	}

	public E takeFirst() throws InterruptedException {
		return take();
	}

	public E takeLast() throws InterruptedException {
		return pollLast(0, TimeUnit.SECONDS);
	}

	public DataType getType() {
		return DataType.LIST;
	}
}
