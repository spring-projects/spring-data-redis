/*
 * Copyright 2017-2023 the original author or authors.
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
 *  limitations under the License.
 */
package org.springframework.data.redis.support.collections;

import java.util.Collection;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.TimeUnit;

import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.connection.RedisListCommands;
import org.springframework.data.redis.core.BoundListOperations;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.util.Assert;

/**
 * Implementation and view of an existing {@link RedisList} where the elements in the list (deque)
 * are returned in reverse order.
 *
 * @author John Blum
 * @see org.springframework.data.redis.support.collections.AbstractRedisCollection
 * @see org.springframework.data.redis.support.collections.RedisList
 * @since 3.2.0
 */
class ReversedRedisList<E> extends AbstractRedisCollection<E> implements RedisList<E> {

	private final RedisList<E> redisList;
	private final Deque<E> reversedDeque;
	private final List<E> reversedList;

	/**
	 * Constructs a new {@link ReversedRedisList} initialized with a view of the given {@link RedisList}
	 * from which this list is derived along with a {@link Deque#reversed() reversed Deque view}
	 * and {@link List#reversed() reversed List view}.
	 *
	 * @param redisList original {@link RedisList}.
	 * @param reversedList {@link List#reversed() List view} in reverse order; must not be {@literal null}.
	 * @param reversedDeque {@link Deque#reversed() Deque view} in reverse order; must not be {@literal null}.
	 */
	@SuppressWarnings("unchecked")
	ReversedRedisList(RedisList<E> redisList, List<E> reversedList, Deque<E> reversedDeque) {

		super(redisList.getKey(), (RedisOperations<String, E>) redisList.getOperations());

		this.redisList = redisList;
		this.reversedList = reversedList;
		this.reversedDeque = reversedDeque;
	}

	/**
	 * Gets the {@link #getReversedDeque() Deque} as a {@link BlockingDeque}.
	 *
	 * @return the {@link #getReversedDeque() Deque} as a {@link BlockingDeque}.
	 * @see #getReversedDeque()
	 */
	BlockingDeque<E> getReversedBlockingDeque() {
		return (BlockingDeque<E>) getReversedDeque();
	}

	/**
	 * Get the configured reference to the underlying reversed {@link Deque}.
	 *
	 * @return the configured reference to the underlying reversed {@link Deque}.
	 */
	Deque<E> getReversedDeque() {
		return this.reversedDeque;
	}

	/**
	 * Get the configured reference to the underlying reversed {@link List}.
	 *
	 * @return the configured reference to the underlying reversed {@link List}.
	 */
	List<E> getReversedList() {
		return this.reversedList;
	}

	/**
	 * Gets the configured reference to the original, wrapped {@link RedisList}.
	 *
	 * @return the configured reference to the original, wrapped {@link RedisList}.
	 */
	RedisList<E> getRedisList() {
		return this.redisList;
	}

	BoundListOperations<String, E> getBoundListOperations() {
		return getOperations().boundListOps(getKey());
	}

	// Methods from AbstractCollection

	// @see https://groups.google.com/g/redis-db/c/Co_L08-V360/m/_TkD-E_mm6oJ
	@Override
	public Iterator<E> iterator() {
		return new ReversedRedisListIterator();
	}

	@Override
	public int size() {
		return getRedisList().size();
	}

	// Methods from BaseRedisList (BoundKeyOperations)

	@Override
	public RedisList<E> reversed() {
		return getRedisList();
	}

	@Override
	public DataType getType() {
		return getRedisList().getType();
	}

	// Methods from BlockingDeque (BlockingQueue)

	@Override
	public int drainTo(Collection<? super E> collection) {
		return getReversedBlockingDeque().drainTo(collection);
	}

	@Override
	public int drainTo(Collection<? super E> collection, int maxElements) {
		return getReversedBlockingDeque().drainTo(collection, maxElements);
	}

	@Override
	public E element() {
		return getReversedBlockingDeque().element();
	}

	@Override
	public boolean offer(E element) {
		return getReversedBlockingDeque().offer(element);
	}

	@Override
	public boolean offer(E element, long timeout, TimeUnit unit) throws InterruptedException {
		return getReversedBlockingDeque().offer(element, timeout, unit);
	}

	@Override
	public boolean offerFirst(E element) {
		return getReversedBlockingDeque().offerFirst(element);
	}

	@Override
	public boolean offerFirst(E element, long timeout, TimeUnit unit) throws InterruptedException {
		return getReversedBlockingDeque().offerFirst(element, timeout, unit);
	}

	@Override
	public boolean offerLast(E element) {
		return getReversedBlockingDeque().offerLast(element);
	}

	@Override
	public boolean offerLast(E element, long timeout, TimeUnit unit) throws InterruptedException {
		return getReversedBlockingDeque().offerLast(element, timeout, unit);
	}

	@Override
	public E peek() {
		return getReversedBlockingDeque().peek();
	}

	@Override
	public E poll() {
		return getReversedBlockingDeque().poll();
	}

	@Override
	public E poll(long timeout, TimeUnit unit) throws InterruptedException {
		return getReversedBlockingDeque().poll(timeout, unit);
	}

	@Override
	public E pollFirst(long timeout, TimeUnit unit) throws InterruptedException {
		return getReversedBlockingDeque().pollFirst(timeout, unit);
	}

	@Override
	public E pollLast(long timeout, TimeUnit unit) throws InterruptedException {
		return getReversedBlockingDeque().pollLast(timeout, unit);
	}

	@Override
	public void push(E element) {
		getReversedBlockingDeque().push(element);
	}

	@Override
	public void put(E element) throws InterruptedException {
		getReversedBlockingDeque().put(element);
	}

	@Override
	public void putFirst(E element) throws InterruptedException {
		getReversedBlockingDeque().putFirst(element);
	}

	@Override
	public void putLast(E element) throws InterruptedException {
		getReversedBlockingDeque().putLast(element);
	}

	@Override
	public E remove() {
		return getReversedBlockingDeque().remove();
	}

	@Override
	public int remainingCapacity() {
		return getReversedBlockingDeque().remainingCapacity();
	}

	@Override
	public boolean removeFirstOccurrence(Object target) {
		return getReversedBlockingDeque().removeFirstOccurrence(target);
	}

	@Override
	public boolean removeLastOccurrence(Object target) {
		return getReversedBlockingDeque().removeLastOccurrence(target);
	}

	@Override
	public E take() throws InterruptedException {
		return getReversedBlockingDeque().take();
	}

	@Override
	public E takeFirst() throws InterruptedException {
		return getReversedBlockingDeque().takeFirst();
	}

	@Override
	public E takeLast() throws InterruptedException {
		return getReversedBlockingDeque().takeLast();
	}

	// Methods from Deque

	@Override
	public E pollFirst() {
		return getReversedDeque().pollFirst();
	}

	@Override
	public E pollLast() {
		return getReversedDeque().pollLast();
	}

	@Override
	public E peekFirst() {
		return getReversedDeque().peekFirst();
	}

	@Override
	public E peekLast() {
		return getReversedDeque().peekLast();
	}

	@Override
	public E pop() {
		return getReversedDeque().pop();
	}

	@Override
	public Iterator<E> descendingIterator() {
		return getReversedDeque().descendingIterator();
	}

	// Methods from List

	@Override
	public void add(int index, E element) {
		getReversedList().add(index, element);
	}

	@Override
	public boolean addAll(int index, Collection<? extends E> collection) {
		return getReversedList().addAll(index, collection);
	}

	@Override
	public E get(int index) {
		return getReversedList().get(index);
	}

	@Override
	public int indexOf(Object target) {
		return getReversedList().indexOf(target);
	}

	@Override
	public int lastIndexOf(Object target) {
		return getReversedList().lastIndexOf(target);
	}

	@Override
	public ListIterator<E> listIterator() {
		return getReversedList().listIterator();
	}

	@Override
	public ListIterator<E> listIterator(int index) {
		return getReversedList().listIterator(index);
	}

	@Override
	public E remove(int index) {
		return getReversedList().remove(index);
	}

	@Override
	public E set(int index, E element) {
		return getReversedList().set(index, element);
	}

	@Override
	public List<E> subList(int fromIndex, int toIndex) {
		return getReversedList().subList(fromIndex, toIndex);
	}

	// Methods from RedisList

	@Override
	public void addFirst(E element) {
		getRedisList().addLast(element);
	}

	@Override
	public void addLast(E element) {
		getRedisList().addFirst(element);
	}

	@Override
	public E getFirst() {
		return getRedisList().peekLast();
	}

	@Override
	public E getLast() {
		return getRedisList().peekFirst();
	}

	@Override
	public E removeFirst() {
		return getRedisList().removeLast();
	}

	@Override
	public E removeLast() {
		return getRedisList().removeFirst();
	}

	@Override
	public E moveFirstTo(RedisList<E> destination, RedisListCommands.Direction destinationPosition) {
		return getRedisList().moveFirstTo(destination, destinationPosition);
	}

	@Override
	public E moveFirstTo(RedisList<E> destination, RedisListCommands.Direction destinationPosition, long timeout,
			TimeUnit unit) {

		return getRedisList().moveFirstTo(destination, destinationPosition, timeout, unit);
	}

	@Override
	public E moveLastTo(RedisList<E> destination, RedisListCommands.Direction destinationPosition) {
		return getRedisList().moveLastTo(destination, destinationPosition);
	}

	@Override
	public E moveLastTo(RedisList<E> destination, RedisListCommands.Direction destinationPosition, long timeout,
			TimeUnit unit) {

		return getRedisList().moveLastTo(destination, destinationPosition, timeout, unit);
	}

	@Override
	public List<E> range(long start, long end) {
		return getRedisList().range(start, end);
	}

	@Override
	public RedisList<E> trim(int start, int end) {
		return getRedisList().trim(start, end);
	}

	@Override
	public RedisList<E> trim(long start, long end) {
		return getRedisList().trim(start, end);
	}

	private class ReversedRedisListIterator implements Iterator<E> {

		private int index = 0;
		private E element;

		@Override
		public boolean hasNext() {
			return this.index < getReversedList().size();
		}

		@Override
		public E next() {
			this.element = getReversedList().get(this.index++);
			return this.element;
		}

		@Override
		public void remove() {
			Assert.state(this.element != null, "Must first call next()");
			getRedisList().remove(this.element);
			this.index -= 1;
			this.element = null;
		}
	}
}
