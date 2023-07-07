/*
 * Copyright 2023 the original author or authors.
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

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.connection.RedisListCommands.Direction;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.lang.Nullable;

/**
 * Implementation and view of an existing {@link RedisList} where the elements in the list (deque) are returned in
 * reverse order.
 *
 * @param <E> the type of elements in this collection.
 * @author John Blum
 * @author Mark Paluch
 * @since 3.2
 */
class ReversedRedisListView<E> implements RedisList<E> {

	private final RedisList<E> base;

	ReversedRedisListView(RedisList<E> list) {
		this.base = list;
	}

	// ========== RedisCollection ==========

	@Override
	public String getKey() {
		return this.base.getKey();
	}

	@Nullable
	@Override
	public Long getExpire() {
		return this.base.getExpire();
	}

	@Override
	public RedisOperations<String, ?> getOperations() {
		return this.base.getOperations();
	}

	@Nullable
	@Override
	public DataType getType() {
		return this.base.getType();
	}

	@Nullable
	@Override
	public Boolean expire(long timeout, TimeUnit unit) {
		return this.base.expire(timeout, unit);
	}

	@Nullable
	@Override
	public Boolean expireAt(Date date) {
		return this.base.expireAt(date);
	}

	@Nullable
	@Override
	public Boolean persist() {
		return this.base.persist();
	}

	@Nullable
	@Override
	public E moveFirstTo(RedisList<E> destination, Direction destinationPosition) {
		return this.base.moveLastTo(destination, destinationPosition);
	}

	@Nullable
	@Override
	public E moveFirstTo(RedisList<E> destination, Direction destinationPosition, long timeout, TimeUnit unit) {
		return base.moveLastTo(destination, destinationPosition, timeout, unit);
	}

	@Nullable
	@Override
	public E moveLastTo(RedisList<E> destination, Direction destinationPosition) {
		return base.moveFirstTo(destination, destinationPosition);
	}

	@Nullable
	@Override
	public E moveLastTo(RedisList<E> destination, Direction destinationPosition, long timeout, TimeUnit unit) {
		return base.moveFirstTo(destination, destinationPosition, timeout, unit);
	}

	@Override
	public List<E> range(long start, long end) {
		return this.base.range(end, start);
	}

	@Override
	public void rename(String newKey) {
		this.base.rename(newKey);
	}

	@Override
	public RedisList<E> trim(int start, int end) {
		this.base.trim(end, start);
		return this;
	}

	@Override
	public RedisList<E> trim(long start, long end) {
		this.base.trim(end, start);
		return this;
	}

	// ========== Iterable ==========

	@Override
	public void forEach(Consumer<? super E> action) {
		for (E e : this)
			action.accept(e);
	}

	@Override
	public Iterator<E> iterator() {
		return new DescendingIterator();
	}

	@Override
	public Spliterator<E> spliterator() {
		return Spliterators.spliteratorUnknownSize(new DescendingIterator(), 0);
	}

	// ========== Collection ==========

	@Override
	public boolean add(E element) {
		this.base.add(0, element);
		return true;
	}

	@Override
	@SuppressWarnings("unchecked")
	public boolean addAll(Collection<? extends E> collection) {

		return !org.springframework.util.CollectionUtils.isEmpty(collection)
			&& this.base.addAll(0, Arrays.asList(reverse((E[]) collection.toArray())));
	}

	@Override
	public void clear() {
		this.base.clear();
	}

	@Override
	public boolean contains(Object element) {
		return this.base.contains(element);
	}

	@Override
	public boolean containsAll(Collection<?> collection) {
		return this.base.containsAll(collection);
	}

	public boolean equals(Object obj) {

		if (this == obj) {
			return true;
		}

		if (!(obj instanceof List<?> that)) {
			return false;
		}

		ListIterator<E> thisListIterator = this.listIterator();
		ListIterator<?> thatListIterator = that.listIterator();

		while (thisListIterator.hasNext() && thatListIterator.hasNext()) {
			if (!(Objects.equals(thisListIterator.next(), thatListIterator.next()))) {
				return false;
			}
		}

		return !(thisListIterator.hasNext() || thatListIterator.hasNext());
	}

	public int hashCode() {
		int hashCode = 1;
		for (E element : this) {
			hashCode = 31 * hashCode + Objects.hashCode(element);
		}
		return hashCode;
	}

	@Override
	public boolean isEmpty() {
		return this.base.isEmpty();
	}

	@Override
	public Stream<E> parallelStream() {
		return StreamSupport.stream(spliterator(), true);
	}

	@Override
	public boolean remove(Object element) {

		Iterator<E> it = iterator();

		while (it.hasNext()) {
			if (Objects.equals(element, it.next())) {
				it.remove();
				return true;
			}
		}

		return false;
	}

	@Override
	public boolean removeAll(Collection<?> collection) {

		Objects.requireNonNull(collection);

		Iterator<?> it = iterator();
		boolean modified = false;

		while (it.hasNext()) {
			if (collection.contains(it.next())) {
				it.remove();
				modified = true;
			}
		}

		return modified;
	}

	@Override
	public boolean retainAll(Collection<?> collection) {

		Objects.requireNonNull(collection);

		Iterator<E> it = iterator();
		boolean modified = false;

		while (it.hasNext()) {
			if (!collection.contains(it.next())) {
				it.remove();
				modified = true;
			}
		}

		return modified;
	}

	@Override
	public int size() {
		return this.base.size();
	}

	@Override
	public Stream<E> stream() {
		return StreamSupport.stream(spliterator(), false);
	}

	@Override
	public Object[] toArray() {
		return reverse(this.base.toArray());
	}

	@Override
	public <T> T[] toArray(T[] array) {
		return toArrayReversed(this.base, array);
	}

	@Override
	public <T> T[] toArray(IntFunction<T[]> generator) {
		return reverse(this.base.toArray(generator));
	}

	public String toString() {

		Iterator<E> it = iterator();

		if (!it.hasNext()) {
			return "[]";
		}

		StringBuilder stringBuilder = new StringBuilder("[");

		for (;;) {
			E element = it.next();
			stringBuilder.append(element == this ? "(this Collection)" : element);
			if (!it.hasNext()) {
				return stringBuilder.append(']').toString();
			}
			stringBuilder.append(',').append(' ');
		}
	}

	// ========== List ==========

	@Override
	public void add(int index, E element) {

		int size = this.base.size();

		checkClosedRange(index, size);
		this.base.add(size - index, element);
	}

	@Override
	public boolean addAll(int index, Collection<? extends E> c) {

		int size = base.size();

		checkClosedRange(index, size);

		@SuppressWarnings("unchecked")
		E[] adds = (E[]) c.toArray();

		if (adds.length == 0) {
			return false;
		} else {
			base.addAll(size - index, Arrays.asList(reverse(adds)));
			return true;
		}
	}

	@Override
	public E get(int index) {
		int size = base.size();
		Objects.checkIndex(index, size);
		return this.base.get(size - index - 1);
	}

	@Override
	@SuppressWarnings("all")
	public int indexOf(Object element) {
		int lastIndex = this.base.lastIndexOf(element);
		return lastIndex == -1 ? -1 : this.base.size() - lastIndex - 1;
	}

	@Override
	@SuppressWarnings("all")
	public int lastIndexOf(Object element) {
		int index = this.base.indexOf(element);
		return index == -1 ? -1 : this.base.size() - index - 1;
	}

	@Override
	public ListIterator<E> listIterator() {
		return new DescendingListIterator(base.size(), 0);
	}

	@Override
	public ListIterator<E> listIterator(int index) {
		int size = this.base.size();
		checkClosedRange(index, size);
		return new DescendingListIterator(size, index);
	}

	@Override
	public E remove(int index) {
		int size = this.base.size();
		Objects.checkIndex(index, size);
		return this.base.remove(size - index - 1);
	}

	@Override
	public boolean removeIf(Predicate<? super E> filter) {
		return this.base.removeIf(filter);
	}

	@Override
	public void replaceAll(UnaryOperator<E> operator) {
		this.base.replaceAll(operator);
	}

	@Override
	public void sort(Comparator<? super E> comparator) {
		this.base.sort(Collections.reverseOrder(comparator));
	}

	@Override
	public E set(int index, E element) {
		int size = this.base.size();
		Objects.checkIndex(index, size);
		return this.base.set(size - index - 1, element);
	}

	@Override
	public List<E> subList(int fromIndex, int toIndex) {
		throw new UnsupportedOperationException();
	}

	// ========== BlockingDeque ==========

	@Override
	public E element() {

		E value = peek();
		if (value == null) {
			throw new NoSuchElementException();
		}

		return value;
	}

	@Override
	public boolean offer(E element) {
		return this.base.offerFirst(element);
	}

	@Override
	public boolean offer(E element, long timeout, TimeUnit unit) throws InterruptedException {
		return this.base.offerFirst(element, timeout, unit);
	}

	@Override
	public boolean offerFirst(E element) {
		return this.base.offerLast(element);
	}

	@Override
	public boolean offerFirst(E element, long timeout, TimeUnit unit) throws InterruptedException {
		return this.base.offerLast(element, timeout, unit);
	}

	@Override
	public boolean offerLast(E element) {
		return this.base.offerFirst(element);
	}

	@Override
	public boolean offerLast(E element, long timeout, TimeUnit unit) throws InterruptedException {
		return this.base.offerFirst(element, timeout, unit);
	}

	@Override
	public void putFirst(E element) throws InterruptedException {
		this.base.putLast(element);
	}

	@Override
	public void putLast(E element) throws InterruptedException {
		this.base.putFirst(element);
	}

	@Nullable
	@Override
	public E pollFirst(long timeout, TimeUnit unit) throws InterruptedException {
		return this.base.pollLast(timeout, unit);
	}

	@Nullable
	@Override
	public E pollLast(long timeout, TimeUnit unit) throws InterruptedException {
		return this.base.pollFirst(timeout, unit);
	}

	@Override
	public void put(E element) throws InterruptedException {
		this.base.offerFirst(element);
	}

	@Override
	public boolean removeFirstOccurrence(Object element) {
		return this.base.removeLastOccurrence(element);
	}

	@Override
	public boolean removeLastOccurrence(Object element) {
		return this.base.removeFirstOccurrence(element);
	}

	@Override
	public E take() throws InterruptedException {
		return takeFirst();
	}

	@Override
	public E takeFirst() throws InterruptedException {
		return this.base.takeLast();
	}

	@Override
	public E takeLast() throws InterruptedException {
		return this.base.takeFirst();
	}

	// ========== Deque ==========

	@Override
	public Iterator<E> descendingIterator() {
		return this.base.iterator();
	}

	@Override
	public int drainTo(Collection<? super E> collection) {
		return drainTo(collection, size());
	}

	@Override
	public int drainTo(Collection<? super E> collection, int maxElements) {

		if (this.equals(collection)) {
			throw new IllegalArgumentException("Cannot drain a queue to itself");
		}

		int loop = Math.min(size(), maxElements);

		for (int index = 0; index < loop; index++) {
			collection.add(poll());
		}

		return loop;
	}

	@Override
	public E peek() {
		return peekLast();
	}

	@Override
	public E peekFirst() {
		return this.base.peekLast();
	}

	@Override
	public E peekLast() {
		return this.base.peekFirst();
	}

	@Override
	public E poll() {
		return pollLast();
	}

	@Nullable
	@Override
	public E poll(long timeout, TimeUnit unit) throws InterruptedException {
		return pollLast(0, TimeUnit.SECONDS);
	}

	@Override
	public E pollFirst() {
		return this.base.pollLast();
	}

	@Override
	public E pollLast() {
		return this.base.pollFirst();
	}

	@Override
	public E pop() {

		E element = poll();

		if (element == null) {
			throw new NoSuchElementException();
		}

		return element;
	}

	@Override
	public void push(E element) {
		this.base.addLast(element);
	}

	@Override
	public int remainingCapacity() {
		return this.base.remainingCapacity();
	}

	@Override
	public E remove() {

		E value = poll();
		if (value == null) {
			throw new NoSuchElementException();
		}

		return value;
	}

	@Override
	public RedisList<E> reversed() {
		return this.base;
	}

	class DescendingIterator implements Iterator<E> {

		final ListIterator<E> it = base.listIterator(base.size());

		@Override
		public boolean hasNext() {
			return this.it.hasPrevious();
		}

		@Override
		public E next() {
			return this.it.previous();
		}

		@Override
		public void remove() {
			this.it.remove();
		}
	}

	class DescendingListIterator implements ListIterator<E> {

		final ListIterator<E> it;

		DescendingListIterator(int size, int position) {

			if (position < 0 || position > size) {
				String message = String.format("Position [%d] is out of bounds: [0, %d]", position, size);
				throw new IndexOutOfBoundsException(message);
			}

			this.it = base.listIterator(size - position);
		}

		@Override
		public boolean hasNext() {
			return it.hasPrevious();
		}

		@Override
		public E next() {
			return it.previous();
		}

		@Override
		public boolean hasPrevious() {
			return it.hasNext();
		}

		@Override
		public E previous() {
			return it.next();
		}

		@Override
		public int nextIndex() {
			return base.size() - it.nextIndex();
		}

		@Override
		public int previousIndex() {
			return nextIndex() - 1;
		}

		@Override
		public void remove() {
			it.remove();
		}

		@Override
		public void set(E e) {
			it.set(e);
		}

		@Override
		public void add(E e) {
			it.add(e);
			it.previous();
		}
	}

	/**
	 * Reverses the elements of an array in-place.
	 *
	 * @param <T> the array component type
	 * @param array the array to be reversed
	 * @return the reversed array, always the same array as the argument
	 */
	static <T> T[] reverse(T[] array) {

		int limit = array.length / 2;

		for (int i = 0, j = array.length - 1; i < limit; i++, j--) {
			swap(array, i, j);
		}

		return array;
	}

	private static <T> void swap(T[] array, int indexOne, int indexTwo) {
		T element = array[indexOne];
		array[indexOne] = array[indexTwo];
		array[indexTwo] = element;
	}

	static <T> T[] toArrayReversed(Collection<?> collection, T[] array) {

		T[] newArray = reverse(collection.toArray(Arrays.copyOfRange(array, 0, 0)));

		if (newArray.length > array.length) {
			return newArray;
		} else {
			System.arraycopy(newArray, 0, array, 0, newArray.length);
			if (array.length > newArray.length) {
				array[newArray.length] = null;
			}
			return array;
		}
	}

	static void checkClosedRange(int index, int size) {
		if (index < 0 || index > size) {
			throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + size);
		}
	}
}
