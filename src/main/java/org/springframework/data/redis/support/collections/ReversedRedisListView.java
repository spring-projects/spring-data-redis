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
 * @param <E>
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
		return base.getKey();
	}

	@Nullable
	@Override
	public DataType getType() {
		return base.getType();
	}

	@Nullable
	@Override
	public Long getExpire() {
		return base.getExpire();
	}

	@Nullable
	@Override
	public Boolean expire(long timeout, TimeUnit unit) {
		return base.expire(timeout, unit);
	}

	@Nullable
	@Override
	public Boolean expireAt(Date date) {
		return base.expireAt(date);
	}

	@Nullable
	@Override
	public Boolean persist() {
		return base.persist();
	}

	@Override
	public void rename(String newKey) {
		base.remove(newKey);
	}

	@Override
	public RedisOperations<String, ?> getOperations() {
		return base.getOperations();
	}

	@Nullable
	@Override
	public E moveFirstTo(RedisList<E> destination, Direction destinationPosition) {
		return base.moveLastTo(destination, destinationPosition);
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
		return base.range(end, start);
	}

	@Override
	public RedisList<E> trim(int start, int end) {
		base.trim(end, start);
		return this;
	}

	@Override
	public RedisList<E> trim(long start, long end) {
		base.trim(end, start);
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
	public boolean add(E e) {

		base.add(0, e);
		return true;
	}

	@Override
	public boolean addAll(Collection<? extends E> c) {

		@SuppressWarnings("unchecked")
		E[] adds = (E[]) c.toArray();
		if (adds.length == 0) {
			return false;
		} else {
			base.addAll(0, Arrays.asList(reverse(adds)));
			return true;
		}
	}

	@Override
	public void clear() {
		base.clear();
	}

	@Override
	public boolean contains(Object o) {
		return base.contains(o);
	}

	@Override
	public boolean containsAll(Collection<?> c) {
		return base.containsAll(c);
	}

	// copied from AbstractList
	public boolean equals(Object o) {
		if (o == this)
			return true;
		if (!(o instanceof List))
			return false;

		ListIterator<E> e1 = listIterator();
		ListIterator<?> e2 = ((List<?>) o).listIterator();
		while (e1.hasNext() && e2.hasNext()) {
			E o1 = e1.next();
			Object o2 = e2.next();
			if (!(o1 == null ? o2 == null : o1.equals(o2)))
				return false;
		}
		return !(e1.hasNext() || e2.hasNext());
	}

	// copied from AbstractList
	public int hashCode() {
		int hashCode = 1;
		for (E e : this)
			hashCode = 31 * hashCode + (e == null ? 0 : e.hashCode());
		return hashCode;
	}

	@Override
	public boolean isEmpty() {
		return base.isEmpty();
	}

	@Override
	public Stream<E> parallelStream() {
		return StreamSupport.stream(spliterator(), true);
	}

	// copied from AbstractCollection
	@Override
	public boolean remove(Object o) {

		Iterator<E> it = iterator();
		if (o == null) {
			while (it.hasNext()) {
				if (it.next() == null) {
					it.remove();
					return true;
				}
			}
		} else {
			while (it.hasNext()) {
				if (o.equals(it.next())) {
					it.remove();
					return true;
				}
			}
		}
		return false;
	}

	// copied from AbstractCollection
	@Override
	public boolean removeAll(Collection<?> c) {

		Objects.requireNonNull(c);
		boolean modified = false;
		Iterator<?> it = iterator();
		while (it.hasNext()) {
			if (c.contains(it.next())) {
				it.remove();
				modified = true;
			}
		}
		return modified;
	}

	// copied from AbstractCollection
	@Override
	public boolean retainAll(Collection<?> c) {

		Objects.requireNonNull(c);
		boolean modified = false;
		Iterator<E> it = iterator();
		while (it.hasNext()) {
			if (!c.contains(it.next())) {
				it.remove();
				modified = true;
			}
		}
		return modified;
	}

	@Override
	public int size() {
		return base.size();
	}

	@Override
	public Stream<E> stream() {
		return StreamSupport.stream(spliterator(), false);
	}

	@Override
	public Object[] toArray() {
		return reverse(base.toArray());
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> T[] toArray(T[] a) {
		return toArrayReversed(base, a);
	}

	@Override
	public <T> T[] toArray(IntFunction<T[]> generator) {
		return reverse(base.toArray(generator));
	}

	// copied from AbstractCollection
	public String toString() {
		Iterator<E> it = iterator();
		if (!it.hasNext())
			return "[]";

		StringBuilder sb = new StringBuilder();
		sb.append('[');
		for (;;) {
			E e = it.next();
			sb.append(e == this ? "(this Collection)" : e);
			if (!it.hasNext())
				return sb.append(']').toString();
			sb.append(',').append(' ');
		}
	}

	// ========== List ==========

	@Override
	public void add(int index, E element) {

		int size = base.size();
		checkClosedRange(index, size);
		base.add(size - index, element);
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
	public E get(int i) {
		int size = base.size();
		Objects.checkIndex(i, size);
		return base.get(size - i - 1);
	}

	@Override
	public int indexOf(Object o) {
		int i = base.lastIndexOf(o);
		return i == -1 ? -1 : base.size() - i - 1;
	}

	@Override
	public int lastIndexOf(Object o) {
		int i = base.indexOf(o);
		return i == -1 ? -1 : base.size() - i - 1;
	}

	@Override
	public ListIterator<E> listIterator() {
		return new DescendingListIterator(base.size(), 0);
	}

	@Override
	public ListIterator<E> listIterator(int index) {
		int size = base.size();
		checkClosedRange(index, size);
		return new DescendingListIterator(size, index);
	}

	@Override
	public E remove(int index) {
		int size = base.size();
		Objects.checkIndex(index, size);
		return base.remove(size - index - 1);
	}

	@Override
	public boolean removeIf(Predicate<? super E> filter) {
		return base.removeIf(filter);
	}

	@Override
	public void replaceAll(UnaryOperator<E> operator) {
		base.replaceAll(operator);
	}

	@Override
	public void sort(Comparator<? super E> c) {
		base.sort(Collections.reverseOrder(c));
	}

	@Override
	public E set(int index, E element) {

		int size = base.size();
		Objects.checkIndex(index, size);
		return base.set(size - index - 1, element);
	}

	@Override
	public List<E> subList(int fromIndex, int toIndex) {
		throw new UnsupportedOperationException();
	}

	// ========== BlockingDeque ==========

	@Override
	public boolean offerFirst(E e) {
		return base.offerLast(e);
	}

	@Override
	public boolean offerLast(E e) {
		return base.offerFirst(e);
	}

	@Override
	public void putFirst(E e) throws InterruptedException {
		base.putLast(e);
	}

	@Override
	public void putLast(E e) throws InterruptedException {
		base.putFirst(e);
	}

	@Override
	public boolean offerFirst(E e, long timeout, TimeUnit unit) throws InterruptedException {
		return base.offerLast(e, timeout, unit);
	}

	@Override
	public boolean offerLast(E e, long timeout, TimeUnit unit) throws InterruptedException {
		return base.offerFirst(e, timeout, unit);
	}

	@Override
	public E takeFirst() throws InterruptedException {
		return base.takeLast();
	}

	@Override
	public E takeLast() throws InterruptedException {
		return base.takeFirst();
	}

	@Nullable
	@Override
	public E pollFirst(long timeout, TimeUnit unit) throws InterruptedException {
		return base.pollLast(timeout, unit);
	}

	@Nullable
	@Override
	public E pollLast(long timeout, TimeUnit unit) throws InterruptedException {
		return base.pollFirst(timeout, unit);
	}

	@Override
	public boolean removeFirstOccurrence(Object o) {
		return base.removeLastOccurrence(o);
	}

	@Override
	public boolean removeLastOccurrence(Object o) {
		return base.removeFirstOccurrence(o);
	}

	@Override
	public boolean offer(E e) {
		return base.offerFirst(e);
	}

	@Override
	public void put(E e) throws InterruptedException {
		base.offerFirst(e);
	}

	@Override
	public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {
		return base.offerFirst(e, timeout, unit);
	}

	@Override
	public E remove() {
		return pollLast();
	}

	@Override
	public E poll() {
		return pollLast();
	}

	@Override
	public E take() throws InterruptedException {
		return poll(0, TimeUnit.SECONDS);
	}

	@Nullable
	@Override
	public E poll(long timeout, TimeUnit unit) throws InterruptedException {
		return pollLast(0, TimeUnit.SECONDS);
	}

	@Override
	public E element() {
		return peekLast();
	}

	@Override
	public E peek() {
		return peekLast();
	}

	@Override
	public void push(E e) {
		base.addLast(e);
	}

	@Override
	public E pollFirst() {
		return base.pollLast();
	}

	@Override
	public E pollLast() {
		return base.pollLast();
	}

	@Override
	public E peekFirst() {
		return base.peekLast();
	}

	@Override
	public E peekLast() {
		return base.peekFirst();
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
	public Iterator<E> descendingIterator() {
		return base.iterator();
	}

	@Override
	public int remainingCapacity() {
		return base.remainingCapacity();
	}

	@Override
	public int drainTo(Collection<? super E> c) {
		return drainTo(c, size());
	}

	@Override
	public int drainTo(Collection<? super E> c, int maxElements) {

		if (this.equals(c)) {
			throw new IllegalArgumentException("Cannot drain a queue to itself");
		}

		int size = size();
		int loop = Math.min(size, maxElements);

		for (int index = 0; index < loop; index++) {
			c.add(poll());
		}

		return loop;
	}

	@Override
	public RedisList<E> reversed() {
		return base;
	}

	class DescendingIterator implements Iterator<E> {
		final ListIterator<E> it = base.listIterator(base.size());

		@Override
		public boolean hasNext() {
			return it.hasPrevious();
		}

		@Override
		public E next() {
			return it.previous();
		}

		@Override
		public void remove() {
			it.remove();
		}
	}

	class DescendingListIterator implements ListIterator<E> {
		final ListIterator<E> it;

		DescendingListIterator(int size, int pos) {
			if (pos < 0 || pos > size)
				throw new IndexOutOfBoundsException();
			it = base.listIterator(size - pos);
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
	 * @param a the array to be reversed
	 * @return the reversed array, always the same array as the argument
	 */
	static <T> T[] reverse(T[] a) {
		int limit = a.length / 2;
		for (int i = 0, j = a.length - 1; i < limit; i++, j--) {
			T t = a[i];
			a[i] = a[j];
			a[j] = t;
		}
		return a;
	}

	static <T> T[] toArrayReversed(Collection<?> coll, T[] array) {
		T[] newArray = reverse(coll.toArray(Arrays.copyOfRange(array, 0, 0)));
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
