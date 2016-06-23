/*
 * Copyright 2011-2013 the original author or authors.
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

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import static org.springframework.data.redis.matcher.RedisTestMatchers.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.springframework.data.redis.ObjectFactory;
import org.springframework.data.redis.core.RedisTemplate;

/**
 * Integration test for RedisList
 * 
 * @author Costin Leau
 * @author Jennifer Hickey
 */
public abstract class AbstractRedisListTests<T> extends AbstractRedisCollectionTests<T> {

	protected RedisList<T> list;

	/**
	 * Constructs a new <code>AbstractRedisListTests</code> instance.
	 * 
	 * @param factory
	 * @param template
	 */
	@SuppressWarnings("rawtypes")
	public AbstractRedisListTests(ObjectFactory<T> factory, RedisTemplate template) {
		super(factory, template);
	}

	@SuppressWarnings("unchecked")
	@Before
	public void setUp() throws Exception {
		super.setUp();
		list = (RedisList<T>) collection;
	}

	@Test
	public void testAddIndexObjectHead() {
		T t1 = getT();
		T t2 = getT();
		T t3 = getT();

		list.add(t1);
		list.add(t2);

		assertThat(list.get(0), isEqual(t1));
		list.add(0, t3);
		assertThat(list.get(0), isEqual(t3));
	}

	@Test
	public void testAddIndexObjectTail() {
		T t1 = getT();
		T t2 = getT();
		T t3 = getT();

		list.add(t1);
		list.add(t2);

		assertThat(list.get(1), isEqual(t2));
		list.add(2, t3);
		assertThat(list.get(2), isEqual(t3));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testAddIndexObjectMiddle() {
		T t1 = getT();
		T t2 = getT();
		T t3 = getT();

		list.add(t1);
		list.add(t2);

		assertThat(list.get(0), isEqual(t1));
		list.add(1, t3);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void addAllIndexCollectionHead() {
		T t1 = getT();
		T t2 = getT();
		T t3 = getT();
		T t4 = getT();

		list.add(t1);
		list.add(t2);

		List<T> asList = Arrays.asList(t3, t4);

		assertThat(list.get(0), isEqual(t1));
		list.addAll(0, asList);
		// verify insertion order
		assertThat(list.get(0), isEqual(t3));
		assertThat(list.get(1), isEqual(t4));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void addAllIndexCollectionTail() {
		T t1 = getT();
		T t2 = getT();
		T t3 = getT();
		T t4 = getT();

		list.add(t1);
		list.add(t2);

		List<T> asList = Arrays.asList(t3, t4);

		assertThat(list.get(0), isEqual(t1));
		assertTrue(list.addAll(2, asList));

		// verify insertion order
		assertThat(list.get(2), isEqual(t3));
		assertThat(list.get(3), isEqual(t4));
	}

	@SuppressWarnings("unchecked")
	@Test(expected = IllegalArgumentException.class)
	public void addAllIndexCollectionMiddle() {
		T t1 = getT();
		T t2 = getT();
		T t3 = getT();
		T t4 = getT();

		list.add(t1);
		list.add(t2);

		List<T> asList = Arrays.asList(t3, t4);

		assertThat(list.get(0), isEqual(t1));
		assertTrue(list.addAll(1, asList));
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testIndexOfObject() {
		T t1 = getT();
		T t2 = getT();

		assertEquals(-1, list.indexOf(t1));
		list.add(t1);
		assertEquals(0, list.indexOf(t1));

		assertEquals(-1, list.indexOf(t2));
		list.add(t2);
		assertEquals(1, list.indexOf(t1));
	}

	@Test
	public void testOffer() {
		T t1 = getT();

		assertTrue(list.offer(t1));
		assertThat(list.get(0), isEqual(t1));
	}

	@Test
	public void testPeek() {
		assertNull(list.peek());
		T t1 = getT();
		list.add(t1);
		assertThat(list.peek(), isEqual(t1));
		list.clear();
		assertNull(list.peek());
	}

	@Test
	public void testElement() {
		try {
			list.element();
			fail();
		} catch (NoSuchElementException nse) {
			// expected
		}

		T t1 = getT();
		list.add(t1);
		assertThat(list.element(), isEqual(t1));
		list.clear();
		try {
			list.element();
			fail();
		} catch (NoSuchElementException nse) {
			// expected
		}
	}

	@Test
	public void testPop() {
		testPoll();
	}

	@Test
	public void testPoll() {
		assertNull(list.poll());
		T t1 = getT();
		list.add(t1);
		assertThat(list.poll(), isEqual(t1));
		assertNull(list.poll());
	}

	@Test
	public void testPollTimeout() throws InterruptedException {

		T t1 = getT();
		list.add(t1);
		assertThat(list.poll(1, TimeUnit.MILLISECONDS), isEqual(t1));
	}

	@Test
	public void testRemove() {
		try {
			list.remove();
			fail();
		} catch (NoSuchElementException nse) {
			// expected
		}

		T t1 = getT();
		list.add(t1);
		assertThat(list.remove(), isEqual(t1));
		try {
			list.remove();
			fail();
		} catch (NoSuchElementException nse) {
			// expected
		}
	}

	@Test
	public void testRange() {
		T t1 = getT();
		T t2 = getT();

		assertTrue(list.range(0, -1).isEmpty());
		list.add(t1);
		list.add(t2);
		assertEquals(2, list.range(0, -1).size());
		assertThat(list.range(0, 0).get(0), isEqual(t1));
		assertThat(list.range(1, 1).get(0), isEqual(t2));
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testRemoveIndex() {
		T t1 = getT();
		T t2 = getT();

		assertNull(list.remove(0));
		list.add(t1);
		list.add(t2);
		assertNull(list.remove(2));
		assertEquals(t2, list.remove(1));
		assertEquals(t1, list.remove(0));
	}

	@Test
	public void testSet() {
		T t1 = getT();
		T t2 = getT();
		list.add(t1);
		list.set(0, t1);
		assertThat(list.set(0, t2), isEqual(t1));
		assertThat(list.get(0), isEqual(t2));
	}

	@Test
	public void testTrim() {
		T t1 = getT();
		T t2 = getT();

		assertTrue(list.trim(0, 0).isEmpty());
		list.add(t1);
		list.add(t2);
		assertEquals(2, list.size());
		assertEquals(1, list.trim(0, 0).size());
		assertEquals(1, list.size());
		assertThat(list.get(0), isEqual(t1));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testCappedCollection() throws Exception {
		RedisList<T> cappedList = new DefaultRedisList<T>(template.boundListOps(collection.getKey() + ":capped"), 1);
		T first = getT();
		cappedList.offer(first);
		assertEquals(1, cappedList.size());
		cappedList.add(getT());
		assertEquals(1, cappedList.size());
		T last = getT();
		cappedList.add(last);
		assertEquals(1, cappedList.size());
		assertThat(cappedList.get(0), isEqual(first));
	}

	@Test
	public void testAddFirst() {
		T t1 = getT();
		T t2 = getT();
		T t3 = getT();

		list.addFirst(t1);
		list.addFirst(t2);
		list.addFirst(t3);

		Iterator<T> iterator = list.iterator();
		assertThat(iterator.next(), isEqual(t3));
		assertThat(iterator.next(), isEqual(t2));
		assertThat(iterator.next(), isEqual(t1));
	}

	@Test
	public void testAddLast() {
		testAdd();
	}

	@Test
	public void testDescendingIterator() {
		T t1 = getT();
		T t2 = getT();
		T t3 = getT();

		list.add(t1);
		list.add(t2);
		list.add(t3);

		Iterator<T> iterator = list.descendingIterator();
		assertThat(iterator.next(), isEqual(t3));
		assertThat(iterator.next(), isEqual(t2));
		assertThat(iterator.next(), isEqual(t1));

	}

	@SuppressWarnings("unchecked")
	@Test
	public void testDrainToCollectionWithMaxElements() {
		T t1 = getT();
		T t2 = getT();
		T t3 = getT();

		list.add(t1);
		list.add(t2);
		list.add(t3);

		List<T> c = new ArrayList<T>();

		list.drainTo(c, 2);
		assertEquals(1, list.size());
		assertThat(list, hasItem(t3));
		assertEquals(2, c.size());
		assertThat(c, hasItems(t1, t2));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testDrainToCollection() {
		T t1 = getT();
		T t2 = getT();
		T t3 = getT();

		list.add(t1);
		list.add(t2);
		list.add(t3);

		List<T> c = new ArrayList<T>();

		list.drainTo(c);
		assertTrue(list.isEmpty());
		assertEquals(3, c.size());
		assertThat(c, hasItems(t1, t2, t3));
	}

	@Test
	public void testGetFirst() {
		T t1 = getT();
		T t2 = getT();

		list.add(t1);
		list.add(t2);

		assertThat(list.getFirst(), isEqual(t1));
	}

	@Test
	public void testLast() {
		testAdd();
	}

	@Test
	public void testOfferFirst() {
		testAddFirst();
	}

	@Test
	public void testOfferLast() {
		testAddLast();
	}

	@Test
	public void testPeekFirst() {
		testPeek();
	}

	@Test
	public void testPeekLast() {
		T t1 = getT();
		T t2 = getT();

		list.add(t1);
		list.add(t2);

		assertThat(list.peekLast(), isEqual(t2));
		assertEquals(2, list.size());
	}

	@Test
	public void testPollFirst() {
		testPoll();
	}

	@Test
	public void testPollLast() {
		T t1 = getT();
		T t2 = getT();

		list.add(t1);
		list.add(t2);

		T last = list.pollLast();
		assertThat(last, isEqual(t2));
		assertEquals(1, list.size());
		assertThat(list, hasItem(t1));
	}

	@Test
	public void testPollLastTimeout() throws InterruptedException {

		T t1 = getT();
		T t2 = getT();

		list.add(t1);
		list.add(t2);

		T last = list.pollLast(1, TimeUnit.MILLISECONDS);
		assertThat(last, isEqual(t2));
		assertEquals(1, list.size());
		assertThat(list, hasItem(t1));
	}

	@Test
	public void testPut() {
		testOffer();
	}

	@Test
	public void testPutFirst() {
		testAdd();
	}

	@Test
	public void testPutLast() {
		testPut();
	}

	@Test
	public void testRemainingCapacity() {
		assertEquals(Integer.MAX_VALUE, list.remainingCapacity());
	}

	@Test
	public void testRemoveFirst() {
		testPop();
	}

	@Test
	public void testRemoveFirstOccurrence() {
		testRemove();
	}

	@Test
	public void testRemoveLast() {
		testPollLast();
	}

	@Test
	public void testRmoveLastOccurrence() {
		T t1 = getT();
		T t2 = getT();

		list.add(t1);
		list.add(t2);
		list.add(t1);
		list.add(t2);

		list.removeLastOccurrence(t2);
		assertEquals(3, list.size());
		Iterator<T> iterator = list.iterator();
		assertThat(iterator.next(), isEqual(t1));
		assertThat(iterator.next(), isEqual(t2));
		assertThat(iterator.next(), isEqual(t1));
	}

	@Test
	public void testTake() {
		testPoll();
	}

	@Test
	public void testTakeFirst() {
		testTake();
	}

	@Test
	public void testTakeLast() {
		testPollLast();
	}
}
