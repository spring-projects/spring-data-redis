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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;
import static org.junit.Assume.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.springframework.data.redis.ObjectFactory;
import org.springframework.data.redis.connection.ConnectionUtils;
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

		assertThat(list.get(0)).isEqualTo(t1);
		list.add(0, t3);
		assertThat(list.get(0)).isEqualTo(t3);
	}

	@Test
	public void testAddIndexObjectTail() {
		T t1 = getT();
		T t2 = getT();
		T t3 = getT();

		list.add(t1);
		list.add(t2);

		assertThat(list.get(1)).isEqualTo(t2);
		list.add(2, t3);
		assertThat(list.get(2)).isEqualTo(t3);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testAddIndexObjectMiddle() {
		T t1 = getT();
		T t2 = getT();
		T t3 = getT();

		list.add(t1);
		list.add(t2);

		assertThat(list.get(0)).isEqualTo(t1);
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

		assertThat(list.get(0)).isEqualTo(t1);
		list.addAll(0, asList);
		// verify insertion order
		assertThat(list.get(0)).isEqualTo(t3);
		assertThat(list.get(1)).isEqualTo(t4);
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

		assertThat(list.get(0)).isEqualTo(t1);
		assertThat(list.addAll(2, asList)).isTrue();

		// verify insertion order
		assertThat(list.get(2)).isEqualTo(t3);
		assertThat(list.get(3)).isEqualTo(t4);
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

		assertThat(list.get(0)).isEqualTo(t1);
		assertThat(list.addAll(1, asList)).isTrue();
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testIndexOfObject() {
		T t1 = getT();
		T t2 = getT();

		assertThat(list.indexOf(t1)).isEqualTo(-1);
		list.add(t1);
		assertThat(list.indexOf(t1)).isEqualTo(0);

		assertThat(list.indexOf(t2)).isEqualTo(-1);
		list.add(t2);
		assertThat(list.indexOf(t1)).isEqualTo(1);
	}

	@Test
	public void testOffer() {
		T t1 = getT();

		assertThat(list.offer(t1)).isTrue();
		assertThat(list.get(0)).isEqualTo(t1);
	}

	@Test
	public void testPeek() {
		assertThat(list.peek()).isNull();
		T t1 = getT();
		list.add(t1);
		assertThat(list.peek()).isEqualTo(t1);
		list.clear();
		assertThat(list.peek()).isNull();
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
		assertThat(list.element()).isEqualTo(t1);
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
		assertThat(list.poll()).isNull();
		T t1 = getT();
		list.add(t1);
		assertThat(list.poll()).isEqualTo(t1);
		assertThat(list.poll()).isNull();
	}

	@Test
	public void testPollTimeout() throws InterruptedException {
		assumeTrue(!ConnectionUtils.isJredis(template.getConnectionFactory()));
		T t1 = getT();
		list.add(t1);
		assertThat(list.poll(1, TimeUnit.MILLISECONDS)).isEqualTo(t1);
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
		assertThat(list.remove()).isEqualTo(t1);
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

		assertThat(list.range(0, -1).isEmpty()).isTrue();
		list.add(t1);
		list.add(t2);
		assertThat(list.range(0, -1)).hasSize(2);
		assertThat(list.range(0, 0).get(0)).isEqualTo(t1);
		assertThat(list.range(1, 1).get(0)).isEqualTo(t2);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testRemoveIndex() {
		T t1 = getT();
		T t2 = getT();

		assertThat(list.remove(0)).isNull();
		list.add(t1);
		list.add(t2);
		assertThat(list.remove(2)).isNull();
		assertThat(list.remove(1)).isEqualTo(t2);
		assertThat(list.remove(0)).isEqualTo(t1);
	}

	@Test
	public void testSet() {
		T t1 = getT();
		T t2 = getT();
		list.add(t1);
		list.set(0, t1);
		assertThat(list.set(0, t2)).isEqualTo(t1);
		assertThat(list.get(0)).isEqualTo(t2);
	}

	@Test
	public void testTrim() {
		T t1 = getT();
		T t2 = getT();

		assertThat(list.trim(0, 0).isEmpty()).isTrue();
		list.add(t1);
		list.add(t2);
		assertThat(list).hasSize(2);
		assertThat(list.trim(0, 0)).hasSize(1);
		assertThat(list).hasSize(1);
		assertThat(list.get(0)).isEqualTo(t1);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testCappedCollection() throws Exception {
		RedisList<T> cappedList = new DefaultRedisList<T>(template.boundListOps(collection.getKey() + ":capped"), 1);
		T first = getT();
		cappedList.offer(first);
		assertThat(cappedList).hasSize(1);
		cappedList.add(getT());
		assertThat(cappedList).hasSize(1);
		T last = getT();
		cappedList.add(last);
		assertThat(cappedList).hasSize(1);
		assertThat(cappedList.get(0)).isEqualTo(first);
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
		assertThat(iterator.next()).isEqualTo(t3);
		assertThat(iterator.next()).isEqualTo(t2);
		assertThat(iterator.next()).isEqualTo(t1);
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
		assertThat(iterator.next()).isEqualTo(t3);
		assertThat(iterator.next()).isEqualTo(t2);
		assertThat(iterator.next()).isEqualTo(t1);

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
		assertThat(list).hasSize(1);
		assertThat(list).contains(t3);
		assertThat(c).hasSize(2);
		assertThat(c).contains(t1, t2);
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
		assertThat(list.isEmpty()).isTrue();
		assertThat(c).hasSize(3);
		assertThat(c).contains(t1, t2, t3);
	}

	@Test
	public void testGetFirst() {
		T t1 = getT();
		T t2 = getT();

		list.add(t1);
		list.add(t2);

		assertThat(list.getFirst()).isEqualTo(t1);
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

		assertThat(list.peekLast()).isEqualTo(t2);
		assertThat(list).hasSize(2);
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
		assertThat(last).isEqualTo(t2);
		assertThat(list).hasSize(1);
		assertThat(list).contains(t1);
	}

	@Test
	public void testPollLastTimeout() throws InterruptedException {
		assumeTrue(!ConnectionUtils.isJredis(template.getConnectionFactory()));
		T t1 = getT();
		T t2 = getT();

		list.add(t1);
		list.add(t2);

		T last = list.pollLast(1, TimeUnit.MILLISECONDS);
		assertThat(last).isEqualTo(t2);
		assertThat(list).hasSize(1);
		assertThat(list).contains(t1);
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
		assertThat(list.remainingCapacity()).isEqualTo(Integer.MAX_VALUE);
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
		assertThat(list).hasSize(3);
		Iterator<T> iterator = list.iterator();
		assertThat(iterator.next()).isEqualTo(t1);
		assertThat(iterator.next()).isEqualTo(t2);
		assertThat(iterator.next()).isEqualTo(t1);
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
