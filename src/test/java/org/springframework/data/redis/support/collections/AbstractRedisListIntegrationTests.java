/*
 * Copyright 2011-2021 the original author or authors.
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

import static org.assertj.core.api.Assertions.*;
import static org.assertj.core.api.Assumptions.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;

import org.springframework.data.redis.ObjectFactory;
import org.springframework.data.redis.connection.RedisListCommands;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.test.condition.EnabledOnCommand;
import org.springframework.data.redis.test.extension.parametrized.ParameterizedRedisTest;

/**
 * Integration test for RedisList
 *
 * @author Costin Leau
 * @author Jennifer Hickey
 * @author Mark Paluch
 */
public abstract class AbstractRedisListIntegrationTests<T> extends AbstractRedisCollectionIntegrationTests<T> {

	protected RedisList<T> list;

	/**
	 * Constructs a new <code>AbstractRedisListTests</code> instance.
	 *
	 * @param factory
	 * @param template
	 */
	@SuppressWarnings("rawtypes")
	AbstractRedisListIntegrationTests(ObjectFactory<T> factory, RedisTemplate template) {
		super(factory, template);
	}

	@SuppressWarnings("unchecked")
	@BeforeEach
	public void setUp() throws Exception {

		super.setUp();
		list = (RedisList<T>) collection;
	}

	@ParameterizedRedisTest
	void testAddIndexObjectHead() {
		T t1 = getT();
		T t2 = getT();
		T t3 = getT();

		list.add(t1);
		list.add(t2);

		assertThat(list.get(0)).isEqualTo(t1);
		list.add(0, t3);
		assertThat(list.get(0)).isEqualTo(t3);
	}

	@ParameterizedRedisTest
	void testAddIndexObjectTail() {
		T t1 = getT();
		T t2 = getT();
		T t3 = getT();

		list.add(t1);
		list.add(t2);

		assertThat(list.get(1)).isEqualTo(t2);
		list.add(2, t3);
		assertThat(list.get(2)).isEqualTo(t3);
	}

	@ParameterizedRedisTest
	void testAddIndexObjectMiddle() {
		T t1 = getT();
		T t2 = getT();
		T t3 = getT();

		list.add(t1);
		list.add(t2);

		assertThat(list.get(0)).isEqualTo(t1);
		assertThatIllegalArgumentException().isThrownBy(() -> list.add(1, t3));
	}

	@ParameterizedRedisTest
	void addAllIndexCollectionHead() {
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

	@ParameterizedRedisTest
	void addAllIndexCollectionTail() {
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

	@ParameterizedRedisTest
	void addAllIndexCollectionMiddle() {
		T t1 = getT();
		T t2 = getT();
		T t3 = getT();
		T t4 = getT();

		list.add(t1);
		list.add(t2);

		List<T> asList = Arrays.asList(t3, t4);

		assertThat(list.get(0)).isEqualTo(t1);
		assertThatIllegalArgumentException().isThrownBy(() -> list.addAll(1, asList));
	}

	@ParameterizedRedisTest // DATAREDIS-1196
	@EnabledOnCommand("LPOS")
	void testIndexOfObject() {

		assumeThat(template.getConnectionFactory()).isInstanceOf(LettuceConnectionFactory.class);

		T t1 = getT();
		T t2 = getT();

		assertThat(list.indexOf(t1)).isEqualTo(-1);
		list.add(t1);
		assertThat(list.indexOf(t1)).isEqualTo(0);

		assertThat(list.indexOf(t2)).isEqualTo(-1);
		list.add(t2);
		assertThat(list.indexOf(t2)).isEqualTo(1);
	}

	@ParameterizedRedisTest
	void testOffer() {
		T t1 = getT();

		assertThat(list.offer(t1)).isTrue();
		assertThat(list.get(0)).isEqualTo(t1);
	}

	@ParameterizedRedisTest
	void testPeek() {
		assertThat(list.peek()).isNull();
		T t1 = getT();
		list.add(t1);
		assertThat(list.peek()).isEqualTo(t1);
		list.clear();
		assertThat(list.peek()).isNull();
	}

	@ParameterizedRedisTest
	void testElement() {

		assertThatExceptionOfType(NoSuchElementException.class).isThrownBy(list::element);

		T t1 = getT();
		list.add(t1);
		assertThat(list.element()).isEqualTo(t1);
		list.clear();
		assertThatExceptionOfType(NoSuchElementException.class).isThrownBy(list::element);
	}

	@ParameterizedRedisTest
	void testPop() {
		testPoll();
	}

	@ParameterizedRedisTest
	void testPoll() {
		assertThat(list.poll()).isNull();
		T t1 = getT();
		list.add(t1);
		assertThat(list.poll()).isEqualTo(t1);
		assertThat(list.poll()).isNull();
	}

	@ParameterizedRedisTest
	void testPollTimeout() throws InterruptedException {

		T t1 = getT();
		list.add(t1);
		assertThat(list.poll(1, TimeUnit.MILLISECONDS)).isEqualTo(t1);
	}

	@ParameterizedRedisTest
	void testRemove() {
		assertThatExceptionOfType(NoSuchElementException.class).isThrownBy(list::remove);

		T t1 = getT();
		list.add(t1);
		assertThat(list.remove()).isEqualTo(t1);
		assertThatExceptionOfType(NoSuchElementException.class).isThrownBy(list::remove);
	}

	@ParameterizedRedisTest // GH-2039
	@EnabledOnCommand("LMOVE")
	void testMoveFirstTo() {

		RedisList<T> target = new DefaultRedisList<T>(template.boundListOps(collection.getKey() + ":target"));

		T t1 = getT();
		T t2 = getT();
		T t3 = getT();

		list.add(t1);
		list.add(t2);
		list.add(t3);

		assertThat(list.moveFirstTo(target, RedisListCommands.Direction.first())).isEqualTo(t1);
		assertThat(list.moveFirstTo(target, RedisListCommands.Direction.first())).isEqualTo(t2);
		assertThat(list.moveFirstTo(target, RedisListCommands.Direction.last())).isEqualTo(t3);
		assertThat(list).isEmpty();
		assertThat(target).hasSize(3).containsSequence(t2, t1, t3);
	}

	@ParameterizedRedisTest // GH-2039
	@EnabledOnCommand("LMOVE")
	void testMoveLastTo() {

		RedisList<T> target = new DefaultRedisList<T>(template.boundListOps(collection.getKey() + ":target"));

		T t1 = getT();
		T t2 = getT();
		T t3 = getT();

		list.add(t1);
		list.add(t2);
		list.add(t3);

		assertThat(list.moveLastTo(target, RedisListCommands.Direction.first())).isEqualTo(t3);
		assertThat(list.moveLastTo(target, RedisListCommands.Direction.first())).isEqualTo(t2);
		assertThat(list.moveLastTo(target, RedisListCommands.Direction.last())).isEqualTo(t1);
		assertThat(list).isEmpty();
		assertThat(target).hasSize(3).containsSequence(t2, t3, t1);
	}

	@ParameterizedRedisTest
	void testRange() {
		T t1 = getT();
		T t2 = getT();

		assertThat(list.range(0, -1)).isEmpty();
		list.add(t1);
		list.add(t2);
		assertThat(list.range(0, -1)).hasSize(2);
		assertThat(list.range(0, 0).get(0)).isEqualTo(t1);
		assertThat(list.range(1, 1).get(0)).isEqualTo(t2);
	}

	@ParameterizedRedisTest
	void testRemoveIndex() {
		assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(() -> list.remove(0));
	}

	@ParameterizedRedisTest
	void testSet() {
		T t1 = getT();
		T t2 = getT();
		list.add(t1);
		list.set(0, t1);
		assertThat(list.set(0, t2)).isEqualTo(t1);
		assertThat(list.get(0)).isEqualTo(t2);
	}

	@ParameterizedRedisTest
	void testTrim() {
		T t1 = getT();
		T t2 = getT();

		assertThat(list.trim(0, 0)).isEmpty();
		list.add(t1);
		list.add(t2);
		assertThat(list).hasSize(2);
		assertThat(list.trim(0, 0)).hasSize(1);
		assertThat(list).hasSize(1);
		assertThat(list.get(0)).isEqualTo(t1);
	}

	@SuppressWarnings("unchecked")
	@ParameterizedRedisTest
	void testCappedCollection() throws Exception {
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

	@ParameterizedRedisTest
	void testAddFirst() {
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

	@ParameterizedRedisTest
	void testAddLast() {
		testAdd();
	}

	@ParameterizedRedisTest
	void testDescendingIterator() {
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

	@ParameterizedRedisTest
	void testDrainToCollectionWithMaxElements() {
		T t1 = getT();
		T t2 = getT();
		T t3 = getT();

		list.add(t1);
		list.add(t2);
		list.add(t3);

		List<T> c = new ArrayList<>();

		list.drainTo(c, 2);
		assertThat(list).hasSize(1).contains(t3);
		assertThat(c).hasSize(2).contains(t1, t2);
	}

	@ParameterizedRedisTest
	void testDrainToCollection() {
		T t1 = getT();
		T t2 = getT();
		T t3 = getT();

		list.add(t1);
		list.add(t2);
		list.add(t3);

		List<T> c = new ArrayList<>();

		list.drainTo(c);
		assertThat(list).isEmpty();
		assertThat(c).hasSize(3).contains(t1, t2, t3);
	}

	@ParameterizedRedisTest
	void testGetFirst() {
		T t1 = getT();
		T t2 = getT();

		list.add(t1);
		list.add(t2);

		assertThat(list.getFirst()).isEqualTo(t1);
	}

	@ParameterizedRedisTest
	void testLast() {
		testAdd();
	}

	@ParameterizedRedisTest
	void testOfferFirst() {
		testAddFirst();
	}

	@ParameterizedRedisTest
	void testOfferLast() {
		testAddLast();
	}

	@ParameterizedRedisTest
	void testPeekFirst() {
		testPeek();
	}

	@ParameterizedRedisTest
	void testPeekLast() {
		T t1 = getT();
		T t2 = getT();

		list.add(t1);
		list.add(t2);

		assertThat(list.peekLast()).isEqualTo(t2);
		assertThat(list).hasSize(2);
	}

	@ParameterizedRedisTest
	void testPollFirst() {
		testPoll();
	}

	@ParameterizedRedisTest
	void testPollLast() {
		T t1 = getT();
		T t2 = getT();

		list.add(t1);
		list.add(t2);

		T last = list.pollLast();
		assertThat(last).isEqualTo(t2);
		assertThat(list).hasSize(1).contains(t1);
	}

	@ParameterizedRedisTest
	void testPollLastTimeout() throws InterruptedException {

		T t1 = getT();
		T t2 = getT();

		list.add(t1);
		list.add(t2);

		T last = list.pollLast(1, TimeUnit.MILLISECONDS);
		assertThat(last).isEqualTo(t2);
		assertThat(list).hasSize(1).contains(t1);
	}

	@ParameterizedRedisTest
	void testPut() {
		testOffer();
	}

	@ParameterizedRedisTest
	void testPutFirst() {
		testAdd();
	}

	@ParameterizedRedisTest
	void testPutLast() {
		testPut();
	}

	@ParameterizedRedisTest
	void testRemainingCapacity() {
		assertThat(list.remainingCapacity()).isEqualTo(Integer.MAX_VALUE);
	}

	@ParameterizedRedisTest
	void testRemoveFirst() {
		testPop();
	}

	@ParameterizedRedisTest
	void testRemoveFirstOccurrence() {
		testRemove();
	}

	@ParameterizedRedisTest
	void testRemoveLast() {
		testPollLast();
	}

	@ParameterizedRedisTest
	void testRmoveLastOccurrence() {

		T t1 = getT();
		T t2 = getT();

		list.add(t1);
		list.add(t2);
		list.add(t1);
		list.add(t2);

		list.removeLastOccurrence(t2);
		assertThat(list).hasSize(3).containsExactly(t1, t2, t1);
	}

	@ParameterizedRedisTest
	void testTake() {
		testPoll();
	}

	@ParameterizedRedisTest
	void testTakeFirst() {
		testTake();
	}

	@ParameterizedRedisTest
	void testTakeLast() {
		testPollLast();
	}

	@ParameterizedRedisTest // DATAREDIS-1196
	@EnabledOnCommand("LPOS")
	void lastIndexOf() {

		assumeThat(template.getConnectionFactory()).isInstanceOf(LettuceConnectionFactory.class);

		T t1 = getT();
		T t2 = getT();
		T t3 = getT();

		list.add(t1);
		list.add(t2);
		list.add(t1);
		list.add(t3);

		assertThat(list.lastIndexOf(t1)).isEqualTo(2);
	}
}
