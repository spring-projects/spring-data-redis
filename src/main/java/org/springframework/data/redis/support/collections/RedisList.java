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

import static org.springframework.data.redis.connection.RedisListCommands.*;

import java.time.Duration;
import java.util.Deque;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.TimeUnit;

import org.springframework.data.redis.core.TimeoutUtils;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Redis extension for the {@link List} contract. Supports {@link List}, {@link Queue} and {@link Deque} contracts as
 * well as their equivalent blocking siblings {@link BlockingDeque} and {@link BlockingDeque}.
 *
 * @author Costin Leau
 * @author Mark Paluch
 */
public interface RedisList<E> extends RedisCollection<E>, List<E>, BlockingDeque<E> {

	/**
	 * Atomically returns and removes the first element of the list stored at the bound key, and pushes the element at the
	 * first/last element (head/tail depending on the {@link Direction destinationPosition} argument) of the list stored
	 * at {@link RedisList destination}.
	 *
	 * @param destination must not be {@literal null}.
	 * @param destinationPosition must not be {@literal null}.
	 * @return
	 * @since 2.6
	 * @see Direction#first()
	 * @see Direction#last()
	 */
	@Nullable
	E moveFirstTo(RedisList<E> destination, Direction destinationPosition);

	/**
	 * Atomically returns and removes the first element of the list stored at the bound key, and pushes the element at the
	 * first/last element (head/tail depending on the {@link Direction destinationPosition} argument) of the list stored
	 * at {@link RedisList destination}.
	 * <p/>
	 * <b>Blocks connection</b> until element available or {@code timeout} reached.
	 *
	 * @param destination must not be {@literal null}.
	 * @param destinationPosition must not be {@literal null}.
	 * @param timeout must not be {@literal null} or negative.
	 * @return
	 * @since 2.6
	 * @see Direction#first()
	 * @see Direction#last()
	 */
	@Nullable
	default E moveFirstTo(RedisList<E> destination, Direction destinationPosition, Duration timeout) {

		Assert.notNull(timeout, "Timeout must not be null");
		Assert.isTrue(!timeout.isNegative(), "Timeout must not be negative");

		return moveFirstTo(destination, destinationPosition,
				TimeoutUtils.toMillis(timeout.toMillis(), TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
	}

	/**
	 * Atomically returns and removes the first element of the list stored at the bound key, and pushes the element at the
	 * first/last element (head/tail depending on the {@link Direction destinationPosition} argument) of the list stored
	 * at {@link RedisList destination}.
	 * <p/>
	 * <b>Blocks connection</b> until element available or {@code timeout} reached.
	 *
	 * @param destination must not be {@literal null}.
	 * @param destinationPosition must not be {@literal null}.
	 * @param timeout
	 * @param unit must not be {@literal null}.
	 * @return
	 * @since 2.6
	 * @see Direction#first()
	 * @see Direction#last()
	 */
	@Nullable
	E moveFirstTo(RedisList<E> destination, Direction destinationPosition, long timeout, TimeUnit unit);

	/**
	 * Atomically returns and removes the last element of the list stored at the bound key, and pushes the element at the
	 * first/last element (head/tail depending on the {@link Direction destinationPosition} argument) of the list stored
	 * at {@link RedisList destination}.
	 *
	 * @param destination must not be {@literal null}.
	 * @param destinationPosition must not be {@literal null}.
	 * @return
	 * @since 2.6
	 * @see Direction#first()
	 * @see Direction#last()
	 */
	@Nullable
	E moveLastTo(RedisList<E> destination, Direction destinationPosition);

	/**
	 * Atomically returns and removes the last element of the list stored at the bound key, and pushes the element at the
	 * first/last element (head/tail depending on the {@link Direction destinationPosition} argument) of the list stored
	 * at {@link RedisList destination}.
	 * <p/>
	 * <b>Blocks connection</b> until element available or {@code timeout} reached.
	 *
	 * @param destination must not be {@literal null}.
	 * @param destinationPosition must not be {@literal null}.
	 * @param timeout must not be {@literal null} or negative.
	 * @return
	 * @since 2.6
	 * @see Direction#first()
	 * @see Direction#last()
	 */
	@Nullable
	default E moveLastTo(RedisList<E> destination, Direction destinationPosition, Duration timeout) {

		Assert.notNull(timeout, "Timeout must not be null");
		Assert.isTrue(!timeout.isNegative(), "Timeout must not be negative");

		return moveLastTo(destination, destinationPosition,
				TimeoutUtils.toMillis(timeout.toMillis(), TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
	}

	/**
	 * Atomically returns and removes the last element of the list stored at the bound key, and pushes the element at the
	 * first/last element (head/tail depending on the {@link Direction destinationPosition} argument) of the list stored
	 * at {@link RedisList destination}.
	 * <p/>
	 * <b>Blocks connection</b> until element available or {@code timeout} reached.
	 *
	 * @param destination must not be {@literal null}.
	 * @param destinationPosition must not be {@literal null}.
	 * @param timeout
	 * @param unit must not be {@literal null}.
	 * @return
	 * @since 2.6
	 * @see Direction#first()
	 * @see Direction#last()
	 */
	@Nullable
	E moveLastTo(RedisList<E> destination, Direction destinationPosition, long timeout, TimeUnit unit);

	List<E> range(long begin, long end);

	RedisList<E> trim(int begin, int end);
}
