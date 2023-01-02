/*
 * Copyright 2021-2023 the original author or authors.
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
package org.springframework.data.redis.cache;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;

import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.util.Assert;

/**
 * A collection of predefined {@link BatchStrategy} implementations using {@code KEYS} or {@code SCAN} command.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.6
 */
public abstract class BatchStrategies {

	private BatchStrategies() {
		// can't touch this - oh-oh oh oh oh-oh-oh
	}

	/**
	 * A {@link BatchStrategy} using a single {@code KEYS} and {@code DEL} command to remove all matching keys.
	 * {@code KEYS} scans the entire keyspace of the Redis database and can block the Redis worker thread for a long time
	 * on large keyspaces.
	 * <p>
	 * {@code KEYS} is supported for standalone and clustered (sharded) Redis operation modes.
	 *
	 * @return batching strategy using {@code KEYS}.
	 */
	public static BatchStrategy keys() {
		return Keys.INSTANCE;
	}

	/**
	 * A {@link BatchStrategy} using a {@code SCAN} cursors and potentially multiple {@code DEL} commands to remove all
	 * matching keys. This strategy allows a configurable batch size to optimize for scan batching.
	 * <p>
	 * Note that using the {@code SCAN} strategy might be not supported on all drivers and Redis operation modes.
	 *
	 * @return batching strategy using {@code SCAN}.
	 */
	public static BatchStrategy scan(int batchSize) {

		Assert.isTrue(batchSize > 0, "Batch size must be greater than zero!");

		return new Scan(batchSize);
	}

	/**
	 * {@link BatchStrategy} using {@code KEYS}.
	 */
	static class Keys implements BatchStrategy {

		static Keys INSTANCE = new Keys();

		@Override
		public long cleanCache(RedisConnection connection, String name, byte[] pattern) {

			byte[][] keys = Optional.ofNullable(connection.keys(pattern)).orElse(Collections.emptySet())
					.toArray(new byte[0][]);

			if (keys.length > 0) {
				connection.del(keys);
			}

			return keys.length;
		}
	}

	/**
	 * {@link BatchStrategy} using {@code SCAN}.
	 */
	static class Scan implements BatchStrategy {

		private final int batchSize;

		Scan(int batchSize) {
			this.batchSize = batchSize;
		}

		@Override
		public long cleanCache(RedisConnection connection, String name, byte[] pattern) {

			Cursor<byte[]> cursor = connection.scan(ScanOptions.scanOptions().count(batchSize).match(pattern).build());

			long count = 0;

			PartitionIterator<byte[]> partitions = new PartitionIterator<>(cursor, batchSize);
			while (partitions.hasNext()) {

				List<byte[]> keys = partitions.next();
				count += keys.size();

				if (keys.size() > 0) {
					connection.del(keys.toArray(new byte[0][]));
				}
			}

			return count;
		}
	}

	/**
	 * Utility to split and buffer outcome from a {@link Iterator} into {@link List lists} of {@code T} with a maximum
	 * chunks {@code size}.
	 *
	 * @param <T>
	 */
	static class PartitionIterator<T> implements Iterator<List<T>> {

		private final Iterator<T> iterator;
		private final int size;

		PartitionIterator(Iterator<T> iterator, int size) {

			this.iterator = iterator;
			this.size = size;
		}

		@Override
		public boolean hasNext() {
			return iterator.hasNext();
		}

		@Override
		public List<T> next() {

			if (!hasNext()) {
				throw new NoSuchElementException();
			}

			List<T> list = new ArrayList<>(size);
			while (list.size() < size && iterator.hasNext()) {
				list.add(iterator.next());
			}

			return list;
		}
	}
}
