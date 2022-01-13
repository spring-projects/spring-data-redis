/*
 * Copyright 2022 the original author or authors.
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

import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author lilin
 */
public class WeakHashLock<T> {

	private final ConcurrentHashMap<T, WeakLockRef<T, ReentrantLock>> lockMap = new ConcurrentHashMap<>();
	private final ReferenceQueue<ReentrantLock> queue = new ReferenceQueue<>();

	public ReentrantLock get(T key) {
		if (lockMap.size() > 1000) {
			clearEmptyRef();
		}
		WeakReference<ReentrantLock> lockRef = lockMap.get(key);
		ReentrantLock lock = (lockRef == null ? null : lockRef.get());
		while (lock == null) {
			lockMap.putIfAbsent(key, new WeakLockRef<>(new ReentrantLock(), queue, key));
			lockRef = lockMap.get(key);
			lock = (lockRef == null ? null : lockRef.get());
			if (lock != null) {
				return lock;
			}
			clearEmptyRef();
		}
		return lock;
	}

	@SuppressWarnings("unchecked")
	private void clearEmptyRef() {
		Reference<? extends ReentrantLock> ref;
		while ((ref = queue.poll()) != null) {
			WeakLockRef<T, ? extends ReentrantLock> weakLockRef = (WeakLockRef<T, ? extends ReentrantLock>) ref;
			lockMap.remove(weakLockRef.key);
		}
	}

	private static final class WeakLockRef<T, K> extends WeakReference<K> {
		final T key;

		private WeakLockRef(K referent, ReferenceQueue<? super K> q, T key) {
			super(referent, q);
			this.key = key;
		}
	}
}