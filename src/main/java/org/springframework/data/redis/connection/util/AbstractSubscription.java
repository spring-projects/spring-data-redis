/*
 * Copyright 2011-2018 the original author or authors.
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
package org.springframework.data.redis.connection.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;

import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.RedisInvalidSubscriptionException;
import org.springframework.data.redis.connection.Subscription;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

/**
 * Base implementation for a subscription handling the channel/pattern registration so subclasses only have to deal with
 * the actual registration/unregistration.
 *
 * @author Costin Leau
 * @author Christoph Strobl
 */
public abstract class AbstractSubscription implements Subscription {

	private final Collection<ByteArrayWrapper> channels = new ArrayList<>(2);
	private final Collection<ByteArrayWrapper> patterns = new ArrayList<>(2);
	private final AtomicBoolean alive = new AtomicBoolean(true);
	private final MessageListener listener;

	protected AbstractSubscription(MessageListener listener) {
		this(listener, null, null);
	}

	/**
	 * Constructs a new <code>AbstractSubscription</code> instance. Allows channels and patterns to be added to the
	 * subscription w/o triggering a subscription action (as some clients (Jedis) require an initial call before entering
	 * into listening mode).
	 *
	 * @param listener must not be {@literal null}.
	 * @param channels can be {@literal null}.
	 * @param patterns can be {@literal null}.
	 */
	protected AbstractSubscription(MessageListener listener, @Nullable byte[][] channels, @Nullable byte[][] patterns) {

		Assert.notNull(listener, "MessageListener must not be null!");

		this.listener = listener;

		synchronized (this.channels) {
			add(this.channels, channels);
		}
		synchronized (this.patterns) {
			add(this.patterns, patterns);
		}
	}

	/**
	 * Subscribe to the given channels.
	 *
	 * @param channels channels to subscribe to
	 */
	protected abstract void doSubscribe(byte[]... channels);

	/**
	 * Channel unsubscribe.
	 *
	 * @param all true if all the channels are unsubscribed (used as a hint for the underlying implementation).
	 * @param channels channels to be unsubscribed
	 */
	protected abstract void doUnsubscribe(boolean all, byte[]... channels);

	/**
	 * Subscribe to the given patterns
	 *
	 * @param patterns patterns to subscribe to
	 */
	protected abstract void doPsubscribe(byte[]... patterns);

	/**
	 * Pattern unsubscribe.
	 *
	 * @param all true if all the patterns are unsubscribed (used as a hint for the underlying implementation).
	 * @param patterns patterns to be unsubscribed
	 */
	protected abstract void doPUnsubscribe(boolean all, byte[]... patterns);

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.Subscription#close()
	 */
	@Override
	public void close() {
		doClose();
	}

	/**
	 * Shutdown the subscription and free any resources held.
	 */
	protected abstract void doClose();

	public MessageListener getListener() {
		return listener;
	}

	public Collection<byte[]> getChannels() {
		synchronized (channels) {
			return clone(channels);
		}
	}

	public Collection<byte[]> getPatterns() {
		synchronized (patterns) {
			return clone(patterns);
		}
	}

	public void pSubscribe(byte[]... patterns) {
		checkPulse();

		Assert.notEmpty(patterns, "at least one pattern required");

		synchronized (this.patterns) {
			add(this.patterns, patterns);
		}

		doPsubscribe(patterns);
	}

	public void pUnsubscribe() {
		pUnsubscribe((byte[][]) null);
	}

	public void subscribe(byte[]... channels) {
		checkPulse();

		Assert.notEmpty(channels, "at least one channel required");

		synchronized (this.channels) {
			add(this.channels, channels);
		}

		doSubscribe(channels);
	}

	public void unsubscribe() {
		unsubscribe((byte[][]) null);
	}

	public void pUnsubscribe(@Nullable byte[]... patts) {
		if (!isAlive()) {
			return;
		}

		// shortcut for unsubscribing all patterns
		if (ObjectUtils.isEmpty(patts)) {
			if (!this.patterns.isEmpty()) {
				synchronized (this.patterns) {
					patts = getPatterns().toArray(new byte[this.patterns.size()][]);
					doPUnsubscribe(true, patts);
					this.patterns.clear();
				}
			} else {
				// nothing to unsubscribe from
				return;
			}
		} else {
			doPUnsubscribe(false, patts);
			synchronized (this.patterns) {
				remove(this.patterns, patts);
			}
		}

		closeIfUnsubscribed();
	}

	public void unsubscribe(@Nullable byte[]... chans) {
		if (!isAlive()) {
			return;
		}

		// shortcut for unsubscribing all channels
		if (ObjectUtils.isEmpty(chans)) {
			if (!this.channels.isEmpty()) {
				synchronized (this.channels) {
					chans = getChannels().toArray(new byte[this.channels.size()][]);
					doUnsubscribe(true, chans);
					this.channels.clear();
				}
			} else {
				// nothing to unsubscribe from
				return;
			}
		} else {
			doUnsubscribe(false, chans);
			synchronized (this.channels) {
				remove(this.channels, chans);
			}
		}

		closeIfUnsubscribed();
	}

	public boolean isAlive() {
		return alive.get();
	}

	private void checkPulse() {
		if (!isAlive()) {
			throw new RedisInvalidSubscriptionException("Subscription has been unsubscribed and cannot be used anymore");
		}
	}

	private void closeIfUnsubscribed() {
		if (channels.isEmpty() && patterns.isEmpty()) {
			alive.set(false);
			doClose();
		}
	}

	private static Collection<byte[]> clone(Collection<ByteArrayWrapper> col) {
		Collection<byte[]> list = new ArrayList<>(col.size());
		for (ByteArrayWrapper wrapper : col) {
			list.add(wrapper.getArray().clone());
		}
		return list;
	}

	private static void add(Collection<ByteArrayWrapper> col, @Nullable byte[]... bytes) {
		if (!ObjectUtils.isEmpty(bytes)) {
			for (byte[] bs : bytes) {
				col.add(new ByteArrayWrapper(bs));
			}
		}
	}

	private static void remove(Collection<ByteArrayWrapper> col, @Nullable byte[]... bytes) {
		if (!ObjectUtils.isEmpty(bytes)) {
			for (byte[] bs : bytes) {
				col.remove(new ByteArrayWrapper(bs));
			}
		}
	}
}
