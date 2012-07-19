/*
 * Copyright 2011 the original author or authors.
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
package org.springframework.data.redis.listener;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.SmartLifecycle;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.Subscription;
import org.springframework.data.redis.connection.util.ByteArrayWrapper;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.scheduling.SchedulingAwareRunnable;
import org.springframework.util.ClassUtils;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ErrorHandler;

/**
 * Container providing asynchronous behaviour for Redis message listeners.
 * Handles the low level details of listening, converting and message dispatching.
 * <p/>
 * As oppose to the low level Redis (one connection per subscription), the container 
 * uses only one connection that is 'multiplexed' for all registered listeners, 
 * the message dispatch being done through the task executor.
 * 
 * <p/>
 * Note the container uses the connection in a lazy fashion (the connection is used only if at least one listener is configured). 
 * 
 * @author Costin Leau
 */
public class RedisMessageListenerContainer implements InitializingBean, DisposableBean, BeanNameAware, SmartLifecycle {

	/** Logger available to subclasses */
	protected final Log logger = LogFactory.getLog(getClass());



	/**
	 * Default thread name prefix: "RedisListeningContainer-".
	 */
	public static final String DEFAULT_THREAD_NAME_PREFIX = ClassUtils.getShortName(RedisMessageListenerContainer.class)
			+ "-";

	private long initWait = TimeUnit.SECONDS.toMillis(5);

	private Executor subscriptionExecutor;

	private Executor taskExecutor;

	private RedisConnectionFactory connectionFactory;

	private String beanName;

	private ErrorHandler errorHandler;


	private final Object monitor = new Object();
	// whether the container is running (or not)
	private volatile boolean running = false;
	// whether the container has been initialized
	private volatile boolean initialized = false;
	// whether the container uses a connection or not
	// (as the container might be running but w/o listeners, it won't use any resources)
	private volatile boolean listening = false;

	private volatile boolean manageExecutor = false;


	// lookup maps
	// to avoid creation of hashes for each message, the maps use raw byte arrays (wrapped to respect the equals/hashcode contract)

	// lookup map between patterns and listeners
	private final Map<ByteArrayWrapper, Collection<MessageListener>> patternMapping = new ConcurrentHashMap<ByteArrayWrapper, Collection<MessageListener>>();
	// lookup map between channels and listeners
	private final Map<ByteArrayWrapper, Collection<MessageListener>> channelMapping = new ConcurrentHashMap<ByteArrayWrapper, Collection<MessageListener>>();

	private final SubscriptionTask subscriptionTask = new SubscriptionTask();

	private volatile RedisSerializer<String> serializer = new StringRedisSerializer();


	
	public void afterPropertiesSet() {
		if (taskExecutor == null) {
			manageExecutor = true;
			taskExecutor = createDefaultTaskExecutor();
		}

		if (subscriptionExecutor == null) {
			subscriptionExecutor = taskExecutor;
		}

		initialized = true;
	}

	/**
	 * Creates a default TaskExecutor. Called if no explicit TaskExecutor has been specified.
	 * <p>The default implementation builds a {@link org.springframework.core.task.SimpleAsyncTaskExecutor}
	 * with the specified bean name (or the class name, if no bean name specified) as thread name prefix.
	 * @see org.springframework.core.task.SimpleAsyncTaskExecutor#SimpleAsyncTaskExecutor(String)
	 */
	protected TaskExecutor createDefaultTaskExecutor() {
		String threadNamePrefix = (beanName != null ? beanName + "-" : DEFAULT_THREAD_NAME_PREFIX);
		return new SimpleAsyncTaskExecutor(threadNamePrefix);
	}

	
	public void destroy() throws Exception {
		initialized = false;

		stop();

		if (manageExecutor) {
			if (taskExecutor instanceof DisposableBean) {
				((DisposableBean) taskExecutor).destroy();

				if (logger.isDebugEnabled()) {
					logger.debug("Stopped internally-managed task executor");
				}
			}
		}
	}

	
	public boolean isAutoStartup() {
		return true;
	}

	
	public void stop(Runnable callback) {
		stop();
		callback.run();
	}

	
	public int getPhase() {
		// start the latest
		return Integer.MAX_VALUE;
	}

	
	public boolean isRunning() {
		return running;
	}

	
	public void start() {
		if (!running) {
			running = true;
			// wait for the subscription to start before returning
			// technically speaking we can only be notified right before the subscription starts
			synchronized (monitor) {
				lazyListen();
				try {
					// wait up to 5 seconds
					monitor.wait(initWait);
				} catch (InterruptedException e) {
					// stop waiting
				}
			}

			if (logger.isDebugEnabled()) {
				logger.debug("Started RedisMessageListenerContainer");
			}
		}
	}

	
	public void stop() {
		if (isRunning()) {
			running = false;
			synchronized (monitor) {
				boolean shouldWait = listening;
				subscriptionTask.cancel();
				if (shouldWait) {
					try {
						monitor.wait(initWait);
					} catch (InterruptedException ex) {
						// stop waiting
					}
				}
			}
		}

		if (logger.isDebugEnabled()) {
			logger.debug("Stopped RedisMessageListenerContainer");
		}
	}


	/**
	 * Process a message received from the provider.
	 * 
	 * @param message
	 * @param pattern
	 */
	protected void processMessage(MessageListener listener, Message message, byte[] pattern) {
		executeListener(listener, message, pattern);
	}


	/**
	 * Execute the specified listener.
	 * 
	 * @see #handleListenerException
	 */
	protected void executeListener(MessageListener listener, Message message, byte[] pattern) {
		try {
			listener.onMessage(message, pattern);
		} catch (Throwable ex) {
			handleListenerException(ex);
		}
	}

	/**
	 * Return whether this container is currently active,
	 * that is, whether it has been set up but not shut down yet.
	 */
	public final boolean isActive() {
		return initialized;
	}

	/**
	 * Handle the given exception that arose during listener execution.
	 * <p>The default implementation logs the exception at error level.
	 * This can be overridden in subclasses.
	 * @param ex the exception to handle
	 */
	protected void handleListenerException(Throwable ex) {
		if (isActive()) {
			// Regular case: failed while active.
			// Invoke ErrorHandler if available.
			invokeErrorHandler(ex);
		}
		else {
			// Rare case: listener thread failed after container shutdown.
			// Log at debug level, to avoid spamming the shutdown logger.
			logger.debug("Listener exception after container shutdown", ex);
		}
	}

	/**
	 * Invoke the registered ErrorHandler, if any. Log at error level otherwise.
	 * @param ex the uncaught error that arose during message processing.
	 * @see #setErrorHandler
	 */
	protected void invokeErrorHandler(Throwable ex) {
		if (this.errorHandler != null) {
			this.errorHandler.handleError(ex);
		}
		else if (logger.isWarnEnabled()) {
			logger.warn("Execution of JMS message listener failed, and no ErrorHandler has been set.", ex);
		}
	}

	/**
	 * Returns the connectionFactory.
	 *
	 * @return Returns the connectionFactory
	 */
	public RedisConnectionFactory getConnectionFactory() {
		return connectionFactory;
	}

	/**
	 * @param connectionFactory The connectionFactory to set.
	 */
	public void setConnectionFactory(RedisConnectionFactory connectionFactory) {
		this.connectionFactory = connectionFactory;
	}

	
	public void setBeanName(String name) {
		this.beanName = name;
	}


	/**
	 * Sets the task executor used for running the message listeners when messages are received.
	 * If no task executor is set, an instance of {@link SimpleAsyncTaskExecutor} will be used by default.
	 * The task executor can be adjusted depending on the work done by the listeners and the number of 
	 * messages coming in.
	 * 
	 * @param taskExecutor The taskExecutor to set.
	 */
	public void setTaskExecutor(Executor taskExecutor) {
		this.taskExecutor = taskExecutor;
	}

	/**
	 * Sets the task execution used for subscribing to Redis channels. By default, if no executor is set,
	 * the {@link #setTaskExecutor(Executor)} will be used. In some cases, this might be undersired as
	 * the listening to the connection is a long running task.
	 *
	 * <p/>Note: This implementation uses at most one long running thread (depending on whether there are any listeners registered or not)
	 * and up to two threads during the initial registration. 
	 * 
	 * @param subscriptionExecutor The subscriptionExecutor to set.
	 */
	public void setSubscriptionExecutor(Executor subscriptionExecutor) {
		this.subscriptionExecutor = subscriptionExecutor;
	}

	/**
	 * Sets the serializer for converting the {@link Topic}s into low-level channels and patterns.
	 * By default, {@link StringRedisSerializer} is used.
	 * 
	 * @param serializer The serializer to set.
	 */
	public void setTopicSerializer(RedisSerializer<String> serializer) {
		this.serializer = serializer;
	}

	/**
	 * Set an ErrorHandler to be invoked in case of any uncaught exceptions thrown
	 * while processing a Message. By default there will be <b>no</b> ErrorHandler
	 * so that error-level logging is the only result.
	 */
	public void setErrorHandler(ErrorHandler errorHandler) {
		this.errorHandler = errorHandler;
	}

	/**
	 * Attaches the given listeners (and their topics) to the container.
	 * 
	 * <p/>
	 * Note: it's possible to call this method while the container is running forcing a reinitialization
	 * of the container. Note however that this might cause some messages to be lost (while the container
	 * reinitializes) - hence calling this method at runtime is considered advanced usage.
	 * 
	 * @param listeners map of message listeners and their associated topics
	 */
	public void setMessageListeners(Map<? extends MessageListener, Collection<? extends Topic>> listeners) {
		initMapping(listeners);
	}

	/**
	 * Adds a message listener to the (potentially running) container. If the container is running,
	 * the listener starts receiving (matching) messages as soon as possible.
	 * 
	 * @param listener message listener
	 * @param topics message listener topic
	 */
	public void addMessageListener(MessageListener listener, Collection<? extends Topic> topics) {
		addListener(listener, topics);
		lazyListen();
	}

	/**
	 * Adds a message listener to the (potentially running) container. If the container is running,
	 * the listener starts receiving (matching) messages as soon as possible.
	 * 
	 * @param listener message listener
	 * @param topic message topic
	 */
	public void addMessageListener(MessageListener listener, Topic topic) {
		addMessageListener(listener, Collections.singleton(topic));
	}

	private void initMapping(Map<? extends MessageListener, Collection<? extends Topic>> listeners) {
		// stop the listener if currently running
		if (isRunning()) {
			stop();
		}

		patternMapping.clear();
		channelMapping.clear();

		if (!CollectionUtils.isEmpty(listeners)) {
			for (Map.Entry<? extends MessageListener, Collection<? extends Topic>> entry : listeners.entrySet()) {
				addListener(entry.getKey(), entry.getValue());
			}
		}

		// resume activity
		if (initialized) {
			start();
		}
	}

	/**
	 * Method inspecting whether listening for messages (and thus using a thread) is actually needed and triggering it.
	 */
	private void lazyListen() {
		boolean debug = logger.isDebugEnabled();
		boolean started = false;

		if (isRunning()) {
			if (!listening) {
				synchronized (monitor) {
					if (!listening) {
						if (channelMapping.size() > 0 || patternMapping.size() > 0) {
							subscriptionExecutor.execute(subscriptionTask);
							listening = true;
							started = true;
						}
					}
				}
				if (debug) {
					if (started) {
						logger.debug("Started listening for Redis messages");
					}
					else {
						logger.debug("Postpone listening for Redis messages until actual listeners are added");
					}
				}
			}
		}
	}

	private void addListener(MessageListener listener, Collection<? extends Topic> topics) {
		List<byte[]> channels = new ArrayList<byte[]>(topics.size());
		List<byte[]> patterns = new ArrayList<byte[]>(topics.size());

		boolean trace = logger.isTraceEnabled();

		for (Topic topic : topics) {

			ByteArrayWrapper holder = new ByteArrayWrapper(serializer.serialize(topic.getTopic()));

			if (topic instanceof ChannelTopic) {
				Collection<MessageListener> collection = channelMapping.get(holder);
				if (collection == null) {
					collection = new CopyOnWriteArraySet<MessageListener>();
					channelMapping.put(holder, collection);
				}
				collection.add(listener);
				channels.add(holder.getArray());

				if (trace)
					logger.trace("Adding listener '" + listener + "' on channel '" + topic.getTopic() + "'");
			}

			else if (topic instanceof PatternTopic) {
				Collection<MessageListener> collection = patternMapping.get(holder);
				if (collection == null) {
					collection = new CopyOnWriteArraySet<MessageListener>();
					patternMapping.put(holder, collection);
				}
				collection.add(listener);
				patterns.add(holder.getArray());

				if (trace)
					logger.trace("Adding listener '" + listener + "' for pattern '" + topic.getTopic() + "'");
			}

			else {
				throw new IllegalArgumentException("Unknown topic type '" + topic.getClass() + "'");
			}
		}

		// check the current listening state
		if (listening) {
			subscriptionTask.subscribeChannel(channels.toArray(new byte[channels.size()][]));
			subscriptionTask.subscribePattern(patterns.toArray(new byte[patterns.size()][]));
		}
	}


	/**
	 * Runnable used for Redis subscription. Implemented as a dedicated class to provide as many hints
	 * as possible to the underlying thread pool.
	 * 
	 * @author Costin Leau
	 */
	private class SubscriptionTask implements SchedulingAwareRunnable {

		/**
		 * Runnable used, on a parallel thread, to do the initial pSubscribe.
		 * This is required since, during initialization, both subscribe and pSubscribe
		 * might be needed but since the first call is blocking, the second call needs to
		 * executed in parallel.
		 *  
		 * @author Costin Leau
		 */
		private class PatternSubscriptionTask implements SchedulingAwareRunnable {

			private long WAIT = 500;
			private long ROUNDS = 3;

			
			public boolean isLongLived() {
				return false;
			}

			
			public void run() {
				// wait for subscription to be initialized
				boolean done = false;
				// wait 3 rounds for subscription to be initialized
				for (int i = 0; i < ROUNDS || done; i++) {
					if (connection != null) {
						synchronized (localMonitor) {
							if (connection != null && connection.isSubscribed()) {
								done = true;
								connection.getSubscription().pSubscribe(unwrap(patternMapping.keySet()));
							}
							else {
								try {
									Thread.sleep(WAIT);
								} catch (InterruptedException ex) {
									done = true;
								}
							}
						}
					}
				}
			}
		}

		private volatile RedisConnection connection;
		private final Object localMonitor = new Object();

		
		public boolean isLongLived() {
			return true;
		}

		
		public void run() {
			connection = connectionFactory.getConnection();
			try {
				if (connection.isSubscribed()) {
					throw new IllegalStateException("Retrieved connection is already subscribed; aborting listening");
				}

				// NB: each Xsubscribe call blocks

				synchronized (monitor) {
					monitor.notify();
				}

				// subscribe one way or the other
				// and schedule the rest
				if (!channelMapping.isEmpty()) {
					// schedule the rest of the subscription
					if (!patternMapping.isEmpty()) {
						subscriptionExecutor.execute(new PatternSubscriptionTask());
					}
					connection.subscribe(new DispatchMessageListener(), unwrap(channelMapping.keySet()));
				}
				else {
					connection.pSubscribe(new DispatchMessageListener(), unwrap(patternMapping.keySet()));
				}

			} finally {
				// this block is executed once the subscription has ended
				// meaning cleanup is required

				listening = false;

				if (connection != null) {
					synchronized (localMonitor) {
						if (connection != null) {
							connection.close();
							connection = null;
						}
					}
				}

				// done with the thread, app can be destroyed
				synchronized (monitor) {
					monitor.notify();
				}

			}
		}

		private byte[][] unwrap(Collection<ByteArrayWrapper> holders) {
			if (CollectionUtils.isEmpty(holders)) {
				return new byte[0][];
			}

			byte[][] unwrapped = new byte[holders.size()][];

			int index = 0;
			for (ByteArrayWrapper arrayHolder : holders) {
				unwrapped[index++] = arrayHolder.getArray();
			}

			return unwrapped;
		}

		void cancel() {
			if (connection != null) {
				synchronized (localMonitor) {
					if (connection != null) {
						Subscription sub = connection.getSubscription();
						if (sub != null) {
							sub.pUnsubscribe();
							sub.unsubscribe();
						}
					}
				}
			}
		}

		void subscribeChannel(byte[]... channels) {
			if (channels != null && channels.length > 0) {
				if (connection != null) {
					synchronized (localMonitor) {
						if (connection != null) {
							Subscription sub = connection.getSubscription();
							if (sub != null) {
								sub.subscribe(channels);
							}
						}
					}
				}
			}
		}

		void subscribePattern(byte[]... patterns) {
			if (patterns != null && patterns.length > 0) {
				if (connection != null) {
					synchronized (localMonitor) {
						if (connection != null) {
							Subscription sub = connection.getSubscription();
							if (sub != null) {
								sub.pSubscribe(patterns);
							}
						}
					}
				}
			}
		}

		void unsubscribeChannel(byte[]... channels) {
			if (channels != null && channels.length > 0) {
				if (connection != null) {
					synchronized (localMonitor) {
						if (connection != null) {
							Subscription sub = connection.getSubscription();
							if (sub != null) {
								sub.unsubscribe(channels);
							}
						}
					}
				}
			}
		}

		void unsubscribePattern(byte[]... patterns) {
			if (patterns != null && patterns.length > 0) {
				if (connection != null) {
					synchronized (localMonitor) {
						if (connection != null) {
							Subscription sub = connection.getSubscription();
							if (sub != null) {
								sub.pUnsubscribe(patterns);
							}
						}
					}
				}
			}
		}
	}

	/**
	 * Actual message dispatcher/multiplexer.
	 * 
	 * @author Costin Leau
	 */
	private class DispatchMessageListener implements MessageListener {

		
		public void onMessage(Message message, byte[] pattern) {
			// do channel matching first
			byte[] channel = message.getChannel();

			Collection<MessageListener> ch = channelMapping.get(new ByteArrayWrapper(channel));
			Collection<MessageListener> pt = null;

			// followed by pattern matching
			if (pattern != null && pattern.length > 0) {
				pt = patternMapping.get(new ByteArrayWrapper(pattern));
			}

			if (!CollectionUtils.isEmpty(ch)) {
				dispatchChannels(ch, message);
			}

			if (!CollectionUtils.isEmpty(pt)) {
				dispatchPatterns(pt, message, pattern);
			}
		}

		private void dispatchChannels(Collection<MessageListener> ch, final Message message) {
			for (final MessageListener messageListener : ch) {
				taskExecutor.execute(new Runnable() {
					
					public void run() {
						processMessage(messageListener, message, message.getChannel());
					}
				});
			}
		}

		private void dispatchPatterns(Collection<MessageListener> pt, final Message message, final byte[] pattern) {
			for (final MessageListener messageListener : pt) {
				taskExecutor.execute(new Runnable() {
					
					public void run() {
						processMessage(messageListener, message, pattern.clone());
					}
				});
			}
		}
	}
}