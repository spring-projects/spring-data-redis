/*
 * Copyright 2011-2023 the original author or authors.
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
package org.springframework.data.redis.listener;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.SmartLifecycle;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.connection.ConnectionUtils;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.Subscription;
import org.springframework.data.redis.connection.SubscriptionListener;
import org.springframework.data.redis.connection.util.ByteArrayWrapper;
import org.springframework.data.redis.listener.adapter.RedisListenerExecutionFailedException;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ErrorHandler;
import org.springframework.util.ObjectUtils;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.BackOffExecution;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Container providing asynchronous behaviour for Redis message listeners. Handles the low level details of listening,
 * converting and message dispatching.
 * <p>
 * As opposed to the low level Redis (one connection per subscription), the container uses only one connection that is
 * 'multiplexed' for all registered listeners, the message dispatch being done through the
 * {@link #setTaskExecutor(Executor) task executor}. It is recommended to configure the task executor (and subscription
 * executor when using a blocking Redis connector) instead of using the default {@link SimpleAsyncTaskExecutor} for
 * reuse of thread pools.
 * <p>
 * The container uses a single Redis connection in a lazy fashion (the connection is used only if at least one listener
 * is configured). Listeners can be registered eagerly before {@link #start() starting} the container to subscribe to
 * all registered topics upon startup. Listeners are guaranteed to be subscribed after the {@link #start()} method
 * returns.
 * <p>
 * Subscriptions are retried gracefully using {@link BackOff} that can be configured through
 * {@link #setRecoveryInterval(long)} until reaching the maximum number of attempts. Listener errors are handled through
 * a {@link ErrorHandler} if configured.
 * <p>
 * This class can be used concurrently after initializing the container with {@link #afterPropertiesSet()} and
 * {@link #start()} allowing concurrent calls to {@link #addMessageListener} and {@link #removeMessageListener} without
 * external synchronization.
 * <p>
 * {@link MessageListener Listeners} that wish to receive subscription/unsubscription callbacks in response to
 * subscribe/unsubscribe commands can implement {@link SubscriptionListener}.
 *
 * @author Costin Leau
 * @author Jennifer Hickey
 * @author Way Joke
 * @author Thomas Darimont
 * @author Mark Paluch
 * @author John Blum
 * @see MessageListener
 * @see SubscriptionListener
 */
public class RedisMessageListenerContainer implements InitializingBean, DisposableBean, BeanNameAware, SmartLifecycle {

	/**
	 * The default recovery interval: 5000 ms = 5 seconds.
	 */
	public static final long DEFAULT_RECOVERY_INTERVAL = FixedBackOff.DEFAULT_INTERVAL;

	/**
	 * The default subscription wait time: 2000 ms = 2 seconds.
	 */
	public static final long DEFAULT_SUBSCRIPTION_REGISTRATION_WAIT_TIME = 2000L;

	/**
	 * Default thread name prefix: "RedisListeningContainer-".
	 */
	public static final String DEFAULT_THREAD_NAME_PREFIX = ClassUtils.getShortName(RedisMessageListenerContainer.class)
			+ "-";

	/** Logger available to subclasses */
	protected final Log logger = LogFactory.getLog(getClass());

	private @Nullable ErrorHandler errorHandler;

	private @Nullable Executor subscriptionExecutor;

	private @Nullable Executor taskExecutor;

	private @Nullable RedisConnectionFactory connectionFactory;

	private RedisSerializer<String> serializer = RedisSerializer.string();

	private long maxSubscriptionRegistrationWaitingTime = DEFAULT_SUBSCRIPTION_REGISTRATION_WAIT_TIME;

	private BackOff backOff = new FixedBackOff(DEFAULT_RECOVERY_INTERVAL, FixedBackOff.UNLIMITED_ATTEMPTS);
	private @Nullable String beanName;

	// whether the container has been initialized via afterPropertiesSet
	private boolean afterPropertiesSet = false;

	// whether the TaskExecutor was created by the container
	private boolean manageExecutor = false;

	private @Nullable Subscriber subscriber;

	private final AtomicBoolean started = new AtomicBoolean();

	// whether the container is running (or not)
	private final AtomicReference<State> state = new AtomicReference<>(State.notListening());

	// Lookup maps; to avoid creation of hashes for each message, the maps use raw byte arrays (wrapped to respect
	// the equals/hashcode contract)

	// lookup map between channels and listeners
	private final Map<ByteArrayWrapper, Collection<MessageListener>> channelMapping = new ConcurrentHashMap<>();
	// lookup map between patterns and listeners
	private final Map<ByteArrayWrapper, Collection<MessageListener>> patternMapping = new ConcurrentHashMap<>();
	// lookup map between listeners and channels
	private final Map<MessageListener, Set<Topic>> listenerTopics = new ConcurrentHashMap<>();

	private volatile CompletableFuture<Void> listenFuture = new CompletableFuture<>();

	private volatile CompletableFuture<Void> unsubscribeFuture = new CompletableFuture<>();

	/**
	 * Set an ErrorHandler to be invoked in case of any uncaught exceptions thrown while processing a Message. By default,
	 * there will be <b>no</b> ErrorHandler so that error-level logging is the only result.
	 */
	public void setErrorHandler(ErrorHandler errorHandler) {
		this.errorHandler = errorHandler;
	}

	/**
	 * Sets the task execution used for subscribing to Redis channels. By default, if no executor is set, the
	 * {@link #setTaskExecutor(Executor)} will be used. In some cases, this might be undesired as the listening to the
	 * connection is a long-running task.
	 * <p>
	 * Note: This implementation uses at most one long-running thread (depending on whether there are any listeners
	 * registered or not) and up to two threads during the initial registration.
	 *
	 * @param subscriptionExecutor the subscriptionExecutor to set.
	 */
	public void setSubscriptionExecutor(Executor subscriptionExecutor) {
		this.subscriptionExecutor = subscriptionExecutor;
	}

	/**
	 * Sets the task executor used for running the message listeners when messages are received. If no task executor is
	 * set, an instance of {@link SimpleAsyncTaskExecutor} will be used by default. The task executor can be adjusted
	 * depending on the work done by the listeners and the number of messages coming in.
	 *
	 * @param taskExecutor the taskExecutor to set.
	 */
	public void setTaskExecutor(Executor taskExecutor) {
		this.taskExecutor = taskExecutor;
	}

	/**
	 * Returns the connectionFactory.
	 *
	 * @return Returns the connectionFactory
	 */
	@Nullable
	public RedisConnectionFactory getConnectionFactory() {
		return this.connectionFactory;
	}

	/**
	 * @param connectionFactory The connectionFactory to set.
	 */
	public void setConnectionFactory(RedisConnectionFactory connectionFactory) {

		Assert.notNull(connectionFactory, "ConnectionFactory must not be null");

		this.connectionFactory = connectionFactory;
	}

	/**
	 * Sets the serializer for converting the {@link Topic}s into low-level channels and patterns. By default,
	 * {@link StringRedisSerializer} is used.
	 *
	 * @param serializer The serializer to set.
	 */
	public void setTopicSerializer(RedisSerializer<String> serializer) {
		this.serializer = serializer;
	}

	public long getMaxSubscriptionRegistrationWaitingTime() {
		return this.maxSubscriptionRegistrationWaitingTime;
	}

	/**
	 * Specify the max time to wait for subscription registrations, in <strong>milliseconds</strong> The default is
	 * {@code 2000ms}, that is, 2 second. The timeout applies for awaiting the subscription registration. Note that
	 * subscriptions can be created asynchronously and an expired timeout does not cancel the timeout.
	 *
	 * @param maxSubscriptionRegistrationWaitingTime the maximum subscription registration wait time
	 * @see #DEFAULT_SUBSCRIPTION_REGISTRATION_WAIT_TIME
	 * @see #start()
	 */
	public void setMaxSubscriptionRegistrationWaitingTime(long maxSubscriptionRegistrationWaitingTime) {
		this.maxSubscriptionRegistrationWaitingTime = maxSubscriptionRegistrationWaitingTime;
	}

	/**
	 * Specify the interval between recovery attempts, in <b>milliseconds</b>. The default is 5000 ms, that is, 5 seconds.
	 *
	 * @see #handleSubscriptionException
	 * @see #setRecoveryBackoff(BackOff)
	 */
	public void setRecoveryInterval(long recoveryInterval) {
		setRecoveryBackoff(new FixedBackOff(recoveryInterval, FixedBackOff.UNLIMITED_ATTEMPTS));
	}

	/**
	 * Specify the interval {@link BackOff} recovery attempts.
	 *
	 * @see #handleSubscriptionException
	 * @see #setRecoveryInterval(long)
	 * @since 2.7
	 */
	public void setRecoveryBackoff(BackOff recoveryInterval) {

		Assert.notNull(recoveryInterval, "Recovery interval must not be null");

		this.backOff = recoveryInterval;
	}

	/**
	 * Attaches the given listeners (and their topics) to the container.
	 * <p>
	 * Note: it's possible to call this method while the container is running forcing a reinitialization of the container.
	 * Note however that this might cause some messages to be lost (while the container reinitializes) - hence calling
	 * this method at runtime is considered advanced usage.
	 *
	 * @param listeners map of message listeners and their associated topics
	 */
	public void setMessageListeners(Map<? extends MessageListener, Collection<? extends Topic>> listeners) {
		initMapping(listeners);
	}

	@Override
	public void setBeanName(String name) {
		this.beanName = name;
	}

	@Override
	public void afterPropertiesSet() {

		Assert.state(!this.afterPropertiesSet, "Container already initialized");
		Assert.notNull(this.connectionFactory, "RedisConnectionFactory is not set");

		if (this.taskExecutor == null) {
			this.manageExecutor = true;
			this.taskExecutor = createDefaultTaskExecutor();
		}

		if (this.subscriptionExecutor == null) {
			this.subscriptionExecutor = this.taskExecutor;
		}

		this.subscriber = createSubscriber(connectionFactory, this.subscriptionExecutor);
		this.afterPropertiesSet = true;
	}

	/**
	 * Creates a default TaskExecutor. Called if no explicit TaskExecutor has been specified.
	 * <p>
	 * The default implementation builds a {@link org.springframework.core.task.SimpleAsyncTaskExecutor} with the
	 * specified bean name (or the class name, if no bean name specified) as thread name prefix.
	 *
	 * @see org.springframework.core.task.SimpleAsyncTaskExecutor#SimpleAsyncTaskExecutor(String)
	 */
	protected TaskExecutor createDefaultTaskExecutor() {
		String threadNamePrefix = this.beanName != null ? this.beanName + "-" : DEFAULT_THREAD_NAME_PREFIX;
		return new SimpleAsyncTaskExecutor(threadNamePrefix);
	}

	/**
	 * Destroy the container and stop it.
	 *
	 * @throws Exception
	 */
	@Override
	public void destroy() throws Exception {

		this.afterPropertiesSet = false;

		stop();

		if (this.manageExecutor) {
			if (this.taskExecutor instanceof DisposableBean bean) {
				bean.destroy();
				logDebug(() -> "Stopped internally-managed task executor");
			}
		}
	}

	/**
	 * Startup the container and subscribe to topics if {@link MessageListener listeners} were registered prior to
	 * starting the container.
	 * <p>
	 * This method is a potentially blocking method that blocks until a previous {@link #stop()} is finished and until all
	 * previously registered listeners are successfully subscribed.
	 * <p>
	 * Multiple calls to this method are ignored if the container is already running. Concurrent calls to this method are
	 * synchronized until the container is started up.
	 *
	 * @see #setRecoveryInterval(long)
	 * @see #setMaxSubscriptionRegistrationWaitingTime(long)
	 * @see #stop()
	 */
	@Override
	public void start() {

		if (started.compareAndSet(false, true)) {
			logDebug(() -> "Starting RedisMessageListenerContainer...");
			lazyListen();
		}
	}

	/**
	 * Lazily initiate subscriptions if the container has listeners.
	 */
	private void lazyListen() {

		CompletableFuture<Void> containerListenFuture = this.listenFuture;
		State state = this.state.get();

		CompletableFuture<Void> futureToAwait = state.isPrepareListening() ? containerListenFuture
				: lazyListen(this.backOff.start());

		try {
			futureToAwait.get(getMaxSubscriptionRegistrationWaitingTime(), TimeUnit.MILLISECONDS);
		} catch (InterruptedException cause) {
			Thread.currentThread().interrupt();
		} catch (ExecutionException cause) {

			if (cause.getCause() instanceof DataAccessException) {
				throw new RedisListenerExecutionFailedException(cause.getMessage(), cause.getCause());
			}

			throw new CompletionException(cause.getCause());
		} catch (TimeoutException cause) {
			throw new IllegalStateException("Subscription registration timeout exceeded", cause);
		}
	}

	/**
	 * Method inspecting whether listening for messages (and thus using a thread) is actually needed and triggering it.
	 */
	private CompletableFuture<Void> lazyListen(BackOffExecution backOffExecution) {

		if (!hasTopics()) {
			logger.debug("Postpone listening for Redis messages until actual listeners are added");
			return CompletableFuture.completedFuture(null);
		}

		CompletableFuture<Void> containerListenFuture = this.listenFuture;

		while (!doSubscribe(backOffExecution)) {
			// busy-loop, allow for synchronization against doUnsubscribe therefore we want to retry.
			containerListenFuture = this.listenFuture;
		}

		return containerListenFuture;
	}

	private boolean doSubscribe(BackOffExecution backOffExecution) {

		CompletableFuture<Void> containerListenFuture = this.listenFuture;
		CompletableFuture<Void> containerUnsubscribeFuture = this.unsubscribeFuture;

		State state = this.state.get();

		// someone has called stop while we were in here.
		if (!state.isPrepareListening() && state.isListening()) {
			containerUnsubscribeFuture.join();
		}

		if (!this.state.compareAndSet(state, State.prepareListening())) {
			return false;
		}

		CompletableFuture<Void> listenFuture = getRequiredSubscriber().initialize(backOffExecution,
				patternMapping.keySet().stream().map(ByteArrayWrapper::getArray).collect(Collectors.toList()),
				channelMapping.keySet().stream().map(ByteArrayWrapper::getArray).collect(Collectors.toList()));
		listenFuture.whenComplete((unused, throwable) -> {

			if (throwable == null) {
				logger.debug("RedisMessageListenerContainer listeners registered successfully");
				this.state.set(State.listening());
			} else {
				logger.debug("Failed to start RedisMessageListenerContainer listeners", throwable);
				this.state.set(State.notListening());
			}

			propagate(unused, throwable, containerListenFuture);

			// re-arm listen future for a later lazy-listen attempt
			if (throwable != null) {
				this.listenFuture = new CompletableFuture<>();
			}
		});

		logger.debug("Subscribing to topics for RedisMessageListenerContainer");

		return true;
	}

	/**
	 * Stop the message listener container and cancel any subscriptions if the container is {@link #isListening()
	 * listening}. Stopping releases any allocated connections.
	 * <p>
	 * This method is a potentially blocking method that blocks until a previous {@link #start()} is finished and until
	 * the connection is closed if the container was listening.
	 * <p>
	 * Multiple calls to this method are ignored if the container was already stopped. Concurrent calls to this method are
	 * synchronized until the container is stopped.
	 */
	@Override
	public void stop() {
		stop(() -> {});
	}

	/**
	 * Stop the message listener container and cancel any subscriptions if the container is {@link #isListening()
	 * listening}. Stopping releases any allocated connections.
	 * <p>
	 * This method is a potentially blocking method that blocks until a previous {@link #start()} is finished and until
	 * the connection is closed if the container was listening.
	 * <p>
	 * Multiple calls to this method are ignored if the container was already stopped. Concurrent calls to this method are
	 * synchronized until the container is stopped.
	 *
	 * @param callback callback to notify when the container actually stops.
	 */
	@Override
	public void stop(Runnable callback) {

		if (this.started.compareAndSet(true, false)) {

			stopListening();

			if (logger.isDebugEnabled()) {
				logger.debug("Stopped RedisMessageListenerContainer");
			}

			callback.run();
		}
	}

	private void stopListening() {
		while (!doUnsubscribe()) {
			// busy-loop, allow for synchronization against doSubscribe therefore we want to retry.
		}
	}

	private boolean doUnsubscribe() {

		CompletableFuture<Void> listenFuture = this.listenFuture;
		State state = this.state.get();

		if (!state.isListenerActivated()) {
			return true;
		}

		awaitRegistrationTime(listenFuture);

		if (this.state.compareAndSet(state, State.prepareUnsubscribe())) {

			getRequiredSubscriber().unsubscribeAll();

			awaitRegistrationTime(this.unsubscribeFuture);

			this.state.set(State.notListening());

			this.listenFuture = new CompletableFuture<>();
			this.unsubscribeFuture = new CompletableFuture<>();

			if (logger.isDebugEnabled()) {
				logger.debug("Stopped listening");
			}

			return true;
		} else {
			return false;
		}
	}

	private void awaitRegistrationTime(CompletableFuture<Void> future) {

		try {
			future.get(getMaxSubscriptionRegistrationWaitingTime(), TimeUnit.MILLISECONDS);
		} catch (InterruptedException cause) {
			Thread.currentThread().interrupt();
		} catch (ExecutionException | TimeoutException ignore) {}
	}

	@Override
	public boolean isRunning() {
		return this.started.get();
	}

	public boolean isListening() {
		return this.state.get().isListening();
	}

	/**
	 * Return whether this container is currently active, that is, whether it has been set up but not shut down yet.
	 */
	public final boolean isActive() {
		return this.afterPropertiesSet;
	}

	/**
	 * Adds a message listener to the (potentially running) container. If the container is running, the listener starts
	 * receiving (matching) messages as soon as possible.
	 *
	 * @param listener message listener
	 * @param topics message listener topic
	 */
	public void addMessageListener(MessageListener listener, Collection<? extends Topic> topics) {
		addListener(listener, topics);
	}

	/**
	 * Adds a message listener to the (potentially running) container. If the container is running, the listener starts
	 * receiving (matching) messages as soon as possible.
	 *
	 * @param listener message listener
	 * @param topic message topic
	 */
	public void addMessageListener(MessageListener listener, Topic topic) {
		addMessageListener(listener, Collections.singleton(topic));
	}

	/**
	 * Removes a message listener from the given topics. If the container is running, the listener stops receiving
	 * (matching) messages as soon as possible.
	 * <p>
	 * Note that this method obeys the Redis (p)unsubscribe semantics - meaning an empty/null collection will remove
	 * listener from all channels.
	 *
	 * @param listener message listener
	 * @param topics message listener topics
	 */
	public void removeMessageListener(@Nullable MessageListener listener, Collection<? extends Topic> topics) {
		removeListener(listener, topics);
	}

	/**
	 * Removes a message listener from the given topic. If the container is running, the listener stops receiving
	 * (matching) messages as soon as possible.
	 * <p>
	 * Note that this method obeys the Redis (p)unsubscribe semantics - meaning an empty/null collection will remove
	 * listener from all channels.
	 *
	 * @param listener message listener
	 * @param topic message topic
	 */
	public void removeMessageListener(@Nullable MessageListener listener, Topic topic) {
		removeMessageListener(listener, Collections.singleton(topic));
	}

	/**
	 * Removes the given message listener completely (from all topics). If the container is running, the listener stops
	 * receiving (matching) messages as soon as possible.
	 *
	 * @param listener message listener
	 */
	public void removeMessageListener(MessageListener listener) {

		Assert.notNull(listener, "MessageListener must not be null");

		removeMessageListener(listener, Collections.emptySet());
	}

	private void initMapping(Map<? extends MessageListener, Collection<? extends Topic>> listeners) {

		// stop the listener if currently running
		if (isRunning()) {
			stop();
		}

		patternMapping.clear();
		channelMapping.clear();
		listenerTopics.clear();

		if (!CollectionUtils.isEmpty(listeners)) {
			for (Map.Entry<? extends MessageListener, Collection<? extends Topic>> entry : listeners.entrySet()) {
				addListener(entry.getKey(), entry.getValue());
			}
		}

		// resume activity
		if (this.afterPropertiesSet) {
			start();
		}
	}

	private void addListener(MessageListener listener, Collection<? extends Topic> topics) {

		Assert.notNull(listener, "a valid listener is required");
		Assert.notEmpty(topics, "at least one topic is required");

		List<byte[]> channels = new ArrayList<>(topics.size());
		List<byte[]> patterns = new ArrayList<>(topics.size());

		// add listener mapping
		Set<Topic> set = listenerTopics.get(listener);

		if (set == null) {
			set = new CopyOnWriteArraySet<>();
			listenerTopics.put(listener, set);
		}

		set.addAll(topics);

		for (Topic topic : topics) {

			ByteArrayWrapper serializedTopic = new ByteArrayWrapper(serialize(topic));

			if (topic instanceof ChannelTopic) {
				Collection<MessageListener> collection = resolveMessageListeners(this.channelMapping, serializedTopic);
				collection.add(listener);
				channels.add(serializedTopic.getArray());
				logTrace(() -> String.format("Adding listener '%s' on channel '%s'", listener, topic.getTopic()));
			} else if (topic instanceof PatternTopic) {
				Collection<MessageListener> collection = resolveMessageListeners(this.patternMapping, serializedTopic);
				collection.add(listener);
				patterns.add(serializedTopic.getArray());
				logTrace(() -> String.format("Adding listener '%s' for pattern '%s'", listener, topic.getTopic()));
			} else {
				throw new IllegalArgumentException(String.format("Unknown topic type '%s'", topic.getClass()));
			}
		}
		boolean wasListening = isListening();

		if (isRunning()) {

			lazyListen();

			// check the current listening state
			if (wasListening) {

				CompletableFuture<Void> future = new CompletableFuture<>();

				getRequiredSubscriber().addSynchronization(new SynchronizingMessageListener.SubscriptionSynchronization(
						patterns, channels, () -> future.complete(null)));
				getRequiredSubscriber().subscribeChannel(channels.toArray(new byte[channels.size()][]));
				getRequiredSubscriber().subscribePattern(patterns.toArray(new byte[patterns.size()][]));

				try {
					future.join();
				} catch (CompletionException cause) {

					if (cause.getCause() instanceof DataAccessException) {
						throw new RedisListenerExecutionFailedException(cause.getMessage(), cause.getCause());
					}

					throw cause;
				}
			}
		}
	}

	private Collection<MessageListener> resolveMessageListeners(
			Map<ByteArrayWrapper, Collection<MessageListener>> mapping, ByteArrayWrapper topic) {

		Collection<MessageListener> messageListeners = mapping.get(topic);

		if (messageListeners == null) {
			messageListeners = new CopyOnWriteArraySet<>();
			mapping.put(topic, messageListeners);
		}

		return messageListeners;
	}

	private void removeListener(@Nullable MessageListener listener, Collection<? extends Topic> topics) {

		Assert.notNull(topics, "Topics must not be null");

		if (listener != null && listenerTopics.get(listener) == null) {
			// Listener not subscribed
			return;
		}

		if (topics.isEmpty()) {
			topics = listenerTopics.get(listener);
		}

		// check stop listening case
		if (CollectionUtils.isEmpty(topics)) {
			stopListening();
			return;
		}

		List<byte[]> channelsToRemove = new ArrayList<>();
		List<byte[]> patternsToRemove = new ArrayList<>();

		// check unsubscribe all topics case
		if (CollectionUtils.isEmpty(topics)) {
			Set<Topic> set = listenerTopics.remove(listener);
			// listener not found, bail out
			if (set == null) {
				return;
			}
			topics = set;
		}

		for (Topic topic : topics) {

			ByteArrayWrapper holder = new ByteArrayWrapper(serialize(topic));

			if (topic instanceof ChannelTopic) {
				remove(listener, topic, holder, channelMapping, channelsToRemove);
				logTrace(() -> String.format("Removing listener '%s' from channel '%s'", listener, topic.getTopic()));
			}

			else if (topic instanceof PatternTopic) {
				remove(listener, topic, holder, patternMapping, patternsToRemove);
				logTrace(() -> String.format("Removing listener '%s' from pattern '%s'", listener, topic.getTopic()));
			}
		}

		// double check whether there are still subscriptions available otherwise cancel the connection
		// as most drivers forfeit the connection on unsubscribe
		if (listenerTopics.isEmpty()) {
			stopListening();
		}
		// check the current listening state
		else if (isListening()) {
			getRequiredSubscriber().unsubscribeChannel(channelsToRemove.toArray(new byte[channelsToRemove.size()][]));
			getRequiredSubscriber().unsubscribePattern(patternsToRemove.toArray(new byte[patternsToRemove.size()][]));
		}
	}

	private void remove(@Nullable MessageListener listener, Topic topic, ByteArrayWrapper holder,
			Map<ByteArrayWrapper, Collection<MessageListener>> mapping, List<byte[]> topicToRemove) {

		Collection<MessageListener> listeners = mapping.get(holder);
		Collection<MessageListener> listenersToRemove = null;

		if (listeners != null) {
			// remove only one listener
			listeners.remove(listener);
			listenersToRemove = Collections.singletonList(listener);

			// start removing listeners
			for (MessageListener messageListener : listenersToRemove) {
				Set<Topic> topics = listenerTopics.get(messageListener);
				if (topics != null) {
					topics.remove(topic);
				}
				if (CollectionUtils.isEmpty(topics)) {
					listenerTopics.remove(messageListener);
				}
			}

			// if we removed everything, remove the empty holder collection
			if (listeners.isEmpty()) {
				mapping.remove(holder);
				topicToRemove.add(holder.getArray());
			}
		}
	}

	private Subscriber createSubscriber(RedisConnectionFactory connectionFactory, Executor executor) {
		return ConnectionUtils.isAsync(connectionFactory) ? new Subscriber(connectionFactory)
				: new BlockingSubscriber(connectionFactory, executor);
	}

	/**
	 * Process a message received from the provider.
	 *
	 * @param listener the message listener to notify.
	 * @param message the received message.
	 * @param source the source, either the channel or pattern.
	 * @see #handleListenerException
	 */
	protected void processMessage(MessageListener listener, Message message, byte[] source) {

		try {
			listener.onMessage(message, source);
		} catch (Throwable cause) {
			handleListenerException(cause);
		}
	}

	/**
	 * Handle the given exception that arose during listener execution.
	 * <p>
	 * The default implementation logs the exception at error level. This can be overridden in subclasses.
	 *
	 * @param cause the exception to handle
	 */
	protected void handleListenerException(Throwable cause) {

		if (isActive()) {
			// Regular case: failed while active.
			// Invoke ErrorHandler if available.
			invokeErrorHandler(cause);
		} else {
			// Rare case: listener thread failed after container shutdown.
			// Log at debug level, to avoid spamming the shutdown logger.
			logger.debug("Listener exception after container shutdown", cause);
		}
	}

	/**
	 * Invoke the registered ErrorHandler, if any. Log at error level otherwise.
	 *
	 * @param cause the uncaught error that arose during message processing.
	 * @see #setErrorHandler
	 */
	protected void invokeErrorHandler(Throwable cause) {

		if (this.errorHandler != null) {
			this.errorHandler.handleError(cause);
		} else if (logger.isWarnEnabled()) {
			logger.warn("Execution of message listener failed, and no ErrorHandler has been set", cause);
		}
	}

	/**
	 * Handle subscription task exception. Will attempt to restart the subscription if the Exception is a connection
	 * failure (for example, Redis was restarted).
	 *
	 * @param ex Throwable exception
	 */
	protected void handleSubscriptionException(CompletableFuture<Void> future, BackOffExecution backOffExecution,
			Throwable ex) {

		getRequiredSubscriber().closeConnection();

		if (ex instanceof RedisConnectionFailureException && isRunning()) {

			BackOffExecution loggingBackOffExecution = () -> {

				long recoveryInterval = backOffExecution.nextBackOff();

				if (recoveryInterval != BackOffExecution.STOP) {
					logger.error(String.format("Connection failure occurred: %s; Restarting subscription task after %s ms", ex,
							recoveryInterval), ex);
				}

				return recoveryInterval;
			};

			Runnable recoveryFunction = () -> {

				CompletableFuture<Void> lazyListen = lazyListen(backOffExecution);
				lazyListen.whenComplete(propagate(future));
			};

			if (potentiallyRecover(loggingBackOffExecution, recoveryFunction)) {
				return;
			}

			logger.error("SubscriptionTask aborted with exception:", ex);
			future.completeExceptionally(new IllegalStateException("Subscription attempts exceeded", ex));
			return;
		}

		if (isRunning()) { // log only if the container is still running to prevent close errors from logging
			logger.error("SubscriptionTask aborted with exception:", ex);
		}

		future.completeExceptionally(ex);
	}

	/**
	 * Sleep according to the specified recovery interval. Called between recovery attempts.
	 */
	private boolean potentiallyRecover(BackOffExecution backOffExecution, Runnable retryRunnable) {

		long recoveryInterval = backOffExecution.nextBackOff();

		if (recoveryInterval == BackOffExecution.STOP) {
			return false;
		}

		try {
			if (subscriptionExecutor instanceof ScheduledExecutorService) {
				((ScheduledExecutorService) subscriptionExecutor).schedule(retryRunnable, recoveryInterval,
						TimeUnit.MILLISECONDS);
			} else {
				Thread.sleep(recoveryInterval);
				retryRunnable.run();
			}

			return true;
		} catch (InterruptedException interEx) {
			logger.debug("Thread interrupted while sleeping the recovery interval");
			Thread.currentThread().interrupt();
			return false;
		}
	}

	private <T> BiConsumer<? super T, ? super Throwable> propagate(CompletableFuture<T> target) {
		return (value, throwable) -> propagate(value, throwable, target);
	}

	private <T> void propagate(@Nullable T value, @Nullable Throwable throwable, CompletableFuture<T> target) {

		if (throwable != null) {
			target.completeExceptionally(throwable);
		} else {
			target.complete(value);
		}
	}

	private void dispatchSubscriptionNotification(Collection<MessageListener> listeners, byte[] pattern, long count,
			SubscriptionConsumer listenerConsumer) {

		if (!CollectionUtils.isEmpty(listeners)) {

			byte[] source = pattern.clone();
			Executor executor = getRequiredTaskExecutor();

			for (MessageListener messageListener : listeners) {
				if (messageListener instanceof SubscriptionListener) {
					executor.execute(() -> listenerConsumer.accept((SubscriptionListener) messageListener, source, count));
				}
			}
		}
	}

	private void dispatchMessage(Collection<MessageListener> listeners, Message message, @Nullable byte[] pattern) {

		byte[] source = (pattern != null ? pattern.clone() : message.getChannel());
		Executor executor = getRequiredTaskExecutor();

		for (MessageListener messageListener : listeners) {
			executor.execute(() -> processMessage(messageListener, message, source));
		}
	}

	private boolean hasTopics() {
		return !this.channelMapping.isEmpty() || !this.patternMapping.isEmpty();
	}

	private Subscriber getRequiredSubscriber() {

		Assert.state(this.subscriber != null,
				"Subscriber not created; Configure RedisConnectionFactory to create a Subscriber");

		return this.subscriber;
	}

	private Executor getRequiredTaskExecutor() {

		Assert.state(this.taskExecutor != null, "No executor configured");

		return this.taskExecutor;
	}

	@SuppressWarnings("ConstantConditions")
	private byte[] serialize(Topic topic) {
		return serializer.serialize(topic.getTopic());
	}

	private void logDebug(Supplier<String> message) {

		if (this.logger.isDebugEnabled()) {
			this.logger.debug(message.get());
		}
	}

	private void logTrace(Supplier<String> message) {

		if (this.logger.isTraceEnabled()) {
			this.logger.trace(message.get());
		}
	}

	/**
	 * Represents an operation that accepts three input arguments {@link SubscriptionListener},
	 * {@code channel or pattern}, and {@code count} and returns no result.
	 */
	interface SubscriptionConsumer {
		void accept(SubscriptionListener listener, byte[] channelOrPattern, long count);
	}

	/**
	 * Container listening state.
	 *
	 * @author Mark Paluch
	 * @since 2.7
	 */
	static class State {

		private final boolean prepareListening;
		private final boolean listening;

		private State(boolean prepareListening, boolean listening) {

			this.prepareListening = prepareListening;
			this.listening = listening;
		}

		/**
		 * Initial state. Next state is {@link #prepareListening()}.
		 */
		static State notListening() {
			return new State(false, false);
		}

		/**
		 * Prepare listening after {@link #notListening()}. Next states are either {@link #notListening()} upon failure or
		 * {@link #listening()}.
		 */
		static State prepareListening() {
			return new State(true, false);
		}

		/**
		 * Active listening state after {@link #prepareListening()}. Next is {@link #prepareUnsubscribe()}.
		 */
		static State listening() {
			return new State(true, true);
		}

		/**
		 * Prepare unsubscribe after {@link #listening()}. Next state is {@link #notListening()}.
		 */
		static State prepareUnsubscribe() {
			return new State(false, true);
		}

		private boolean isListenerActivated() {
			return isListening() || isPrepareListening();
		}

		public boolean isListening() {
			return listening;
		}

		public boolean isPrepareListening() {
			return prepareListening;
		}
	}

	/**
	 * Actual message dispatcher/multiplexer.
	 *
	 * @author Costin Leau
	 */
	private class DispatchMessageListener implements MessageListener, SubscriptionListener {

		@Override
		public void onMessage(Message message, @Nullable byte[] pattern) {
			Collection<MessageListener> listeners = null;

			// if it's a pattern, disregard channel
			if (pattern != null && pattern.length > 0) {
				listeners = patternMapping.get(new ByteArrayWrapper(pattern));
			} else {
				pattern = null;
				// do channel matching first
				listeners = channelMapping.get(new ByteArrayWrapper(message.getChannel()));
			}

			if (!CollectionUtils.isEmpty(listeners)) {
				dispatchMessage(listeners, message, pattern);
			}
		}

		@Override
		public void onChannelSubscribed(byte[] channel, long count) {
			dispatchSubscriptionNotification(
					channelMapping.getOrDefault(new ByteArrayWrapper(channel), Collections.emptyList()), channel, count,
					SubscriptionListener::onChannelSubscribed);
		}

		@Override
		public void onChannelUnsubscribed(byte[] channel, long count) {
			dispatchSubscriptionNotification(
					channelMapping.getOrDefault(new ByteArrayWrapper(channel), Collections.emptyList()), channel, count,
					SubscriptionListener::onChannelUnsubscribed);
		}

		@Override
		public void onPatternSubscribed(byte[] pattern, long count) {
			dispatchSubscriptionNotification(
					patternMapping.getOrDefault(new ByteArrayWrapper(pattern), Collections.emptyList()), pattern, count,
					SubscriptionListener::onPatternSubscribed);
		}

		@Override
		public void onPatternUnsubscribed(byte[] pattern, long count) {
			dispatchSubscriptionNotification(
					patternMapping.getOrDefault(new ByteArrayWrapper(pattern), Collections.emptyList()), pattern, count,
					SubscriptionListener::onPatternUnsubscribed);
		}
	}

	/**
	 * Topic subscriber controller. Keeps track of the actual Redis connection and provides entry points to initially
	 * subscribe to Redis topics and update subscriptions (add/remove).
	 * <p>
	 * Actual subscription notifications are routed through {@link DispatchMessageListener} to multicast events to the
	 * actual listeners without blocking the event loop.
	 *
	 * @author Mark Paluch
	 * @since 2.7
	 */
	class Subscriber {

		private volatile @Nullable RedisConnection connection;

		private final RedisConnectionFactory connectionFactory;
		private final Object localMonitor = new Object();
		private final DispatchMessageListener delegateListener = new DispatchMessageListener();
		private final SynchronizingMessageListener synchronizingMessageListener = new SynchronizingMessageListener(
				delegateListener, delegateListener);

		Subscriber(RedisConnectionFactory connectionFactory) {
			this.connectionFactory = connectionFactory;
		}

		/**
		 * Perform the initial subscription.
		 *
		 * @param backOffExecution backoff execution to track the progress for retries.
		 * @param patterns patterns to subscribe to.
		 * @param channels channels to subscribe to.
		 * @return a future that is completed either successfully after establishing all subscriptions or exceptionally
		 *         after an error or when running out of {@link BackOffExecution#STOP retries}.
		 */
		public CompletableFuture<Void> initialize(BackOffExecution backOffExecution, Collection<byte[]> patterns,
				Collection<byte[]> channels) {

			synchronized (localMonitor) {

				CompletableFuture<Void> initFuture = new CompletableFuture<>();
				try {
					RedisConnection connection = connectionFactory.getConnection();
					this.connection = connection;

					if (connection.isSubscribed()) {

						initFuture.completeExceptionally(
								new IllegalStateException("Retrieved connection is already subscribed; aborting listening"));
						return initFuture;
					}

					try {
						eventuallyPerformSubscription(connection, backOffExecution, initFuture, patterns, channels);
					} catch (Throwable t) {
						handleSubscriptionException(initFuture, backOffExecution, t);
					}
				} catch (RuntimeException e) {
					initFuture.completeExceptionally(e);
				}

				return initFuture;
			}
		}

		/**
		 * Performs a potentially asynchronous registration of a subscription.
		 */
		void eventuallyPerformSubscription(RedisConnection connection, BackOffExecution backOffExecution,
				CompletableFuture<Void> subscriptionDone, Collection<byte[]> patterns, Collection<byte[]> channels) {

			addSynchronization(new SynchronizingMessageListener.SubscriptionSynchronization(patterns, channels,
					() -> subscriptionDone.complete(null)));

			doSubscribe(connection, patterns, channels);
		}

		/**
		 * Perform the actual subscription. Can be overridden by subclasses.
		 *
		 * @param connection the connection to use.
		 * @param patterns patterns to subscribe to.
		 * @param channels channels to subscribe to.
		 */
		void doSubscribe(RedisConnection connection, Collection<byte[]> patterns, Collection<byte[]> channels) {

			if (!patterns.isEmpty()) {
				connection.pSubscribe(synchronizingMessageListener, patterns.toArray(new byte[0][]));
			}

			if (!channels.isEmpty()) {
				if (patterns.isEmpty()) {
					connection.subscribe(synchronizingMessageListener, channels.toArray(new byte[0][]));
				} else {
					subscribeChannel(channels.toArray(new byte[0][]));
				}
			}
		}

		void addSynchronization(SynchronizingMessageListener.SubscriptionSynchronization synchronizer) {
			this.synchronizingMessageListener.addSynchronization(synchronizer);
		}

		public void unsubscribeAll() {

			synchronized (localMonitor) {

				RedisConnection connection = this.connection;
				if (connection == null) {
					return;
				}

				doUnsubscribe(connection);
			}
		}

		void doUnsubscribe(RedisConnection connection) {
			closeSubscription(connection);
			closeConnection();

			unsubscribeFuture.complete(null);
		}

		/**
		 * Cancel all subscriptions and close the connection.
		 */
		public void cancel() {

			synchronized (localMonitor) {

				RedisConnection connection = this.connection;
				if (connection == null) {
					return;
				}

				doCancel(connection);
			}
		}

		void doCancel(RedisConnection connection) {
			closeSubscription(connection);
			closeConnection();
		}

		void closeSubscription(RedisConnection connection) {

			if (logger.isTraceEnabled()) {
				logger.trace("Cancelling Redis subscription...");
			}

			Subscription sub = connection.getSubscription();

			if (sub != null) {

				if (logger.isTraceEnabled()) {
					logger.trace("Unsubscribing from all channels");
				}

				try {
					sub.close();
				} catch (Exception e) {
					logger.warn("Unable to unsubscribe from subscriptions", e);
				}
			}
		}

		/**
		 * Close the current Redis connection.
		 */
		public void closeConnection() {

			synchronized (localMonitor) {

				RedisConnection connection = this.connection;
				this.connection = null;

				if (connection != null) {
					logger.trace("Closing connection");
					try {
						connection.close();
					} catch (Exception e) {
						logger.warn("Error closing subscription connection", e);
					}
				}
			}
		}

		/**
		 * Update an existing subscription by subscribing to additional {@code channels}.
		 *
		 * @param channels channels to subscribe to.
		 */
		public void subscribeChannel(byte[]... channels) {
			doWithSubscription(channels, Subscription::subscribe);
		}

		/**
		 * Update an existing subscription by subscribing to additional {@code patterns}.
		 *
		 * @param patterns patterns to subscribe to.
		 */
		public void subscribePattern(byte[]... patterns) {
			doWithSubscription(patterns, Subscription::pSubscribe);
		}

		/**
		 * Update an existing subscription by unsubscribing from {@code channels}.
		 *
		 * @param channels channels to unsubscribe from.
		 */
		public void unsubscribeChannel(byte[]... channels) {
			doWithSubscription(channels, Subscription::unsubscribe);
		}

		/**
		 * Update an existing subscription by unsubscribing from {@code patterns}.
		 *
		 * @param patterns patterns to unsubscribe from.
		 */
		public void unsubscribePattern(byte[]... patterns) {
			doWithSubscription(patterns, Subscription::pUnsubscribe);
		}

		private void doWithSubscription(byte[][] data, BiConsumer<Subscription, byte[][]> function) {

			if (ObjectUtils.isEmpty(data)) {
				return;
			}

			synchronized (localMonitor) {
				RedisConnection connection = this.connection;
				if (connection != null) {
					Subscription sub = connection.getSubscription();
					if (sub != null) {
						function.accept(sub, data);
					}
				}
			}
		}

	}

	/**
	 * Blocking variant of a subscriber for connectors that block within the (p)subscribe method.
	 *
	 * @author Mark Paluch
	 * @since 2.7
	 */
	class BlockingSubscriber extends Subscriber {

		private final Executor executor;

		BlockingSubscriber(RedisConnectionFactory connectionFactory, Executor executor) {
			super(connectionFactory);
			this.executor = executor;
		}

		@Override
		void doUnsubscribe(RedisConnection connection) {
			closeSubscription(connection); // connection will be closed after exiting the doSubscribe method
		}

		@Override
		protected void eventuallyPerformSubscription(RedisConnection connection, BackOffExecution backOffExecution,
				CompletableFuture<Void> subscriptionDone, Collection<byte[]> patterns, Collection<byte[]> channels) {

			Collection<byte[]> initiallySubscribeToChannels;

			if (!patterns.isEmpty() && !channels.isEmpty()) {

				initiallySubscribeToChannels = Collections.emptySet();

				// perform channel subscription later as the first call to (p)subscribe blocks the client
				addSynchronization(
						new SynchronizingMessageListener.SubscriptionSynchronization(patterns, Collections.emptySet(), () -> {
							try {
								subscribeChannel(channels.toArray(new byte[0][]));
							} catch (Exception cause) {
								handleSubscriptionException(subscriptionDone, backOffExecution, cause);
							}
						}));
			} else {
				initiallySubscribeToChannels = channels;
			}

			addSynchronization(new SynchronizingMessageListener.SubscriptionSynchronization(patterns, channels,
					() -> subscriptionDone.complete(null)));

			executor.execute(() -> {

				try {
					doSubscribe(connection, patterns, initiallySubscribeToChannels);
					closeConnection();
					unsubscribeFuture.complete(null);
				} catch (Throwable cause) {
					handleSubscriptionException(subscriptionDone, backOffExecution, cause);
				}
			});
		}
	}
}
