/*
 * Copyright 2026-present the original author or authors.
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
package org.springframework.data.redis.annotation;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.data.redis.config.BatchMethodRedisListenerEndpoint;
import org.springframework.data.redis.config.MethodRedisListenerEndpoint;
import org.springframework.data.redis.config.RedisListenerConfigUtils;
import org.springframework.data.redis.config.RedisListenerEndpoint;
import org.springframework.data.redis.config.RedisListenerEndpointRegistry;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.SubscriptionListener;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.listener.StringMessage;
import org.springframework.data.redis.listener.Topic;
import org.springframework.data.redis.listener.adapter.HandlerMethodMessageListenerAdapter;
import org.springframework.data.redis.listener.support.PubSubHeaders;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Headers;

/**
 * Unit tests for {@link RedisListenerAnnotationBeanPostProcessor}.
 *
 * @author Ilyass Bougati
 * @author Mark Paluch
 */
@MockitoSettings(strictness = Strictness.LENIENT)
class RedisListenerAnnotationBeanPostProcessorUnitTests {

	@Mock RedisListenerEndpointRegistry endpointRegistry;
	@Mock BeanFactory beanFactory;
	@Mock RedisMessageListenerContainer container;

	private RedisListenerAnnotationBeanPostProcessor processor;

	@BeforeEach
	void setUp() {

		processor = new RedisListenerAnnotationBeanPostProcessor();
		init(processor);

		when(beanFactory.getBean(RedisListenerConfigUtils.REDIS_MESSAGE_LISTENER_BEAN_NAME,
				RedisMessageListenerContainer.class)).thenReturn(container);
	}

	@Test // GH-1004
	void shouldRegisterEndpoint() throws NoSuchMethodException {

		AnnotatedService bean = new AnnotatedService();

		Object result = processor.postProcessAfterInitialization(bean, "annotatedService");

		processor.afterSingletonsInstantiated();

		ArgumentCaptor<MethodRedisListenerEndpoint> endpointCaptor = ArgumentCaptor
				.forClass(MethodRedisListenerEndpoint.class);

		assertThat(result).isSameAs(bean);

		verify(endpointRegistry).registerListener(endpointCaptor.capture(), eq(container));

		MethodRedisListenerEndpoint registeredEndpoint = endpointCaptor.getValue();
		assertThat(registeredEndpoint.getBean()).isEqualTo(bean);
		assertThat(registeredEndpoint.getMethod()).isEqualTo(AnnotatedService.class.getMethod("handle", String.class));
	}

	@Test // GH-1004
	void shouldNotRegisterWithoutAnnotation() {

		PlainService bean = new PlainService();

		Object result = processor.postProcessAfterInitialization(bean, "plainService");

		assertThat(result).isSameAs(bean);
		verifyNoInteractions(endpointRegistry);
		verifyNoInteractions(beanFactory);
	}

	@Test // GH-1004
	void shouldInjectPayload() throws NoSuchMethodException {

		WithArgumentResolution bean = mock(WithArgumentResolution.class);
		Method method = WithArgumentResolution.class.getMethod("handle", String.class, Topic.class);

		MethodRedisListenerEndpoint endpoint = processor.createEndpoint(method.getAnnotation(RedisListener.class),
				method, bean);

		HandlerMethodMessageListenerAdapter listener = endpoint.createListener();

		listener.onMessage(new StringMessage("test-channel", "hello"), null);

		verify(bean).handle("hello", ChannelTopic.of("test-channel"));
	}


	@Test // GH-1004
	void shouldInjectConvertedPayload() throws NoSuchMethodException {

		WithArgumentResolution bean = mock(WithArgumentResolution.class);
		Method method = WithArgumentResolution.class.getMethod("handle", String.class, String.class);

		MethodRedisListenerEndpoint endpoint = processor.createEndpoint(method.getAnnotation(RedisListener.class),
				method, bean);

		HandlerMethodMessageListenerAdapter listener = endpoint.createListener();

		listener.onMessage(new StringMessage("test-channel", "hello"), null);

		verify(bean).handle("hello", "test-channel");
	}

	@Test // GH-1004
	void shouldInjectHeaders() throws NoSuchMethodException {

		WithArgumentResolution bean = mock(WithArgumentResolution.class);
		Method method = WithArgumentResolution.class.getMethod("handleHeaders", String.class, Map.class);

		MethodRedisListenerEndpoint endpoint = processor.createEndpoint(method.getAnnotation(RedisListener.class),
				method, bean);

		HandlerMethodMessageListenerAdapter listener = endpoint.createListener();

		listener.onMessage(new StringMessage("test-channel", "hello"), null);

		ArgumentCaptor<Map<String, Object>> headersCaptor = ArgumentCaptor.forClass(Map.class);

		verify(bean).handleHeaders(eq("hello"), headersCaptor.capture());
		Map<String, Object> headers = headersCaptor.getValue();

		assertThat(headers).containsEntry(PubSubHeaders.TOPIC, ChannelTopic.of("test-channel"))
				.containsEntry(PubSubHeaders.CHANNEL, ChannelTopic.of("test-channel")) //
				.doesNotContainKey(PubSubHeaders.PATTERN);
	}

	@Test // GH-3439
	void shouldDelegateSubscriptionCallbacksWhenBeanImplementsSubscriptionListener() throws NoSuchMethodException {

		SubscriptionAwareService bean = mock(SubscriptionAwareService.class);
		Method method = SubscriptionAwareService.class.getMethod("handle", String.class);

		MethodRedisListenerEndpoint endpoint = processor.createEndpoint(method.getAnnotation(RedisListener.class),
				method, bean);

		HandlerMethodMessageListenerAdapter listener = endpoint.createListener();

		assertThat(listener).isInstanceOf(SubscriptionListener.class);

		SubscriptionListener subscriptionListener = (SubscriptionListener) listener;
		subscriptionListener.onChannelSubscribed("test-channel".getBytes(), 1);
		subscriptionListener.onChannelUnsubscribed("test-channel".getBytes(), 0);
		subscriptionListener.onPatternSubscribed("test-*".getBytes(), 1);
		subscriptionListener.onPatternUnsubscribed("test-*".getBytes(), 0);

		verify(bean).onChannelSubscribed("test-channel".getBytes(), 1);
		verify(bean).onChannelUnsubscribed("test-channel".getBytes(), 0);
		verify(bean).onPatternSubscribed("test-*".getBytes(), 1);
		verify(bean).onPatternUnsubscribed("test-*".getBytes(), 0);
	}

	@Test // GH-3439
	void shouldNotImplementSubscriptionListenerWhenBeanDoesNot() throws NoSuchMethodException {

		AnnotatedService bean = new AnnotatedService();
		Method method = AnnotatedService.class.getMethod("handle", String.class);

		MethodRedisListenerEndpoint endpoint = processor.createEndpoint(method.getAnnotation(RedisListener.class),
				method, bean);

		assertThat(endpoint.createListener()).isNotInstanceOf(SubscriptionListener.class);
	}

	@Test // GH-3439
	void shouldRejectNullListenerGrouping() {
		assertThatIllegalArgumentException().isThrownBy(() -> processor.setListenerGrouping(null));
	}

	@Test // GH-3439
	void shouldGroupPerTopic() {

		List<RedisListenerEndpoint> endpoints = registerGrouped(new GroupedService(), 2);

		assertThat(endpoints).allSatisfy(endpoint -> assertThat(endpoint).isInstanceOf(BatchMethodRedisListenerEndpoint.class));

		List<String> topics = endpoints.stream().map(endpoint -> ((BatchMethodRedisListenerEndpoint) endpoint).getTopic())
				.toList();
		assertThat(topics).containsExactlyInAnyOrder("ch1", "ch2");

		List<String> ids = endpoints.stream().map(RedisListenerEndpoint::getId).toList();
		assertThat(ids).allSatisfy(id -> assertThat(id).isNotEmpty());
		assertThat(ids).doesNotHaveDuplicates();
	}

	@Test // GH-3439
	void shouldNotGroupSingleAnnotation() {
		assertThat(registerGrouped(new AnnotatedService(), 1)).singleElement()
				.isExactlyInstanceOf(MethodRedisListenerEndpoint.class);
	}

	@Test // GH-3439
	void shouldSeparateChannelAndPattern() {
		assertThat(registerGrouped(new ChannelAndPatternService(), 2))
				.allSatisfy(endpoint -> assertThat(endpoint).isExactlyInstanceOf(MethodRedisListenerEndpoint.class));
	}

	@Test // GH-3439
	void shouldGroupAnnotationsWithCustomId() {

		// grouped endpoints get a generated id, custom ids are dropped
		assertThat(registerGrouped(new CustomIdService(), 1)).singleElement().satisfies(endpoint -> {
			assertThat(endpoint).isInstanceOf(BatchMethodRedisListenerEndpoint.class);
			assertThat(endpoint.getId()).isNotEmpty().isNotIn("first", "second");
		});
	}

	@Test // GH-3439
	void shouldKeepConsumesPerMethodWhenGrouping() {

		ConsumesService bean = new ConsumesService();
		BatchMethodRedisListenerEndpoint batch = (BatchMethodRedisListenerEndpoint) registerGrouped(bean, 1).get(0);

		batch.register(container);
		batch.start();

		ArgumentCaptor<MessageListener> listenerCaptor = ArgumentCaptor.forClass(MessageListener.class);
		verify(container).addMessageListener(listenerCaptor.capture(), any(Topic.class));
		listenerCaptor.getValue().onMessage(new StringMessage("ch1", "hi"), null);

		assertThat(bean.contentTypes).containsExactlyInAnyOrder("text/plain", "application/json");
	}

	@Test // GH-3439
	void shouldSeparateContainers() {

		processor.setListenerGrouping(ListenerGrouping.PER_BEAN_AND_TOPIC);

		RedisMessageListenerContainer container1 = mock(RedisMessageListenerContainer.class);
		RedisMessageListenerContainer container2 = mock(RedisMessageListenerContainer.class);
		when(beanFactory.getBean("c1", RedisMessageListenerContainer.class)).thenReturn(container1);
		when(beanFactory.getBean("c2", RedisMessageListenerContainer.class)).thenReturn(container2);

		TwoContainerService bean = new TwoContainerService();
		processor.postProcessAfterInitialization(bean, "twoContainerService");
		processor.afterSingletonsInstantiated();

		verify(endpointRegistry).registerListener(any(), eq(container1));
		verify(endpointRegistry).registerListener(any(), eq(container2));
	}

	@Test // GH-3439
	void shouldGroupSubclassEndpoints() {

		processor = new RedisListenerAnnotationBeanPostProcessor() {

			@Override
			public MethodRedisListenerEndpoint createEndpoint(RedisListener redisListener, Method method, Object bean) {

				MethodRedisListenerEndpoint endpoint = new CustomMethodRedisListenerEndpoint(bean, method);
				endpoint.setId(method.getName());
				endpoint.setTopic(redisListener.topic());
				return endpoint;
			}
		};
		init(processor);

		assertThat(registerGrouped(new CustomIdService(), 1)).singleElement()
				.isInstanceOf(BatchMethodRedisListenerEndpoint.class);
	}

	@Test // GH-3439
	void shouldCallProcessRedisListenerWhenGrouping() {

		List<RedisListener> processed = new CopyOnWriteArrayList<>();
		processor = new RedisListenerAnnotationBeanPostProcessor() {

			@Override
			protected void processRedisListener(RedisListener redisListener, Method method, Object bean) {
				processed.add(redisListener);
				super.processRedisListener(redisListener, method, bean);
			}
		};
		init(processor);

		List<RedisListenerEndpoint> endpoints = registerGrouped(new GroupedService(), 2);

		assertThat(processed).hasSize(4);
		assertThat(endpoints).allSatisfy(endpoint -> assertThat(endpoint).isInstanceOf(BatchMethodRedisListenerEndpoint.class));
	}

	@Test // GH-3439
	void shouldKeepDefaultBehavior() {

		processor.postProcessAfterInitialization(new GroupedService(), "groupedService");
		processor.afterSingletonsInstantiated();

		assertThat(registeredEndpoints(4))
				.allSatisfy(endpoint -> assertThat(endpoint).isExactlyInstanceOf(MethodRedisListenerEndpoint.class));
	}

	private void init(RedisListenerAnnotationBeanPostProcessor processor) {

		processor.setEndpointRegistry(endpointRegistry);
		processor.afterSingletonsInstantiated();
		processor.setBeanFactory(beanFactory);
	}

	// processes the bean with PER_BEAN_AND_TOPIC grouping and returns the expected number of registered endpoints
	private List<RedisListenerEndpoint> registerGrouped(Object bean, int count) {

		processor.setListenerGrouping(ListenerGrouping.PER_BEAN_AND_TOPIC);
		processor.postProcessAfterInitialization(bean, "bean");
		processor.afterSingletonsInstantiated();

		return registeredEndpoints(count);
	}

	private List<RedisListenerEndpoint> registeredEndpoints(int count) {

		ArgumentCaptor<RedisListenerEndpoint> endpointCaptor = ArgumentCaptor.forClass(RedisListenerEndpoint.class);
		verify(endpointRegistry, times(count)).registerListener(endpointCaptor.capture(), eq(container));
		return endpointCaptor.getAllValues();
	}

	static class AnnotatedService {

		@RedisListener(topic = "test-channel")
		public void handle(String message) {}

	}

	static class GroupedService {

		@RedisListener(topic = "ch1")
		@RedisListener(topic = "ch2")
		public void a(String message) {}

		@RedisListener(topic = "ch1")
		@RedisListener(topic = "ch2")
		public void b(String message) {}

	}

	static class ChannelAndPatternService {

		@RedisListener(topic = "news.1")
		public void a(String message) {}

		@RedisListener(topic = "news.*")
		public void b(String message) {}

	}

	static class TwoContainerService {

		@RedisListener(container = "c1", topic = "ch1")
		public void a(String message) {}

		@RedisListener(container = "c2", topic = "ch1")
		public void b(String message) {}

	}

	static class CustomIdService {

		@RedisListener(id = "first", topic = "ch1")
		public void a(String message) {}

		@RedisListener(id = "second", topic = "ch1")
		public void b(String message) {}

	}

	static class ConsumesService {

		final List<String> contentTypes = new CopyOnWriteArrayList<>();

		@RedisListener(topic = "ch1", consumes = "text/plain")
		public void a(@Header(MessageHeaders.CONTENT_TYPE) String contentType) {
			contentTypes.add(contentType);
		}

		@RedisListener(topic = "ch1", consumes = "application/json")
		public void b(@Header(MessageHeaders.CONTENT_TYPE) String contentType) {
			contentTypes.add(contentType);
		}

	}

	/**
	 * Stand-in for a hand-rolled {@link MethodRedisListenerEndpoint} subclass, e.g. one carrying its own
	 * {@code SubscriptionListener} adapter. Grouping must skip these.
	 */
	static class CustomMethodRedisListenerEndpoint extends MethodRedisListenerEndpoint {

		CustomMethodRedisListenerEndpoint(Object bean, Method method) {
			super(bean, method);
		}

	}

	static class SubscriptionAwareService implements SubscriptionListener {

		@RedisListener(topic = "test-channel")
		public void handle(String message) {}

	}

	static class WithArgumentResolution {

		@RedisListener(topic = "test-channel")
		public void handle(String message, @Header Topic topic) {}

		@RedisListener(topic = "test-channel")
		public void handle(String message, @Header String topic) {}

		@RedisListener(topic = "test-channel")
		public void handleHeaders(String message, @Headers Map<String, Object> headers) {}

	}

	static class PlainService {

		public void doSomething() {}

	}

}
