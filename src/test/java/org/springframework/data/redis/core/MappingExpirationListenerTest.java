package org.springframework.data.redis.core;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.core.convert.ConversionService;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.core.convert.RedisConverter;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
/**
 * @author Lucian Torje
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class MappingExpirationListenerTest {

    @Mock
    private RedisOperations<?, ?> redisOperations;
    @Mock
    private RedisConverter redisConverter;
    @Mock
    private RedisMessageListenerContainer listenerContainer;
    @Mock
    private Message message;
    @Mock
    private RedisKeyExpiredEvent<?> event;
    @Mock
    private ConversionService conversionService;

    private RedisKeyValueAdapter.MappingExpirationListener listener;

    @Test
    void testOnNonKeyExpiration() {
        byte[] key = "testKey".getBytes();
        when(message.getBody()).thenReturn(key);
        listener = new RedisKeyValueAdapter.MappingExpirationListener(listenerContainer, redisOperations, redisConverter, RedisKeyValueAdapter.ShadowCopy.ON);

        listener.onMessage(message, null);

        verify(redisOperations, times(0)).execute(any(RedisCallback.class));
    }

    @Test
    void testOnValidKeyExpiration() {
        List<Object> eventList = new ArrayList<>();

        byte[] key = "abc:testKey".getBytes();
        when(message.getBody()).thenReturn(key);

        listener = new RedisKeyValueAdapter.MappingExpirationListener(listenerContainer, redisOperations, redisConverter, RedisKeyValueAdapter.ShadowCopy.OFF);
        listener.setApplicationEventPublisher(eventList::add);
        listener.onMessage(message, null);

        verify(redisOperations, times(1)).execute(any(RedisCallback.class));
        assertThat(eventList).hasSize(1);
        assertThat(eventList.get(0)).isInstanceOf(RedisKeyExpiredEvent.class);
        assertThat(((RedisKeyExpiredEvent) (eventList.get(0))).getKeyspace()).isEqualTo("abc");
        assertThat(((RedisKeyExpiredEvent) (eventList.get(0))).getId()).isEqualTo("testKey".getBytes());
    }
}