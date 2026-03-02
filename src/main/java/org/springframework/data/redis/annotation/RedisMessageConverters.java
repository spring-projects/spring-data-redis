package org.springframework.data.redis.annotation;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.converter.StringMessageConverter;
import org.springframework.messaging.converter.CompositeMessageConverter;

/**
 * Represents a builder for {@link MessageConverter}s to be used with Redis listeners.
 *
 * @author Ilyass Bougati
 */
public final class RedisMessageConverters {

    private RedisMessageConverters() {
    }

    public interface Builder {

        Builder registerDefaults(boolean registerDefaults);

        Builder withStringConverter(MessageConverter stringMessageConverter);

        Builder withStringConverter(Charset charset);

        Builder addCustomConverter(MessageConverter converter);

        MessageConverter build();
    }

    static class DefaultBuilder implements Builder {

        private boolean registerDefaults = true;
        private MessageConverter stringMessageConverter;
        private final List<MessageConverter> customConverters = new ArrayList<>();

        @Override
        public Builder registerDefaults(boolean registerDefaults) {
            this.registerDefaults = registerDefaults;
            return this;
        }

        @Override
        public Builder withStringConverter(MessageConverter stringMessageConverter) {
            this.stringMessageConverter = stringMessageConverter;
            return this;
        }

        @Override
        public Builder withStringConverter(Charset charset) {
            this.stringMessageConverter = new StringMessageConverter(charset);
            return this;
        }

        @Override
        public Builder addCustomConverter(MessageConverter converter) {
            this.customConverters.add(converter);
            return this;
        }

        @Override
        public MessageConverter build() {
            List<MessageConverter> converters = new ArrayList<>();

            // If string converter isn't explicitly set, default to UTF-8
            if (this.stringMessageConverter == null && this.registerDefaults) {
                this.stringMessageConverter = new StringMessageConverter(StandardCharsets.UTF_8);
            }

            if (this.stringMessageConverter != null) {
                converters.add(this.stringMessageConverter);
            }

            converters.addAll(this.customConverters);

            if (converters.isEmpty()) {
                return new StringMessageConverter(StandardCharsets.UTF_8);
            }

            if (converters.size() == 1) {
                return converters.get(0);
            }

            return new CompositeMessageConverter(converters);
        }
    }

    public static Builder builder() {
        return new DefaultBuilder();
    }
}