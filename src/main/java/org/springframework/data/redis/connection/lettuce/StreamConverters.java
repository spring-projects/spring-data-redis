/*
 * Copyright 2018-2020 the original author or authors.
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
package org.springframework.data.redis.connection.lettuce;

import io.lettuce.core.StreamMessage;
import io.lettuce.core.XClaimArgs;
import io.lettuce.core.XReadArgs;

import java.util.List;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.redis.connection.RedisStreamCommands.XClaimOptions;
import org.springframework.data.redis.connection.convert.ListConverter;
import org.springframework.data.redis.connection.stream.ByteRecord;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.lang.Nullable;

/**
 * Converters for Redis Stream-specific types.
 * <p/>
 * Converters typically convert between value objects/argument objects retaining the actual types of values (i.e. no
 * serialization/deserialization happens here).
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.2
 */
@SuppressWarnings({ "unchecked", "rawtypes" })
class StreamConverters {

	private static final Converter<List<StreamMessage<byte[], byte[]>>, List<RecordId>> MESSAGEs_TO_IDs = new ListConverter<>(
			messageToIdConverter());

	/**
	 * Convert {@link StreamReadOptions} to Lettuce's {@link XReadArgs}.
	 *
	 * @param readOptions must not be {@literal null}.
	 * @return the converted {@link XReadArgs}.
	 */
	static XReadArgs toReadArgs(StreamReadOptions readOptions) {
		return StreamReadOptionsToXReadArgsConverter.INSTANCE.convert(readOptions);
	}

	/**
	 * Convert {@link XClaimOptions} to Lettuce's {@link XClaimArgs}.
	 *
	 * @param options must not be {@literal null}.
	 * @return the converted {@link XClaimArgs}.
	 * @since 2.3
	 */
	static XClaimArgs toXClaimArgs(XClaimOptions options) {
		return XClaimOptionsToXClaimArgsConverter.INSTANCE.convert(options);
	}

	static Converter<StreamMessage<byte[], byte[]>, ByteRecord> byteRecordConverter() {
		return (it) -> StreamRecords.newRecord().in(it.getStream()).withId(it.getId()).ofBytes(it.getBody());
	}

	static Converter<List<StreamMessage<byte[], byte[]>>, List<ByteRecord>> byteRecordListConverter() {
		return new ListConverter<>(byteRecordConverter());
	}

	static Converter<StreamMessage<byte[], byte[]>, RecordId> messageToIdConverter() {
		return (it) -> RecordId.of(it.getId());
	}

	static Converter<List<StreamMessage<byte[], byte[]>>, List<RecordId>> messagesToIds() {
		return MESSAGEs_TO_IDs;
	}

	/**
	 * {@link Converter} to convert {@link StreamReadOptions} to Lettuce's {@link XReadArgs}.
	 */
	enum StreamReadOptionsToXReadArgsConverter implements Converter<StreamReadOptions, XReadArgs> {

		INSTANCE;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.core.convert.converter.Converter#convert(java.lang.Object)
		 */
		@Override
		public XReadArgs convert(StreamReadOptions source) {

			XReadArgs args = new XReadArgs();

			if (source.isNoack()) {
				args.noack(true);
			}

			if (source.getBlock() != null) {
				args.block(source.getBlock());
			}

			if (source.getCount() != null) {
				args.count(source.getCount());
			}
			return args;
		}
	}

	/**
	 * {@link Converter} to convert {@link XClaimOptions} to Lettuce's {@link XClaimArgs}.
	 * 
	 * @since 2.3
	 */
	enum XClaimOptionsToXClaimArgsConverter implements Converter<XClaimOptions, XClaimArgs> {
		INSTANCE;

		@Nullable
		@Override
		public XClaimArgs convert(XClaimOptions source) {

			XClaimArgs args = XClaimArgs.Builder.minIdleTime(source.getMinIdleTime());
			args.minIdleTime(source.getMinIdleTime());
			args.force(source.isForce());

			if (source.getIdleTime() != null) {
				args.idle(source.getIdleTime());
			}
			if (source.getRetryCount() != null) {
				args.retryCount(source.getRetryCount());
			}
			if (source.getUnixTime() != null) {
				args.time(source.getUnixTime());
			}

			return args;

		}
	}
}
