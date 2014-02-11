/*
 * Copyright 2014 the original author or authors.
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
package org.springframework.data.redis.connection.srp;

import java.nio.charset.Charset;

import org.hamcrest.core.IsEqual;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.core.convert.converter.Converter;

import redis.reply.BulkReply;
import redis.reply.Reply;

/**
 * @author Christoph Strobl
 * @author Thomas Darimont
 */
public class SrpReplyToTimeAsLongConverterUnitTests {

	@SuppressWarnings("rawtypes") private Converter<Reply[], Long> converter;

	@Before
	public void setUp() {
		this.converter = SrpConverters.repliesToTimeAsLong();
	}

	/**
	 * @see DATAREDIS-206
	 */
	@Test
	public void testConverterShouldCreateMillisecondsCorrectlyWhenGivenValidReplyArray() {

		Reply<?> seconds = new BulkReply("1392183718".getBytes(Charset.forName("UTF-8")));
		Reply<?> microseconds = new BulkReply("555122".getBytes(Charset.forName("UTF-8")));

		Assert.assertThat(converter.convert(new Reply[] { seconds, microseconds }), IsEqual.equalTo(1392183718555L));
	}

	/**
	 * @see DATAREDIS-206
	 */
	@Test(expected = IllegalArgumentException.class)
	public void testConverterShouldThrowExceptionWhenGivenReplyArrayIsNull() {

		converter.convert(null);
	}

	/**
	 * @see DATAREDIS-206
	 */
	@Test(expected = IllegalArgumentException.class)
	public void testConverterShouldThrowExceptionWhenGivenReplyArrayIsEmpty() {

		converter.convert(new Reply[] {});
	}

	/**
	 * @see DATAREDIS-206
	 */
	@Test(expected = IllegalArgumentException.class)
	public void testConverterShouldThrowExceptionWhenGivenReplyArrayHasOnlyOneItem() {

		converter.convert(new Reply[] { new BulkReply(null) });
	}

	/**
	 * @see DATAREDIS-206
	 */
	@Test(expected = IllegalArgumentException.class)
	public void testConverterShouldThrowExceptionWhenGivenReplyArrayMoreThanTwoItems() {

		converter.convert(new Reply[] { new BulkReply(null), new BulkReply(null), new BulkReply(null) });
	}

	/**
	 * @see DATAREDIS-206
	 */
	@Test(expected = NumberFormatException.class)
	public void testConverterShouldThrowExecptionForNonParsableReply() {

		Reply<?> invalidDataBlock = new BulkReply("123-not-a-number".getBytes(Charset.forName("UTF-8")));
		Reply<?> microseconds = new BulkReply("555122".getBytes(Charset.forName("UTF-8")));

		converter.convert(new Reply[] { invalidDataBlock, microseconds });
	}

	/**
	 * @see DATAREDIS-206
	 */
	@Test(expected = IllegalArgumentException.class)
	public void testConverterShouldThrowExecptionForEmptyDataBlocks() {

		Reply<?> invalidDataBlock = new BulkReply(null);
		Reply<?> microseconds = new BulkReply("555122".getBytes(Charset.forName("UTF-8")));

		converter.convert(new Reply[] { invalidDataBlock, microseconds });
	}
}
