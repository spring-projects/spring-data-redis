/*
 * Copyright 2016 the original author or authors.
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
package org.springframework.data.redis.connection.lettuce;

import static org.hamcrest.collection.IsIterableContainingInAnyOrder.*;
import static org.hamcrest.collection.IsIterableContainingInOrder.*;
import static org.hamcrest.core.AnyOf.*;
import static org.hamcrest.core.Is.*;
import static org.hamcrest.core.IsEqual.*;
import static org.hamcrest.core.IsNot.*;
import static org.hamcrest.core.IsNull.*;
import static org.junit.Assert.*;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

public class LettuceReactiveSetCommandsTests extends LettuceReactiveCommandsTestsBase {

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void sAddShouldAddSingleValue() {
		assertThat(connection.setCommands().sAdd(KEY_1_BBUFFER, VALUE_1_BBUFFER).block(), is(1L));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void sAddShouldAddValues() {
		assertThat(connection.setCommands().sAdd(KEY_1_BBUFFER, Arrays.asList(VALUE_1_BBUFFER, VALUE_2_BBUFFER)).block(),
				is(2L));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void sRemShouldRemoveSingleValue() {

		nativeCommands.sadd(KEY_1, VALUE_1, VALUE_2, VALUE_3);

		assertThat(connection.setCommands().sRem(KEY_1_BBUFFER, VALUE_1_BBUFFER).block(), is(1L));
		assertThat(nativeCommands.sismember(KEY_1, VALUE_1), is(false));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void sRemShouldRemoveValues() {

		nativeCommands.sadd(KEY_1, VALUE_1, VALUE_2, VALUE_3);

		assertThat(connection.setCommands().sRem(KEY_1_BBUFFER, Arrays.asList(VALUE_1_BBUFFER, VALUE_2_BBUFFER)).block(),
				is(2L));
		assertThat(nativeCommands.sismember(KEY_1, VALUE_1), is(false));
		assertThat(nativeCommands.sismember(KEY_1, VALUE_2), is(false));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void sPopShouldRetrieveRandomValue() {

		nativeCommands.sadd(KEY_1, VALUE_1, VALUE_2, VALUE_3);

		assertThat(connection.setCommands().sPop(KEY_1_BBUFFER).block(), is(notNullValue()));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void sPopShouldReturnNullWhenNotPresent() {
		assertThat(connection.setCommands().sPop(KEY_1_BBUFFER).block(), is(nullValue()));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void sMoveShouldMoveValueCorrectly() {

		nativeCommands.sadd(KEY_1, VALUE_1, VALUE_2, VALUE_3);
		nativeCommands.sadd(KEY_2, VALUE_1);

		assertThat(connection.setCommands().sMove(KEY_1_BBUFFER, KEY_2_BBUFFER, VALUE_3_BBUFFER).block(), is(true));
		assertThat(nativeCommands.sismember(KEY_2, VALUE_3), is(true));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void sMoveShouldReturnFalseIfValueIsNotAMember() {

		nativeCommands.sadd(KEY_1, VALUE_1, VALUE_2);
		nativeCommands.sadd(KEY_2, VALUE_1);

 		assertThat(connection.setCommands().sMove(KEY_1_BBUFFER, KEY_2_BBUFFER, VALUE_3_BBUFFER).block(), is(false));
		assertThat(nativeCommands.sismember(KEY_2, VALUE_3), is(false));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void sMoveShouldReturnOperateCorrectlyWhenValueAlreadyPresentInTarget() {

		nativeCommands.sadd(KEY_1, VALUE_1, VALUE_2, VALUE_3);
		nativeCommands.sadd(KEY_2, VALUE_1, VALUE_3);

		assertThat(connection.setCommands().sMove(KEY_1_BBUFFER, KEY_2_BBUFFER, VALUE_3_BBUFFER).block(), is(true));
		assertThat(nativeCommands.sismember(KEY_1, VALUE_3), is(false));
		assertThat(nativeCommands.sismember(KEY_2, VALUE_3), is(true));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void sCardShouldCountValuesCorrectly() {

		nativeCommands.sadd(KEY_1, VALUE_1, VALUE_2, VALUE_3);

		assertThat(connection.setCommands().sCard(KEY_1_BBUFFER).block(), is(3L));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void sIsMemberShouldReturnTrueWhenValueContainedInKey() {

		nativeCommands.sadd(KEY_1, VALUE_1, VALUE_2);

		assertThat(connection.setCommands().sIsMember(KEY_1_BBUFFER, VALUE_1_BBUFFER).block(), is(true));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void sIsMemberShouldReturnFalseWhenValueNotContainedInKey() {

		nativeCommands.sadd(KEY_1, VALUE_1, VALUE_2);

		assertThat(connection.setCommands().sIsMember(KEY_1_BBUFFER, VALUE_3_BBUFFER).block(), is(false));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void sInterShouldIntersectSetsCorrectly() {

		nativeCommands.sadd(KEY_1, VALUE_1, VALUE_2);
		nativeCommands.sadd(KEY_2, VALUE_2, VALUE_3);

		List<ByteBuffer> result = connection.setCommands().sInter(Arrays.asList(KEY_1_BBUFFER, KEY_2_BBUFFER)).block();
		assertThat(result, contains(VALUE_2_BBUFFER));
		assertThat(result, not(containsInAnyOrder(VALUE_1_BBUFFER, VALUE_3_BBUFFER)));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void sInterStoreShouldReturnSizeCorrectly() {

		nativeCommands.sadd(KEY_1, VALUE_1, VALUE_2);
		nativeCommands.sadd(KEY_2, VALUE_2, VALUE_3);

		assertThat(connection.setCommands().sInterStore(KEY_3_BBUFFER, Arrays.asList(KEY_1_BBUFFER, KEY_2_BBUFFER)).block(),
				is(1L));
		assertThat(nativeCommands.sismember(KEY_3, VALUE_2), is(true));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void sUnionShouldCombineSetsCorrectly() {

		nativeCommands.sadd(KEY_1, VALUE_1, VALUE_2);
		nativeCommands.sadd(KEY_2, VALUE_2, VALUE_3);

		List<ByteBuffer> result = connection.setCommands().sUnion(Arrays.asList(KEY_1_BBUFFER, KEY_2_BBUFFER)).block();
		assertThat(result, containsInAnyOrder(VALUE_1_BBUFFER, VALUE_3_BBUFFER, VALUE_2_BBUFFER));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void sUnionStoreShouldReturnSizeCorrectly() {

		nativeCommands.sadd(KEY_1, VALUE_1, VALUE_2);
		nativeCommands.sadd(KEY_2, VALUE_2, VALUE_3);

		assertThat(connection.setCommands().sUnionStore(KEY_3_BBUFFER, Arrays.asList(KEY_1_BBUFFER, KEY_2_BBUFFER)).block(),
				is(3L));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void sDiffShouldBeExcecutedCorrectly() {

		nativeCommands.sadd(KEY_1, VALUE_1, VALUE_2);
		nativeCommands.sadd(KEY_2, VALUE_2, VALUE_3);

		List<ByteBuffer> result = connection.setCommands().sDiff(Arrays.asList(KEY_1_BBUFFER, KEY_2_BBUFFER)).block();
		assertThat(result, containsInAnyOrder(VALUE_1_BBUFFER));
		assertThat(result, not(containsInAnyOrder(VALUE_2_BBUFFER, VALUE_3_BBUFFER)));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void sDiffStoreShouldBeExcecutedCorrectly() {

		nativeCommands.sadd(KEY_1, VALUE_1, VALUE_2);
		nativeCommands.sadd(KEY_2, VALUE_2, VALUE_3);

		assertThat(connection.setCommands().sDiffStore(KEY_3_BBUFFER, Arrays.asList(KEY_1_BBUFFER, KEY_2_BBUFFER)).block(),
				is(1L));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void sMembersReadsValuesFromSetCorrectly() {

		nativeCommands.sadd(KEY_1, VALUE_1, VALUE_2, VALUE_3);

		assertThat(connection.setCommands().sMembers(KEY_1_BBUFFER).block(),
				containsInAnyOrder(VALUE_1_BBUFFER, VALUE_2_BBUFFER, VALUE_3_BBUFFER));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void sRandMemberReturnsRandomMember() {

		nativeCommands.sadd(KEY_1, VALUE_1, VALUE_2, VALUE_3);

		assertThat(connection.setCommands().sRandMember(KEY_1_BBUFFER).block(),
				anyOf(equalTo(VALUE_1_BBUFFER), equalTo(VALUE_2_BBUFFER), equalTo(VALUE_3_BBUFFER)));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void sRandMemberReturnsRandomMembers() {

		nativeCommands.sadd(KEY_1, VALUE_1, VALUE_2, VALUE_3);

		assertThat(connection.setCommands().sRandMember(KEY_1_BBUFFER, 2L).block().size(), is(2));
	}

}
