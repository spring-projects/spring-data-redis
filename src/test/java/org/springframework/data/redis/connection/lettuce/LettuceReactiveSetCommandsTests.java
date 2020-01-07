/*
 * Copyright 2016-2020 the original author or authors.
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

import static org.assertj.core.api.Assertions.*;

import reactor.test.StepVerifier;

import java.util.Arrays;

import org.junit.Test;

import org.springframework.data.redis.core.ScanOptions;

/**
 * Integration tests for {@link LettuceReactiveSetCommands}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public class LettuceReactiveSetCommandsTests extends LettuceReactiveCommandsTestsBase {

	@Test // DATAREDIS-525
	public void sAddShouldAddSingleValue() {
		assertThat(connection.setCommands().sAdd(KEY_1_BBUFFER, VALUE_1_BBUFFER).block()).isEqualTo(1L);
	}

	@Test // DATAREDIS-525
	public void sAddShouldAddValues() {
		assertThat(connection.setCommands().sAdd(KEY_1_BBUFFER, Arrays.asList(VALUE_1_BBUFFER, VALUE_2_BBUFFER)).block())
				.isEqualTo(2L);
	}

	@Test // DATAREDIS-525
	public void sRemShouldRemoveSingleValue() {

		nativeCommands.sadd(KEY_1, VALUE_1, VALUE_2, VALUE_3);

		assertThat(connection.setCommands().sRem(KEY_1_BBUFFER, VALUE_1_BBUFFER).block()).isEqualTo(1L);
		assertThat(nativeCommands.sismember(KEY_1, VALUE_1)).isFalse();
	}

	@Test // DATAREDIS-525
	public void sRemShouldRemoveValues() {

		nativeCommands.sadd(KEY_1, VALUE_1, VALUE_2, VALUE_3);

		assertThat(connection.setCommands().sRem(KEY_1_BBUFFER, Arrays.asList(VALUE_1_BBUFFER, VALUE_2_BBUFFER)).block())
				.isEqualTo(2L);
		assertThat(nativeCommands.sismember(KEY_1, VALUE_1)).isFalse();
		assertThat(nativeCommands.sismember(KEY_1, VALUE_2)).isFalse();
	}

	@Test // DATAREDIS-525
	public void sPopShouldRetrieveRandomValue() {

		nativeCommands.sadd(KEY_1, VALUE_1, VALUE_2, VALUE_3);

		assertThat(connection.setCommands().sPop(KEY_1_BBUFFER).block()).isNotNull();
	}

	@Test // DATAREDIS-668
	public void sPopCountShouldRetrieveValues() {

		nativeCommands.sadd(KEY_1, VALUE_1, VALUE_2, VALUE_3);

		StepVerifier.create(connection.setCommands().sPop(KEY_1_BBUFFER, 2)).expectNextCount(2).verifyComplete();
	}

	@Test // DATAREDIS-525
	public void sPopShouldReturnNullWhenNotPresent() {
		assertThat(connection.setCommands().sPop(KEY_1_BBUFFER).block()).isNull();
	}

	@Test // DATAREDIS-525
	public void sMoveShouldMoveValueCorrectly() {

		nativeCommands.sadd(KEY_1, VALUE_1, VALUE_2, VALUE_3);
		nativeCommands.sadd(KEY_2, VALUE_1);

		assertThat(connection.setCommands().sMove(KEY_1_BBUFFER, KEY_2_BBUFFER, VALUE_3_BBUFFER).block()).isTrue();
		assertThat(nativeCommands.sismember(KEY_2, VALUE_3)).isTrue();
	}

	@Test // DATAREDIS-525
	public void sMoveShouldReturnFalseIfValueIsNotAMember() {

		nativeCommands.sadd(KEY_1, VALUE_1, VALUE_2);
		nativeCommands.sadd(KEY_2, VALUE_1);

		assertThat(connection.setCommands().sMove(KEY_1_BBUFFER, KEY_2_BBUFFER, VALUE_3_BBUFFER).block()).isFalse();
		assertThat(nativeCommands.sismember(KEY_2, VALUE_3)).isFalse();
	}

	@Test // DATAREDIS-525
	public void sMoveShouldReturnOperateCorrectlyWhenValueAlreadyPresentInTarget() {

		nativeCommands.sadd(KEY_1, VALUE_1, VALUE_2, VALUE_3);
		nativeCommands.sadd(KEY_2, VALUE_1, VALUE_3);

		assertThat(connection.setCommands().sMove(KEY_1_BBUFFER, KEY_2_BBUFFER, VALUE_3_BBUFFER).block()).isTrue();
		assertThat(nativeCommands.sismember(KEY_1, VALUE_3)).isFalse();
		assertThat(nativeCommands.sismember(KEY_2, VALUE_3)).isTrue();
	}

	@Test // DATAREDIS-525
	public void sCardShouldCountValuesCorrectly() {

		nativeCommands.sadd(KEY_1, VALUE_1, VALUE_2, VALUE_3);

		assertThat(connection.setCommands().sCard(KEY_1_BBUFFER).block()).isEqualTo(3L);
	}

	@Test // DATAREDIS-525
	public void sIsMemberShouldReturnTrueWhenValueContainedInKey() {

		nativeCommands.sadd(KEY_1, VALUE_1, VALUE_2);

		assertThat(connection.setCommands().sIsMember(KEY_1_BBUFFER, VALUE_1_BBUFFER).block()).isTrue();
	}

	@Test // DATAREDIS-525
	public void sIsMemberShouldReturnFalseWhenValueNotContainedInKey() {

		nativeCommands.sadd(KEY_1, VALUE_1, VALUE_2);

		assertThat(connection.setCommands().sIsMember(KEY_1_BBUFFER, VALUE_3_BBUFFER).block()).isFalse();
	}

	@Test // DATAREDIS-525, DATAREDIS-647
	public void sInterShouldIntersectSetsCorrectly() {

		nativeCommands.sadd(KEY_1, VALUE_1, VALUE_2);
		nativeCommands.sadd(KEY_2, VALUE_2, VALUE_3);
		nativeCommands.sadd(KEY_3, VALUE_1, VALUE_3);

		StepVerifier.create(connection.setCommands().sInter(Arrays.asList(KEY_1_BBUFFER, KEY_2_BBUFFER))) //
				.expectNext(VALUE_2_BBUFFER) //
				.verifyComplete();

		StepVerifier.create(connection.setCommands().sInter(Arrays.asList(KEY_1_BBUFFER, KEY_2_BBUFFER, KEY_3_BBUFFER))) //
				.verifyComplete();
	}

	@Test // DATAREDIS-525
	public void sInterStoreShouldReturnSizeCorrectly() {

		nativeCommands.sadd(KEY_1, VALUE_1, VALUE_2);
		nativeCommands.sadd(KEY_2, VALUE_2, VALUE_3);

		assertThat(connection.setCommands().sInterStore(KEY_3_BBUFFER, Arrays.asList(KEY_1_BBUFFER, KEY_2_BBUFFER)).block())
				.isEqualTo(1L);
		assertThat(nativeCommands.sismember(KEY_3, VALUE_2)).isTrue();
	}

	@Test // DATAREDIS-525
	public void sUnionShouldCombineSetsCorrectly() {

		nativeCommands.sadd(KEY_1, VALUE_1, VALUE_2);
		nativeCommands.sadd(KEY_2, VALUE_2, VALUE_3);

		StepVerifier.create(connection.setCommands().sUnion(Arrays.asList(KEY_1_BBUFFER, KEY_2_BBUFFER))) //
				.expectNextCount(3) //
				.expectComplete();
	}

	@Test // DATAREDIS-525
	public void sUnionStoreShouldReturnSizeCorrectly() {

		nativeCommands.sadd(KEY_1, VALUE_1, VALUE_2);
		nativeCommands.sadd(KEY_2, VALUE_2, VALUE_3);

		assertThat(connection.setCommands().sUnionStore(KEY_3_BBUFFER, Arrays.asList(KEY_1_BBUFFER, KEY_2_BBUFFER)).block())
				.isEqualTo(3L);
	}

	@Test // DATAREDIS-525, DATAREDIS-647
	public void sDiffShouldBeExcecutedCorrectly() {

		nativeCommands.sadd(KEY_1, VALUE_1, VALUE_2);
		nativeCommands.sadd(KEY_2, VALUE_2, VALUE_3);
		nativeCommands.sadd(KEY_3, VALUE_2, VALUE_1);

		StepVerifier.create(connection.setCommands().sDiff(Arrays.asList(KEY_1_BBUFFER, KEY_2_BBUFFER))) //
				.expectNext(VALUE_1_BBUFFER) //
				.verifyComplete();

		StepVerifier.create(connection.setCommands().sDiff(Arrays.asList(KEY_1_BBUFFER, KEY_2_BBUFFER, KEY_3_BBUFFER))) //
				.verifyComplete();
	}

	@Test // DATAREDIS-525
	public void sDiffStoreShouldBeExcecutedCorrectly() {

		nativeCommands.sadd(KEY_1, VALUE_1, VALUE_2);
		nativeCommands.sadd(KEY_2, VALUE_2, VALUE_3);

		assertThat(connection.setCommands().sDiffStore(KEY_3_BBUFFER, Arrays.asList(KEY_1_BBUFFER, KEY_2_BBUFFER)).block())
				.isEqualTo(1L);
	}

	@Test // DATAREDIS-525
	public void sMembersReadsValuesFromSetCorrectly() {

		nativeCommands.sadd(KEY_1, VALUE_1, VALUE_2, VALUE_3);

		StepVerifier.create(connection.setCommands().sMembers(KEY_1_BBUFFER).buffer(3)) //
				.consumeNextWith(list -> assertThat(list).contains(VALUE_1_BBUFFER, VALUE_2_BBUFFER, VALUE_3_BBUFFER)) //
				.verifyComplete();
	}

	@Test // DATAREDIS-743
	public void sScanShouldIterateOverSet() {

		nativeCommands.sadd(KEY_1, VALUE_1, VALUE_2, VALUE_3);

		StepVerifier.create(connection.setCommands().sScan(KEY_1_BBUFFER)) //
				.expectNextCount(3) //
				.verifyComplete();

		StepVerifier
				.create(connection.setCommands().sScan(KEY_1_BBUFFER, ScanOptions.scanOptions().match("value-3").build())) //
				.expectNext(VALUE_3_BBUFFER) //
				.verifyComplete();
	}

	@Test // DATAREDIS-525
	public void sRandMemberReturnsRandomMember() {

		nativeCommands.sadd(KEY_1, VALUE_1, VALUE_2, VALUE_3);

		assertThat(connection.setCommands().sRandMember(KEY_1_BBUFFER).block()).isIn(VALUE_1_BBUFFER, VALUE_2_BBUFFER,
				VALUE_3_BBUFFER);
	}

	@Test // DATAREDIS-525
	public void sRandMemberReturnsRandomMembers() {

		nativeCommands.sadd(KEY_1, VALUE_1, VALUE_2, VALUE_3);

		StepVerifier.create(connection.setCommands().sRandMember(KEY_1_BBUFFER, 2L)) //
				.expectNextCount(2) //
				.verifyComplete();
	}

}
