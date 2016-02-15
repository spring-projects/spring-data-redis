/*
 * Copyright 2015-2016 the original author or authors.
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
package org.springframework.data.redis.connection;

/**
 * @author Christoph Strobl
 */
public interface ClusterConnectionTests {

	/**
	 * @see DATAREDIS-315
	 */
	void shouldAllowSettingAndGettingValues();

	/**
	 * @see DATAREDIS-315
	 */
	void delShouldRemoveSingleKeyCorrectly();

	/**
	 * @see DATAREDIS-315
	 */
	void delShouldRemoveMultipleKeysCorrectly();

	/**
	 * @see DATAREDIS-315
	 */
	void delShouldRemoveMultipleKeysOnSameSlotCorrectly();

	/**
	 * @see DATAREDIS-315
	 */
	void typeShouldReadKeyTypeCorrectly();

	/**
	 * @see DATAREDIS-315
	 */
	void keysShouldReturnAllKeys();

	/**
	 * @see DATAREDIS-315
	 */
	void keysShouldReturnAllKeysForSpecificNode();

	/**
	 * @see DATAREDIS-315
	 */
	void randomKeyShouldReturnCorrectlyWhenKeysAvailable();

	/**
	 * @see DATAREDIS-315
	 */
	void randomKeyShouldReturnNullWhenNoKeysAvailable();

	/**
	 * @see DATAREDIS-315
	 */
	void rename();

	/**
	 * @see DATAREDIS-315
	 */
	void renameSameKeysOnSameSlot();

	/**
	 * @see DATAREDIS-315
	 */
	void renameNXWhenTargetKeyDoesNotExist();

	/**
	 * @see DATAREDIS-315
	 */
	void renameNXWhenTargetKeyDoesExist();

	/**
	 * @see DATAREDIS-315
	 */
	void renameNXWhenOnSameSlot();

	/**
	 * @see DATAREDIS-315
	 */
	void expireShouldBeSetCorreclty();

	/**
	 * @see DATAREDIS-315
	 */
	void pExpireShouldBeSetCorreclty();

	/**
	 * @see DATAREDIS-315
	 */
	void expireAtShouldBeSetCorrectly();

	/**
	 * @see DATAREDIS-315
	 */
	void pExpireAtShouldBeSetCorrectly();

	/**
	 * @see DATAREDIS-315
	 */
	void persistShoudRemoveTTL();

	/**
	 * @see DATAREDIS-315
	 */
	void moveShouldNotBeSupported();

	/**
	 * @see DATAREDIS-315
	 */
	void dbSizeShouldReturnCummulatedDbSize();

	/**
	 * @see DATAREDIS-315
	 */
	void dbSizeForSpecificNodeShouldGetNodeDbSize();

	/**
	 * @see DATAREDIS-315
	 */
	void ttlShouldReturnMinusTwoWhenKeyDoesNotExist();

	/**
	 * @see DATAREDIS-315
	 */
	void ttlShouldReturnMinusOneWhenKeyDoesNotHaveExpirationSet();

	/**
	 * @see DATAREDIS-315
	 */
	void ttlShouldReturnValueCorrectly();

	/**
	 * @see DATAREDIS-315
	 */
	void pTtlShouldReturnMinusTwoWhenKeyDoesNotExist();

	/**
	 * @see DATAREDIS-315
	 */
	void pTtlShouldReturnMinusOneWhenKeyDoesNotHaveExpirationSet();

	/**
	 * @see DATAREDIS-315
	 */
	void pTtlShouldReturValueCorrectly();

	/**
	 * @see DATAREDIS-315
	 */
	void sortShouldReturnValuesCorrectly();

	/**
	 * @see DATAREDIS-315
	 */
	void sortAndStoreShouldAddSortedValuesValuesCorrectly();

	/**
	 * @see DATAREDIS-315
	 */
	void sortAndStoreShouldReturnZeroWhenListDoesNotExist();

	/**
	 * @see DATAREDIS-315
	 */
	void dumpAndRestoreShouldWorkCorrectly();

	/**
	 * @see DATAREDIS-315
	 */
	void getShouldReturnValueCorrectly();

	/**
	 * @see DATAREDIS-315
	 */
	void getSetShouldWorkCorrectly();

	/**
	 * @see DATAREDIS-315
	 */
	void mGetShouldReturnCorrectlyWhenKeysMapToSameSlot();

	/**
	 * @see DATAREDIS-315
	 */
	void mGetShouldReturnCorrectlyWhenKeysDoNotMapToSameSlot();

	/**
	 * @see DATAREDIS-315
	 */
	void setShouldSetValueCorrectly();

	/**
	 * @see DATAREDIS-315
	 */
	void setNxShouldSetValueCorrectly();

	/**
	 * @see DATAREDIS-315
	 */
	void setNxShouldNotSetValueWhenAlreadyExistsInDBCorrectly();

	/**
	 * @see DATAREDIS-315
	 */
	void setExShouldSetValueCorrectly();

	/**
	 * @see DATAREDIS-315
	 */
	void pSetExShouldSetValueCorrectly();

	/**
	 * @see DATAREDIS-315
	 */
	void mSetShouldWorkWhenKeysMapToSameSlot();

	/**
	 * @see DATAREDIS-315
	 */
	void mSetShouldWorkWhenKeysDoNotMapToSameSlot();

	/**
	 * @see DATAREDIS-315
	 */
	void mSetNXShouldReturnTrueIfAllKeysSet();

	/**
	 * @see DATAREDIS-315
	 */
	void mSetNXShouldReturnFalseIfNotAllKeysSet();

	/**
	 * @see DATAREDIS-315
	 */
	void mSetNXShouldWorkForOnSameSlotKeys();

	/**
	 * @see DATAREDIS-315
	 */
	void incrShouldIncreaseValueCorrectly();

	/**
	 * @see DATAREDIS-315
	 */
	void incrByShouldIncreaseValueCorrectly();

	/**
	 * @see DATAREDIS-315
	 */
	void incrByFloatShouldIncreaseValueCorrectly();

	/**
	 * @see DATAREDIS-315
	 */
	void decrShouldDecreaseValueCorrectly();

	/**
	 * @see DATAREDIS-315
	 */
	void decrByShouldDecreaseValueCorrectly();

	/**
	 * @see DATAREDIS-315
	 */
	void appendShouldAddValueCorrectly();

	/**
	 * @see DATAREDIS-315
	 */
	void getRangeShouldReturnValueCorrectly();

	/**
	 * @see DATAREDIS-315
	 */
	void setRangeShouldWorkCorrectly();

	/**
	 * @see DATAREDIS-315
	 */
	void getBitShouldWorkCorrectly();

	/**
	 * @see DATAREDIS-315
	 */
	void setBitShouldWorkCorrectly();

	/**
	 * @see DATAREDIS-315
	 */
	void bitCountShouldWorkCorrectly();

	/**
	 * @see DATAREDIS-315
	 */
	void bitCountWithRangeShouldWorkCorrectly();

	/**
	 * @see DATAREDIS-315
	 */
	void bitOpShouldThrowExceptionWhenKeysDoNotMapToSameSlot();

	/**
	 * @see DATAREDIS-315
	 */
	void strLenShouldWorkCorrectly();

	/**
	 * @see DATAREDIS-315
	 */
	void rPushShoultAddValuesCorrectly();

	/**
	 * @see DATAREDIS-315
	 */
	void lPushShoultAddValuesCorrectly();

	/**
	 * @see DATAREDIS-315
	 */
	void rPushNXShoultNotAddValuesWhenKeyDoesNotExist();

	/**
	 * @see DATAREDIS-315
	 */
	void lPushNXShoultNotAddValuesWhenKeyDoesNotExist();

	/**
	 * @see DATAREDIS-315
	 */
	void lLenShouldCountValuesCorrectly();

	/**
	 * @see DATAREDIS-315
	 */
	void lRangeShouldGetValuesCorrectly();

	/**
	 * @see DATAREDIS-315
	 */
	void lTrimShouldTrimListCorrectly();

	/**
	 * @see DATAREDIS-315
	 */
	void lIndexShouldGetElementAtIndexCorrectly();

	/**
	 * @see DATAREDIS-315
	 */
	void lInsertShouldAddElementAtPositionCorrectly();

	/**
	 * @see DATAREDIS-315
	 */
	void lSetShouldSetElementAtPositionCorrectly();

	/**
	 * @see DATAREDIS-315
	 */
	void lRemShouldRemoveElementAtPositionCorrectly();

	/**
	 * @see DATAREDIS-315
	 */
	void lPopShouldReturnElementCorrectly();

	/**
	 * @see DATAREDIS-315
	 */
	void rPopShouldReturnElementCorrectly();

	/**
	 * @see DATAREDIS-315
	 */
	void blPopShouldPopElementCorectly();

	/**
	 * @see DATAREDIS-315
	 */
	void blPopShouldPopElementCorectlyWhenKeyOnSameSlot();

	/**
	 * @see DATAREDIS-315
	 */
	void brPopShouldPopElementCorectly();

	/**
	 * @see DATAREDIS-315
	 */
	void brPopShouldPopElementCorectlyWhenKeyOnSameSlot();

	/**
	 * @see DATAREDIS-315
	 */
	void rPopLPushShouldWorkWhenDoNotMapToSameSlot();

	/**
	 * @see DATAREDIS-315
	 */
	public void rPopLPushShouldWorkWhenKeysOnSameSlot();

	/**
	 * @see DATAREDIS-315
	 */
	void bRPopLPushShouldWork();

	/**
	 * @see DATAREDIS-315
	 */
	void bRPopLPushShouldWorkOnSameSlotKeys();

	/**
	 * @see DATAREDIS-315
	 */
	void sAddShouldAddValueToSetCorrectly();

	/**
	 * @see DATAREDIS-315
	 */
	void sRemShouldRemoveValueFromSetCorrectly();

	/**
	 * @see DATAREDIS-315
	 */
	void sPopShouldPopValueFromSetCorrectly();

	/**
	 * @see DATAREDIS-315
	 */
	void sMoveShouldWorkWhenKeysMapToSameSlot();

	/**
	 * @see DATAREDIS-315
	 */
	void sMoveShouldWorkWhenKeysDoNotMapToSameSlot();

	/**
	 * @see DATAREDIS-315
	 */
	void sCardShouldCountValuesInSetCorrectly();

	/**
	 * @see DATAREDIS-315
	 */
	void sIsMemberShouldReturnTrueIfValueIsMemberOfSet();

	/**
	 * @see DATAREDIS-315
	 */
	void sIsMemberShouldReturnFalseIfValueIsMemberOfSet();

	/**
	 * @see DATAREDIS-315
	 */
	void sInterShouldWorkForKeysMappingToSameSlot();

	/**
	 * @see DATAREDIS-315
	 */
	void sInterShouldWorkForKeysNotMappingToSameSlot();

	/**
	 * @see DATAREDIS-315
	 */
	void sInterStoreShouldWorkForKeysMappingToSameSlot();

	/**
	 * @see DATAREDIS-315
	 */
	void sInterStoreShouldWorkForKeysNotMappingToSameSlot();

	/**
	 * @see DATAREDIS-315
	 */
	void sUnionShouldWorkForKeysMappingToSameSlot();

	/**
	 * @see DATAREDIS-315
	 */
	void sUnionShouldWorkForKeysNotMappingToSameSlot();

	/**
	 * @see DATAREDIS-315
	 */
	void sUnionStoreShouldWorkForKeysMappingToSameSlot();

	/**
	 * @see DATAREDIS-315
	 */
	void sUnionStoreShouldWorkForKeysNotMappingToSameSlot();

	/**
	 * @see DATAREDIS-315
	 */
	void sDiffShouldWorkWhenKeysMapToSameSlot();

	/**
	 * @see DATAREDIS-315
	 */
	void sDiffShouldWorkWhenKeysNotMapToSameSlot();

	/**
	 * @see DATAREDIS-315
	 */
	void sDiffStoreShouldWorkWhenKeysMapToSameSlot();

	/**
	 * @see DATAREDIS-315
	 */
	void sDiffStoreShouldWorkWhenKeysNotMapToSameSlot();

	/**
	 * @see DATAREDIS-315
	 */
	void sMembersShouldReturnValuesContainedInSetCorrectly();

	/**
	 * @see DATAREDIS-315
	 */
	void sRandMamberShouldReturnValueCorrectly();

	/**
	 * @see DATAREDIS-315
	 */
	void sRandMamberWithCountShouldReturnValueCorrectly();

	/**
	 * @see DATAREDIS-315
	 */
	void sscanShouldRetrieveAllValuesInSetCorrectly();

	/**
	 * @see DATAREDIS-315
	 */
	void zAddShouldAddValueWithScoreCorrectly();

	/**
	 * @see DATAREDIS-315
	 */
	void zRemShouldRemoveValueWithScoreCorrectly();

	/**
	 * @see DATAREDIS-315
	 */
	void zIncrByShouldIncScoreForValueCorrectly();

	/**
	 * @see DATAREDIS-315
	 */
	void zRankShouldReturnPositionForValueCorrectly();

	/**
	 * @see DATAREDIS-315
	 */
	void zRankShouldReturnReversePositionForValueCorrectly();

	/**
	 * @see DATAREDIS-315
	 */
	void zRangeShouldReturnValuesCorrectly();

	/**
	 * @see DATAREDIS-315
	 */
	void zRangeWithScoresShouldReturnValuesAndScoreCorrectly();

	/**
	 * @see DATAREDIS-315
	 */
	void zRangeByScoreShouldReturnValuesCorrectly();

	/**
	 * @see DATAREDIS-315
	 */
	void zRangeByScoreWithScoresShouldReturnValuesAndScoreCorrectly();

	/**
	 * @see DATAREDIS-315
	 */
	void zRangeByScoreShouldReturnValuesCorrectlyWhenGivenOffsetAndScore();

	/**
	 * @see DATAREDIS-315
	 */
	void zRangeByScoreWithScoresShouldReturnValuesCorrectlyWhenGivenOffsetAndScore();

	/**
	 * @see DATAREDIS-315
	 */
	void zRevRangeShouldReturnValuesCorrectly();

	/**
	 * @see DATAREDIS-315
	 */
	void zRevRangeWithScoresShouldReturnValuesAndScoreCorrectly();

	/**
	 * @see DATAREDIS-315
	 */
	void zRevRangeByScoreShouldReturnValuesCorrectly();

	/**
	 * @see DATAREDIS-315
	 */
	void zRevRangeByScoreWithScoresShouldReturnValuesAndScoreCorrectly();

	/**
	 * @see DATAREDIS-315
	 */
	void zRevRangeByScoreShouldReturnValuesCorrectlyWhenGivenOffsetAndScore();

	/**
	 * @see DATAREDIS-315
	 */
	void zRevRangeByScoreWithScoresShouldReturnValuesCorrectlyWhenGivenOffsetAndScore();

	/**
	 * @see DATAREDIS-315
	 */
	void zCountShouldCountValuesInRange();

	/**
	 * @see DATAREDIS-315
	 */
	void zCardShouldReturnTotalNumberOfValues();

	/**
	 * @see DATAREDIS-315
	 */
	void zScoreShouldRetrieveScoreForValue();

	/**
	 * @see DATAREDIS-315
	 */
	void zRemRangeShouldRemoveValues();

	/**
	 * @see DATAREDIS-315
	 */
	void zRemRangeByScoreShouldRemoveValues();

	/**
	 * @see DATAREDIS-315
	 */
	void zUnionStoreShouldWorkForSameSlotKeys();

	/**
	 * @see DATAREDIS-315
	 */
	void zUnionStoreShouldThrowExceptionWhenKeysDoNotMapToSameSlots();

	/**
	 * @see DATAREDIS-315
	 */
	void zInterStoreShouldWorkForSameSlotKeys();

	/**
	 * @see DATAREDIS-315
	 */
	void zInterStoreShouldThrowExceptionWhenKeysDoNotMapToSameSlots();

	/**
	 * @see DATAREDIS-315
	 */
	void hSetShouldSetValueCorrectly();

	/**
	 * @see DATAREDIS-315
	 */
	void hSetNXShouldSetValueCorrectly();

	/**
	 * @see DATAREDIS-315
	 */
	void hSetNXShouldNotSetValueWhenAlreadyExists();

	/**
	 * @see DATAREDIS-315
	 */
	void hGetShouldRetrieveValueCorrectly();

	/**
	 * @see DATAREDIS-315
	 */
	void hMGetShouldRetrieveValueCorrectly();

	/**
	 * @see DATAREDIS-315
	 */
	void hMSetShouldAddValuesCorrectly();

	/**
	 * @see DATAREDIS-315
	 */
	void hIncrByShouldIncreaseFieldCorretly();

	/**
	 * @see DATAREDIS-315
	 */
	void hIncrByFloatShouldIncreaseFieldCorretly();

	/**
	 * @see DATAREDIS-315
	 */
	void hExistsShouldReturnPresenceOfFieldCorrectly();

	/**
	 * @see DATAREDIS-315
	 */
	void hDelShouldRemoveFieldsCorrectly();

	/**
	 * @see DATAREDIS-315
	 */
	void hLenShouldRetrieveSizeCorrectly();

	/**
	 * @see DATAREDIS-315
	 */
	void hKeysShouldRetrieveKeysCorrectly();

	/**
	 * @see DATAREDIS-315
	 */
	void hValsShouldRetrieveValuesCorrectly();

	/**
	 * @see DATAREDIS-315
	 */
	void hGetAllShouldRetrieveEntriesCorrectly();

	/**
	 * @see DATAREDIS-315
	 */
	void multiShouldThrowException();

	/**
	 * @see DATAREDIS-315
	 */
	void execShouldThrowException();

	/**
	 * @see DATAREDIS-315
	 */
	void discardShouldThrowException();

	/**
	 * @see DATAREDIS-315
	 */
	void watchShouldThrowException();

	/**
	 * @see DATAREDIS-315
	 */
	void unwatchShouldThrowException();

	/**
	 * @see DATAREDIS-315
	 */
	void selectShouldAllowSelectionOfDBIndexZero();

	/**
	 * @see DATAREDIS-315
	 */
	void selectShouldThrowExceptionWhenSelectingNonZeroDbIndex();

	/**
	 * @see DATAREDIS-315
	 */
	void echoShouldReturnInputCorrectly();

	/**
	 * @see DATAREDIS-315
	 */
	void pingShouldRetrunPongForExistingNode();

	/**
	 * @see DATAREDIS-315
	 */
	void pingShouldRetrunPong();

	/**
	 * @see DATAREDIS-315
	 */
	void pingShouldThrowExceptionWhenNodeNotKnownToCluster();

	/**
	 * @see DATAREDIS-315
	 */
	void flushDbShouldFlushAllClusterNodes();

	/**
	 * @see DATAREDIS-315
	 */
	void flushDbOnSingleNodeShouldFlushOnlyGivenNodesDb();

	/**
	 * @see DATAREDIS-315
	 */
	void zRangeByLexShouldReturnResultCorrectly();

	/**
	 * @see DATAREDIS-315
	 */
	void infoShouldCollectionInfoFromAllClusterNodes();

	/**
	 * @see DATAREDIS-315
	 */
	void clientListShouldGetInfosForAllClients();

	/**
	 * @see DATAREDIS-315
	 */
	void getClusterNodeForKeyShouldReturnNodeCorrectly();

	/**
	 * @see DATAREDIS-315
	 */
	void countKeysShouldReturnNumberOfKeysInSlot();

	/**
	 * @see DATAREDIS-315
	 */

	void pfAddShouldAddValuesCorrectly();

	/**
	 * @see DATAREDIS-315
	 */
	void pfCountShouldAllowCountingOnSingleKey();

	/**
	 * @see DATAREDIS-315
	 */
	void pfCountShouldAllowCountingOnSameSlotKeys();

	/**
	 * @see DATAREDIS-315
	 */
	void pfCountShouldThrowErrorCountingOnDifferentSlotKeys();

	/**
	 * @see DATAREDIS-315
	 */
	void pfMergeShouldWorkWhenAllKeysMapToSameSlot();

	/**
	 * @see DATAREDIS-315
	 */
	public void pfMergeShouldThrowErrorOnDifferentSlotKeys();

	/**
	 * @see DATAREDIS-315
	 */
	void infoShouldCollectInfoForSpecificNode();

	/**
	 * @see DATAREDIS-315
	 */
	void infoShouldCollectInfoForSpecificNodeAndSection();

	/**
	 * @see DATAREDIS-315
	 */
	void getConfigShouldLoadCumulatedConfiguration();

	/**
	 * @see DATAREDIS-315
	 */
	void getConfigShouldLoadConfigurationOfSpecificNode();

	/**
	 * @see DATAREDIS-315
	 */
	void clusterGetSlavesShouldReturnSlaveCorrectly();

	/**
	 * @see DATAREDIS-315
	 */
	void clusterGetMasterSlaveMapShouldListMastersAndSlavesCorrectly();

	/**
	 * @see DATAREDIS-316
	 */
	void setWithExpirationInSecondsShouldWorkCorrectly();

	/**
	 * @see DATAREDIS-316
	 */
	void setWithExpirationInMillisecondsShouldWorkCorrectly();

	/**
	 * @see DATAREDIS-316
	 */
	void setWithOptionIfPresentShouldWorkCorrectly();

	/**
	 * @see DATAREDIS-316
	 */
	void setWithOptionIfAbsentShouldWorkCorrectly();

	/**
	 * @see DATAREDIS-316
	 */
	void setWithExpirationAndIfAbsentShouldWorkCorrectly();

	/**
	 * @see DATAREDIS-316
	 */
	void setWithExpirationAndIfAbsentShouldNotBeAppliedWhenKeyExists();

	/**
	 * @see DATAREDIS-316
	 */
	void setWithExpirationAndIfPresentShouldWorkCorrectly();

	/**
	 * @see DATAREDIS-316
	 */
	void setWithExpirationAndIfPresentShouldNotBeAppliedWhenKeyDoesNotExists();

}
