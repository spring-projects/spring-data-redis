/*
 * Copyright 2015-2021 the original author or authors.
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
package org.springframework.data.redis.connection;

import org.springframework.data.geo.Point;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public interface ClusterConnectionTests {

	Point POINT_ARIGENTO = new Point(13.583333, 37.316667);
	Point POINT_CATANIA = new Point(15.087269, 37.502669);
	Point POINT_PALERMO = new Point(13.361389, 38.115556);

	// DATAREDIS-315
	void appendShouldAddValueCorrectly();

	// DATAREDIS-315
	void bRPopLPushShouldWork();

	// DATAREDIS-315
	void bRPopLPushShouldWorkOnSameSlotKeys();

	// DATAREDIS-315
	void bitCountShouldWorkCorrectly();

	// DATAREDIS-315
	void bitCountWithRangeShouldWorkCorrectly();

	// DATAREDIS-315
	void bitOpShouldThrowExceptionWhenKeysDoNotMapToSameSlot();

	// DATAREDIS-315
	void blPopShouldPopElementCorrectly();

	// DATAREDIS-315
	void blPopShouldPopElementCorrectlyWhenKeyOnSameSlot();

	// DATAREDIS-315
	void brPopShouldPopElementCorrectly();

	// DATAREDIS-315
	void brPopShouldPopElementCorrectlyWhenKeyOnSameSlot();

	// DATAREDIS-315
	void clientListShouldGetInfosForAllClients();

	// DATAREDIS-315
	void clusterGetMasterSlaveMapShouldListMastersAndSlavesCorrectly();

	// DATAREDIS-315
	void clusterGetSlavesShouldReturnSlaveCorrectly();

	// DATAREDIS-315
	void countKeysShouldReturnNumberOfKeysInSlot();

	// DATAREDIS-315
	void dbSizeForSpecificNodeShouldGetNodeDbSize();

	// DATAREDIS-315
	void dbSizeShouldReturnCummulatedDbSize();

	// DATAREDIS-315
	void decrByShouldDecreaseValueCorrectly();

	// DATAREDIS-315
	void decrShouldDecreaseValueCorrectly();

	// DATAREDIS-315
	void delShouldRemoveMultipleKeysCorrectly();

	// DATAREDIS-315
	void delShouldRemoveMultipleKeysOnSameSlotCorrectly();

	// DATAREDIS-315
	void delShouldRemoveSingleKeyCorrectly();

	// DATAREDIS-315
	void discardShouldThrowException();

	// DATAREDIS-315
	void dumpAndRestoreShouldWorkCorrectly();

	// DATAREDIS-696
	void dumpAndRestoreWithReplaceOptionShouldWorkCorrectly();

	// DATAREDIS-315
	void echoShouldReturnInputCorrectly();

	// DATAREDIS-315
	void execShouldThrowException();

	// DATAREDIS-529
	void existsShouldCountSameKeyMultipleTimes();

	// DATAREDIS-529
	void existsWithMultipleKeysShouldConsiderAbsentKeys();

	// DATAREDIS-529
	void existsWithMultipleKeysShouldReturnResultCorrectly();

	// DATAREDIS-315
	void expireAtShouldBeSetCorrectly();

	// DATAREDIS-315
	void expireShouldBeSetCorreclty();

	// DATAREDIS-315
	void flushDbOnSingleNodeShouldFlushOnlyGivenNodesDb();

	// DATAREDIS-315
	void flushDbShouldFlushAllClusterNodes();

	// DATAREDIS-438
	void geoAddMultipleGeoLocations();

	// DATAREDIS-438
	void geoAddSingleGeoLocation();

	// DATAREDIS-438
	void geoDist();

	// DATAREDIS-438
	void geoDistWithMetric();

	// DATAREDIS-438
	void geoHash();

	// DATAREDIS-438
	void geoHashNonExisting();

	// DATAREDIS-438
	void geoPosition();

	// DATAREDIS-438
	void geoPositionNonExisting();

	// DATAREDIS-438
	void geoRadiusByMemberShouldApplyLimit();

	// DATAREDIS-438
	void geoRadiusByMemberShouldReturnDistanceCorrectly();

	// DATAREDIS-438
	void geoRadiusByMemberShouldReturnMembersCorrectly();

	// DATAREDIS-438
	void geoRadiusShouldApplyLimit();

	// DATAREDIS-438
	void geoRadiusShouldReturnDistanceCorrectly();

	// DATAREDIS-438
	void geoRadiusShouldReturnMembersCorrectly();

	// DATAREDIS-438
	void geoRemoveDeletesMembers();

	// DATAREDIS-315
	void getBitShouldWorkCorrectly();

	// DATAREDIS-315
	void getClusterNodeForKeyShouldReturnNodeCorrectly();

	// DATAREDIS-315
	void getConfigShouldLoadConfigurationOfSpecificNode();

	// DATAREDIS-315
	void getConfigShouldLoadCumulatedConfiguration();

	// DATAREDIS-315
	void getRangeShouldReturnValueCorrectly();

	// GH-2050
	void getExShouldWorkCorrectly();

	// GH-2050
	void getDelShouldWorkCorrectly();

	// DATAREDIS-315
	void getSetShouldWorkCorrectly();

	// DATAREDIS-315
	void getShouldReturnValueCorrectly();

	// DATAREDIS-315
	void hDelShouldRemoveFieldsCorrectly();

	// DATAREDIS-315
	void hExistsShouldReturnPresenceOfFieldCorrectly();

	// DATAREDIS-315
	void hGetAllShouldRetrieveEntriesCorrectly();

	// DATAREDIS-315
	void hGetShouldRetrieveValueCorrectly();

	// DATAREDIS-315
	void hIncrByFloatShouldIncreaseFieldCorretly();

	// DATAREDIS-315
	void hIncrByShouldIncreaseFieldCorretly();

	// DATAREDIS-315
	void hKeysShouldRetrieveKeysCorrectly();

	// DATAREDIS-315
	void hLenShouldRetrieveSizeCorrectly();

	// DATAREDIS-315
	void hMGetShouldRetrieveValueCorrectly();

	// DATAREDIS-315
	void hMSetShouldAddValuesCorrectly();

	// DATAREDIS-479
	public void hScanShouldReadEntireValueRange();

	// DATAREDIS-315
	void hSetNXShouldNotSetValueWhenAlreadyExists();

	// DATAREDIS-315
	void hSetNXShouldSetValueCorrectly();

	// DATAREDIS-315
	void hSetShouldSetValueCorrectly();

	// DATAREDIS-698
	void hStrLenReturnsFieldLength();

	// DATAREDIS-698
	void hStrLenReturnsZeroWhenFieldDoesNotExist();

	// DATAREDIS-698
	void hStrLenReturnsZeroWhenKeyDoesNotExist();

	// DATAREDIS-315
	void hValsShouldRetrieveValuesCorrectly();

	// DATAREDIS-315
	void incrByFloatShouldIncreaseValueCorrectly();

	// DATAREDIS-315
	void incrByShouldIncreaseValueCorrectly();

	// DATAREDIS-315
	void incrShouldIncreaseValueCorrectly();

	// DATAREDIS-315
	void infoShouldCollectInfoForSpecificNode();

	// DATAREDIS-315
	void infoShouldCollectInfoForSpecificNodeAndSection();

	// DATAREDIS-315
	void infoShouldCollectionInfoFromAllClusterNodes();

	// DATAREDIS-315
	void keysShouldReturnAllKeys();

	// DATAREDIS-315
	void keysShouldReturnAllKeysForSpecificNode();

	// DATAREDIS-635
	void scanShouldReturnAllKeys();

	// DATAREDIS-635
	void scanShouldReturnAllKeysForSpecificNode();

	// DATAREDIS-315
	void lIndexShouldGetElementAtIndexCorrectly();

	// DATAREDIS-315
	void lInsertShouldAddElementAtPositionCorrectly();

	// DATAREDIS-315
	void lLenShouldCountValuesCorrectly();

	// DATAREDIS-315
	void lPopShouldReturnElementCorrectly();

	// DATAREDIS-315
	void lPushNXShouldNotAddValuesWhenKeyDoesNotExist();

	// DATAREDIS-315
	void lPushShouldAddValuesCorrectly();

	// DATAREDIS-315
	void lRangeShouldGetValuesCorrectly();

	// DATAREDIS-315
	void lRemShouldRemoveElementAtPositionCorrectly();

	// DATAREDIS-315
	void lSetShouldSetElementAtPositionCorrectly();

	// DATAREDIS-315
	void lTrimShouldTrimListCorrectly();

	// DATAREDIS-315
	void mGetShouldReturnCorrectlyWhenKeysDoNotMapToSameSlot();

	// DATAREDIS-756
	void mGetShouldReturnMultipleSameKeysWhenKeysDoNotMapToSameSlot();

	// DATAREDIS-315
	void mGetShouldReturnCorrectlyWhenKeysMapToSameSlot();

	// DATAREDIS-756
	void mGetShouldReturnMultipleSameKeysWhenKeysMapToSameSlot();

	// DATAREDIS-315
	void mSetNXShouldReturnFalseIfNotAllKeysSet();

	// DATAREDIS-315
	void mSetNXShouldReturnTrueIfAllKeysSet();

	// DATAREDIS-315
	void mSetNXShouldWorkForOnSameSlotKeys();

	// DATAREDIS-315
	void mSetShouldWorkWhenKeysDoNotMapToSameSlot();

	// DATAREDIS-315
	void mSetShouldWorkWhenKeysMapToSameSlot();

	// DATAREDIS-315
	void moveShouldNotBeSupported();

	// DATAREDIS-315
	void multiShouldThrowException();

	// DATAREDIS-315
	void pExpireAtShouldBeSetCorrectly();

	// DATAREDIS-315
	void pExpireShouldBeSetCorreclty();

	// DATAREDIS-315
	void pSetExShouldSetValueCorrectly();

	// DATAREDIS-315
	void pTtlShouldReturValueCorrectly();

	// DATAREDIS-315
	void pTtlShouldReturnMinusOneWhenKeyDoesNotHaveExpirationSet();

	// DATAREDIS-315
	void pTtlShouldReturnMinusTwoWhenKeyDoesNotExist();

	// DATAREDIS-526
	void pTtlWithTimeUnitShouldReturnMinusTwoWhenKeyDoesNotExist();

	// DATAREDIS-315
	void persistShouldRemoveTTL();

	void pfAddShouldAddValuesCorrectly();

	// DATAREDIS-315
	void pfCountShouldAllowCountingOnSameSlotKeys();

	// DATAREDIS-315
	void pfCountShouldAllowCountingOnSingleKey();

	// DATAREDIS-315
	void pfCountShouldThrowErrorCountingOnDifferentSlotKeys();

	// DATAREDIS-315
	public void pfMergeShouldThrowErrorOnDifferentSlotKeys();

	// DATAREDIS-315
	void pfMergeShouldWorkWhenAllKeysMapToSameSlot();

	// DATAREDIS-315
	void pingShouldRetrunPong();

	// DATAREDIS-315
	void pingShouldRetrunPongForExistingNode();

	// DATAREDIS-315
	void pingShouldThrowExceptionWhenNodeNotKnownToCluster();

	// DATAREDIS-315
	void rPopLPushShouldWorkWhenDoNotMapToSameSlot();

	// DATAREDIS-315
	public void rPopLPushShouldWorkWhenKeysOnSameSlot();

	// DATAREDIS-315
	void rPopShouldReturnElementCorrectly();

	// DATAREDIS-315
	void rPushNXShouldNotAddValuesWhenKeyDoesNotExist();

	// DATAREDIS-315
	void rPushShouldAddValuesCorrectly();

	// DATAREDIS-315
	void randomKeyShouldReturnCorrectlyWhenKeysAvailable();

	// DATAREDIS-315
	void randomKeyShouldReturnNullWhenNoKeysAvailable();

	// DATAREDIS-315
	void rename();

	// DATAREDIS-1190
	void renameShouldOverwriteTargetKey();

	// DATAREDIS-315
	void renameNXWhenOnSameSlot();

	// DATAREDIS-315
	void renameNXWhenTargetKeyDoesExist();

	// DATAREDIS-315
	void renameNXWhenTargetKeyDoesNotExist();

	// DATAREDIS-315
	void renameSameKeysOnSameSlot();

	// DATAREDIS-315
	void sAddShouldAddValueToSetCorrectly();

	// DATAREDIS-315
	void sCardShouldCountValuesInSetCorrectly();

	// DATAREDIS-315
	void sDiffShouldWorkWhenKeysMapToSameSlot();

	// DATAREDIS-315
	void sDiffShouldWorkWhenKeysNotMapToSameSlot();

	// DATAREDIS-315
	void sDiffStoreShouldWorkWhenKeysMapToSameSlot();

	// DATAREDIS-315
	void sDiffStoreShouldWorkWhenKeysNotMapToSameSlot();

	// DATAREDIS-315
	void sInterShouldWorkForKeysMappingToSameSlot();

	// DATAREDIS-315
	void sInterShouldWorkForKeysNotMappingToSameSlot();

	// DATAREDIS-315
	void sInterStoreShouldWorkForKeysMappingToSameSlot();

	// DATAREDIS-315
	void sInterStoreShouldWorkForKeysNotMappingToSameSlot();

	// DATAREDIS-315
	void sIsMemberShouldReturnFalseIfValueIsMemberOfSet();

	// DATAREDIS-315
	void sIsMemberShouldReturnTrueIfValueIsMemberOfSet();

	// DATAREDIS-315
	void sMembersShouldReturnValuesContainedInSetCorrectly();

	// DATAREDIS-315
	void sMoveShouldWorkWhenKeysDoNotMapToSameSlot();

	// DATAREDIS-315
	void sMoveShouldWorkWhenKeysMapToSameSlot();

	// DATAREDIS-315
	void sPopShouldPopValueFromSetCorrectly();

	// DATAREDIS-315
	void sRandMamberShouldReturnValueCorrectly();

	// DATAREDIS-315
	void sRandMamberWithCountShouldReturnValueCorrectly();

	// DATAREDIS-315
	void sRemShouldRemoveValueFromSetCorrectly();

	// DATAREDIS-315
	void sUnionShouldWorkForKeysMappingToSameSlot();

	// DATAREDIS-315
	void sUnionShouldWorkForKeysNotMappingToSameSlot();

	// DATAREDIS-315
	void sUnionStoreShouldWorkForKeysMappingToSameSlot();

	// DATAREDIS-315
	void sUnionStoreShouldWorkForKeysNotMappingToSameSlot();

	// DATAREDIS-315
	void selectShouldAllowSelectionOfDBIndexZero();

	// DATAREDIS-315
	void selectShouldThrowExceptionWhenSelectingNonZeroDbIndex();

	// DATAREDIS-315
	void setBitShouldWorkCorrectly();

	// DATAREDIS-315
	void setExShouldSetValueCorrectly();

	// DATAREDIS-315
	void setNxShouldNotSetValueWhenAlreadyExistsInDBCorrectly();

	// DATAREDIS-315
	void setNxShouldSetValueCorrectly();

	// DATAREDIS-315
	void setRangeShouldWorkCorrectly();

	// DATAREDIS-315
	void setShouldSetValueCorrectly();

	// DATAREDIS-316
	void setWithExpirationAndIfAbsentShouldNotBeAppliedWhenKeyExists();

	// DATAREDIS-316
	void setWithExpirationAndIfAbsentShouldWorkCorrectly();

	// DATAREDIS-316
	void setWithExpirationAndIfPresentShouldNotBeAppliedWhenKeyDoesNotExists();

	// DATAREDIS-316
	void setWithExpirationAndIfPresentShouldWorkCorrectly();

	// DATAREDIS-316
	void setWithExpirationInMillisecondsShouldWorkCorrectly();

	// DATAREDIS-316
	void setWithExpirationInSecondsShouldWorkCorrectly();

	// DATAREDIS-316
	void setWithOptionIfAbsentShouldWorkCorrectly();

	// DATAREDIS-316
	void setWithOptionIfPresentShouldWorkCorrectly();

	// DATAREDIS-315

	// DATAREDIS-315
	void shouldAllowSettingAndGettingValues();

	// DATAREDIS-315
	void sortAndStoreShouldAddSortedValuesValuesCorrectly();

	// DATAREDIS-315
	void sortAndStoreShouldReturnZeroWhenListDoesNotExist();

	// DATAREDIS-315
	void sortShouldReturnValuesCorrectly();

	// DATAREDIS-315
	void sscanShouldRetrieveAllValuesInSetCorrectly();

	// DATAREDIS-315
	void strLenShouldWorkCorrectly();

	// DATAREDIS-315
	void ttlShouldReturnMinusOneWhenKeyDoesNotHaveExpirationSet();

	// DATAREDIS-315
	void ttlShouldReturnMinusTwoWhenKeyDoesNotExist();

	// DATAREDIS-315
	void ttlShouldReturnValueCorrectly();

	// DATAREDIS-526
	void ttlWithTimeUnitShouldReturnMinusTwoWhenKeyDoesNotExist();

	// DATAREDIS-315
	void typeShouldReadKeyTypeCorrectly();

	// DATAREDIS-315
	void unwatchShouldThrowException();

	// DATAREDIS-315
	void watchShouldThrowException();

	// DATAREDIS-315
	void zAddShouldAddValueWithScoreCorrectly();

	// DATAREDIS-315
	void zCardShouldReturnTotalNumberOfValues();

	// DATAREDIS-315
	void zCountShouldCountValuesInRange();

	// DATAREDIS-315
	void zIncrByShouldIncScoreForValueCorrectly();

	// DATAREDIS-315
	void zInterStoreShouldThrowExceptionWhenKeysDoNotMapToSameSlots();

	// DATAREDIS-315
	void zInterStoreShouldWorkForSameSlotKeys();

	// DATAREDIS-315
	void zRangeByLexShouldReturnResultCorrectly();

	// DATAREDIS-315
	void zRangeByScoreShouldReturnValuesCorrectly();

	// DATAREDIS-315
	void zRangeByScoreShouldReturnValuesCorrectlyWhenGivenOffsetAndScore();

	// DATAREDIS-315
	void zRangeByScoreWithScoresShouldReturnValuesAndScoreCorrectly();

	// DATAREDIS-315
	void zRangeByScoreWithScoresShouldReturnValuesCorrectlyWhenGivenOffsetAndScore();

	// DATAREDIS-315
	void zRangeShouldReturnValuesCorrectly();

	// DATAREDIS-315
	void zRangeWithScoresShouldReturnValuesAndScoreCorrectly();

	// DATAREDIS-315
	void zRankShouldReturnPositionForValueCorrectly();

	// DATAREDIS-315
	void zRankShouldReturnReversePositionForValueCorrectly();

	// DATAREDIS-315
	void zRemRangeByScoreShouldRemoveValues();

	// DATAREDIS-315
	void zRemRangeShouldRemoveValues();

	// DATAREDIS-315
	void zRemShouldRemoveValueWithScoreCorrectly();

	// DATAREDIS-315
	void zRevRangeByScoreShouldReturnValuesCorrectly();

	// DATAREDIS-315
	void zRevRangeByScoreShouldReturnValuesCorrectlyWhenGivenOffsetAndScore();

	// DATAREDIS-315
	void zRevRangeByScoreWithScoresShouldReturnValuesAndScoreCorrectly();

	// DATAREDIS-315
	void zRevRangeByScoreWithScoresShouldReturnValuesCorrectlyWhenGivenOffsetAndScore();

	// DATAREDIS-315
	void zRevRangeShouldReturnValuesCorrectly();

	// DATAREDIS-315
	void zRevRangeWithScoresShouldReturnValuesAndScoreCorrectly();

	// DATAREDIS-479
	void zScanShouldReadEntireValueRange();

	// DATAREDIS-315
	void zScoreShouldRetrieveScoreForValue();

	// DATAREDIS-315
	void zUnionStoreShouldThrowExceptionWhenKeysDoNotMapToSameSlots();

	// DATAREDIS-315
	void zUnionStoreShouldWorkForSameSlotKeys();

}
