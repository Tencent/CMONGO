//cmongo_shard_targeter.h
/**
 *    Tencent is pleased to support the open source community by making CMONGO available.
 *
 *    Copyright (C) 2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 *    Licensed under the GNU Affero General Public License Version 3 (the "License");
 *    you may not use this file except in compliance with the License. You may obtain a 
 *    copy of the License at https://www.gnu.org/licenses/agpl-3.0.en.html
 *
 *    Unless required by applicable law or agreed to in writing, software distributed under
 *    the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 *    either express or implied. See the License for the specific language governing permissions
 *    and limitations under the License.
*/


#ifndef __MONGO_S_CMONGO_SHARD_TARGETER_H__
#define __MONGO_S_CMONGO_SHARD_TARGETER_H__

namespace mongo {

class CMongoShardTargeter : public NSTargeter {
public:
	CMongoShardTargeter(const NamespaceString& nss, TargeterStats* stats);

	Status init(OperationContext* txn);

	const NamespaceString& getNS() const ;

    // Returns ShardKeyNotFound if document does not have a full shard key.
    Status targetInsert(OperationContext* txn, const BSONObj& doc, ShardEndpoint** endpoint) const;

    // Returns ShardKeyNotFound if the update can't be targeted without a shard key.
    Status targetUpdate(OperationContext* txn,
                        const BatchedUpdateDocument& updateDoc,
                        std::vector<ShardEndpoint*>* endpoints) const;

    // Returns ShardKeyNotFound if the delete can't be targeted without a shard key.
    Status targetDelete(OperationContext* txn,
                        const BatchedDeleteDocument& deleteDoc,
                        std::vector<ShardEndpoint*>* endpoints) const;

    Status targetCollection(std::vector<ShardEndpoint*>* endpoints) const;

    Status targetAllShards(std::vector<ShardEndpoint*>* endpoints) const;

    void noteStaleResponse(const ShardEndpoint& endpoint, const BSONObj& staleInfo);

    void noteCouldNotTarget();

    /**
     * Replaces the targeting information with the latest information from the cache.  If this
     * information is stale WRT the noted stale responses or a remote refresh is needed due
     * to a targeting failure, will contact the config servers to reload the metadata.
     *
     * Reports wasChanged = true if the metadata is different after this reload.
     *
     * Also see NSTargeter::refreshIfNeeded().
     */
    Status refreshIfNeeded(OperationContext* txn, bool* wasChanged);

};
}
#endif //__MONGO_S_CMONGO_SHARD_TARGETER_H__
