//catalog_manager_cmongo.h
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
#pragma once

#include "mongo/client/connection_string.h"
#include "mongo/s/catalog/catalog_manager_common.h"
#include "mongo/s/config.h"
#include "mongo/grpc/grpc_client.h"
#include "mongo/s/catalog/type_shard.h"

namespace mongo {
class CatalogManagerCmongoTest;
class Query;

class CatalogManagerCmongo final : public CatalogManagerCommon {
public:
	typedef std::map<std::string, std::shared_ptr<DBConfigCMongo>> DBConfigMap;
	explicit CatalogManagerCmongo(const std::string& metaUrl, const std::string& clusterid, bool enableSharding);

	virtual ~CatalogManagerCmongo() = default;

	ConfigServerMode getMode() override {
		return ConfigServerMode::SCCC;
	}

    Status startup(OperationContext* txn, bool allowNetworking) override;

    void shutDown(OperationContext* txn, bool allowNetworking) override;

    Status shardCollection(OperationContext* txn,
                           const std::string& ns,
                           const ShardKeyPattern& fieldsAndOrder,
                           bool unique,
                           const std::vector<BSONObj>& initPoints,
                           const std::set<ShardId>& initShardIds) override;

    StatusWith<ShardDrainingStatus> removeShard(OperationContext* txn,
                                                const std::string& name) override;

    StatusWith<OpTimePair<DatabaseType>> getDatabase(OperationContext* txn,
                                                     const std::string& dbName) override;

    StatusWith<OpTimePair<CollectionType>> getCollection(OperationContext* txn,
                                                         const std::string& collNs) override;

    Status getCollections(OperationContext* txn,
                          const std::string* dbName,
                          std::vector<CollectionType>* collections,
                          repl::OpTime* optime);

    Status dropCollection(OperationContext* txn, const NamespaceString& ns) override;

    Status getDatabasesForShard(OperationContext* txn,
                                const std::string& shardName,
                                std::vector<std::string>* dbs) override;

    Status getChunks(OperationContext* txn,
                     const BSONObj& query,
                     const BSONObj& sort,
                     boost::optional<int> limit,
                     std::vector<ChunkType>* chunks,
                     repl::OpTime* opTime) override;

    Status getTagsForCollection(OperationContext* txn,
                                const std::string& collectionNs,
                                std::vector<TagsType>* tags) override;

    StatusWith<std::string> getTagForChunk(OperationContext* txn,
                                           const std::string& collectionNs,
                                           const ChunkType& chunk) override;

    StatusWith<OpTimePair<std::vector<ShardType>>> getAllShards(OperationContext* txn) override;

    bool runUserManagementWriteCommand(OperationContext* txn,
                                       const std::string& commandName,
                                       const std::string& dbname,
                                       const BSONObj& cmdObj,
                                       BSONObjBuilder* result) override;

    bool runUserManagementReadCommand(OperationContext* txn,
                                      const std::string& dbname,
                                      const BSONObj& cmdObj,
                                      BSONObjBuilder* result) override;

    Status applyChunkOpsDeprecated(OperationContext* txn,
                                   const BSONArray& updateOps,
                                   const BSONArray& preCondition,
                                   const std::string& nss,
                                   const ChunkVersion& lastChunkVersion) override;

    StatusWith<SettingsType> getGlobalSettings(OperationContext* txn,
                                               const std::string& key) override;

    void writeConfigServerDirect(OperationContext* txn,
                                 const BatchedCommandRequest& request,
                                 BatchedCommandResponse* response) override;

    Status insertConfigDocument(OperationContext* txn,
                                const std::string& ns,
                                const BSONObj& doc) override;

    StatusWith<bool> updateConfigDocument(OperationContext* txn,
                                          const std::string& ns,
                                          const BSONObj& query,
                                          const BSONObj& update,
                                          bool upsert) override;

    Status removeConfigDocuments(OperationContext* txn,
                                 const std::string& ns,
                                 const BSONObj& query) override;

    DistLockManager* getDistLockManager() override;

    Status initConfigVersion(OperationContext* txn) override;

    Status appendInfoForConfigServerDatabases(OperationContext* txn,
                                              BSONArrayBuilder* builder) override;

    bool isMetadataConsistentFromLastCheck(OperationContext* txn) override;

	StatusWith<std::shared_ptr<DBConfig>> getDbConfig(OperationContext *txn, const std::string& dbname) final;

	Status syncRoutes(OperationContext *txn, const masterproto::GetClusterRoutesRsp& data);

	Status syncRoutesRaw(OperationContext *txn, const masterproto::GetClusterRoutesRawRsp& data);

	Status dropDatabase(OperationContext *txn, const std::string& dbName) final;
    Status createDatabase(OperationContext *txn, const std::string& dbName) final;

    Status enableSharding(OperationContext* txn, const std::string& dbName) override;

    /*
    * NOTE(zhenyipeng) update shards list from master/etcd
    * Only support add empty shards, remove shards is not supported now
    */
    Status updateShards(OperationContext *txn) override;

private:
friend class CatalogManagerCmongoTest;
    Status _checkDbDoesNotExist(OperationContext* txn,
                                const std::string& dbName,
                                DatabaseType* db) override;

    StatusWith<std::string> _generateNewShardName(OperationContext* txn) override;

    Status _createCappedConfigCollection(OperationContext* txn,
                                         StringData collName,
                                         int cappedSize) override;

    /**
     * Starts the thread that periodically checks data consistency amongst the config servers.
     * Note: this is not thread safe and can only be called once for the lifetime.
     */
    Status _startConfigServerChecker();

    /**
     * Returns the number of shards recognized by the config servers
     * in this sharded cluster.
     * Optional: use query parameter to filter shard count.
     */
    size_t _getShardCount(const BSONObj& query) const;

    /**
     * Returns OK if all config servers that were contacted have the same state.
     * If inconsistency detected on first attempt, checks at most 3 more times.
     */
    Status _checkConfigServersConsistent(const unsigned tries = 4) const;

    /**
     * Checks data consistency amongst config servers every 60 seconds.
     */
    void _consistencyChecker();

    /**
     * Sends a read only command to the config server.
     */
    bool _runReadCommand(OperationContext* txn,
                         const std::string& dbname,
                         const BSONObj& cmdObj,
                         BSONObjBuilder* result);

    /**
     * Runs a query against the config servers with retry logic.
     */
    StatusWith<std::vector<BSONObj>> _findOnConfig(const std::string& ns,
                                                   const Query& query,
                                                   int limit);

    StatusWith<BSONObj> _findOneOnConfig(const std::string& ns, const Query& query);


	Status _initRoute();
	Status _parseFromPBClusterRoutes(const masterproto::GetClusterRoutesRsp&);
	Status _parseFromPBClusterRoutesRaw(const masterproto::GetClusterRoutesRawRsp&);
	Status _parseFromPBClusterInfo(const masterproto::GetClusterInfoRsp&);
	Status _initShard();
	
    ConnectionString _configServerConnectionString;
    std::vector<ConnectionString> _configServers;

    // protects _inShutdown, _consistentFromLastCheck; used by _consistencyCheckerCV
    stdx::mutex _mutex;

    // True if CatalogManagerLegacy::shutDown has been called. False, otherwise.
    bool _inShutdown = false;

    // Set to true once startup() has been called and returned an OK status.  Allows startup() to be
    // called multiple times with any time after the first successful call being a no-op.
    bool _started = false;

	std::unique_ptr<grpc::MasterClient> _masterClient;

	const std::string _clusterId;

	const bool _isShardCluster;

	DBConfigMap _dbs;

	std::vector<ShardType> _allShards;

	const std::string _metaUrl;
};
}
