//catalog_manager_cmongo.cpp
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

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kSharding

#include "mongo/base/status.h"
#include "mongo/base/status_with.h"
#include "mongo/grpc/grpc_client.h"
#include "mongo/grpc/masterproto/master.pb.h"
#include "mongo/grpc/masterproto/master.grpc.pb.h"
#include "mongo/grpc/cmongoproto/cmongo.pb.h"
#include "mongo/util/etcd_wrapper.h"
#include "mongo/util/log.h"
#include "mongo/s/catalog/cmongo/catalog_manager_cmongo.h"
#include "mongo/s/shard_key_pattern.h"
#include "mongo/s/catalog/type_chunk.h"
#include "mongo/s/catalog/type_collection.h"
#include "mongo/s/catalog/type_config_version.h"
#include "mongo/s/catalog/type_database.h"
#include "mongo/s/catalog/type_settings.h"
#include "mongo/s/catalog/type_shard.h"
#include "mongo/s/catalog/type_tags.h"
#include "mongo/util/mongoutils/str.h"
#include "mongo/util/stringutils.h"
#include "mongo/s/client/dbclient_multi_command.h"
#include "mongo/s/client/shard.h"
#include "mongo/s/client/shard_connection.h"
#include "mongo/s/client/shard_registry.h"
#include "mongo/util/cmongo_shard_helpers.h"
#include "mongo/client/replica_set_monitor.h"
#include "mongo/s/grid.h"

namespace mongo {

CatalogManagerCmongo::CatalogManagerCmongo(const std::string& metaUrl, const std::string& cluster, bool isShardedCluster)
	:_clusterId(cluster),
	 _isShardCluster(isShardedCluster),
	 _metaUrl(metaUrl) {
}

Status CatalogManagerCmongo::startup(OperationContext* txn, bool allowNetworking) {
	// TODO(deyukong): don't forget about non-shard clusters
	LOG(0)<<"CatalogManagerCmongo::startup begin";
	if (_started) {
		return Status::OK();
	}
	if (allowNetworking) {
		// NOTE(deyukong): allowNetworking is to check configSvr status
		// We may do some check here
	}

	_masterClient = std::unique_ptr<grpc::MasterClient>(new grpc::MasterClient(_metaUrl));
	Status s = Status::OK();

	try {
		s = _initShard();
	} catch (const DBException& e) {
		LOG(0)<<"ERROR:_initShard failed:"<<e.what();
		return Status(ErrorCodes::InternalError, e.what());
	} catch (std::runtime_error& e) {
		LOG(0)<<"ERROR:_initShard failed:"<<e.what();
		return Status(ErrorCodes::InternalError, e.what());
	}

	if (s.isOK()) {
		LOG(0)<<"initShard success";
	} else {
		LOG(0)<<"ERROR:initShard failed with status:" << s;
		return s;
	}

	try {
		s = _initRoute();
	} catch (const DBException& e) {
		LOG(0)<<"ERROR:initRoute failed:"<<e.what();
		return Status(ErrorCodes::InternalError, e.what());
	} catch (std::runtime_error& e) {
		LOG(0)<<"ERROR:initRoute failed:"<<e.what();
		return Status(ErrorCodes::InternalError, e.what());
	}
	if(s.isOK()) {
		LOG(0)<<"initRoute success";
	} else {
		LOG(0)<<"ERROR:initRoute failed with status:" << s;
		return s;
	}

	_started = true;
	LOG(0)<<"CatalogManagerCmongo::startup end";
	return Status::OK();
}

Status CatalogManagerCmongo::_initShard() {
	masterproto::GetClusterInfoReq req;
	masterproto::GetClusterInfoRsp rsp;
	req.set_allocated_header(new cmongoproto::ReqHeader());
	req.set_cluster_id(_clusterId);
	try {
		grpc::Status s = _masterClient->GetClusterInfo(req, &rsp);
		if (!s.ok()) {
			LOG(0)<<"_initShard failed:"<<int(s.error_code())<<":"<<s.error_message();
			return Status(ErrorCodes::InternalError, s.error_message());
		} else {
			// TODO(deyukong): invariant is not good here, since it depends on master
			// but master always sets header, I believe!
			invariant(rsp.has_header());
			if (rsp.header().ret_code() != 0) {
				LOG(0)<<"_initShard failed:"
					  <<int(rsp.header().ret_code())
					  <<":"
					  <<rsp.header().ret_msg();
				return Status(ErrorCodes::InternalError, rsp.header().ret_msg());
			} else {
				return _parseFromPBClusterInfo(rsp);
			}
		}
	} catch (const std::exception& e) {
		LOG(0)<<"query master got exception:"<<e.what();
		return Status(ErrorCodes::InternalError, e.what());
	}

}

Status CatalogManagerCmongo::updateShards(OperationContext *txn) {
	if (!_isShardCluster) {
		return Status(ErrorCodes::InvalidOptions, "cluster not sharded, no need to update shards");
	}
	stdx::lock_guard<stdx::mutex> lk(_mutex);

	// get new shard list from etcd
	std::vector<ShardType> shards;
	masterproto::GetClusterInfoReq req;
	masterproto::GetClusterInfoRsp rsp;
	req.set_allocated_header(new cmongoproto::ReqHeader());
	req.set_cluster_id(_clusterId);
	try {
		grpc::Status s = _masterClient->GetClusterInfo(req, &rsp);
		if (!s.ok()) {
			LOG(0) << "get cluster info from master failed:" << int(s.error_code()) << ":" << s.error_message();
			return Status(ErrorCodes::InternalError, s.error_message());
		} else {
			invariant(rsp.has_header());
			if (rsp.header().ret_code() != 0) {
				LOG(0) << "get cluster info failed:" << int(rsp.header().ret_code()) << ":" << rsp.header().ret_msg();
				return Status(ErrorCodes::InternalError, rsp.header().ret_msg());
			} else {
				// parse shards list from PB ClusterInfo
				const masterproto::ClusterInfo& clusterinfo = rsp.cluster_info();
				for (int i = 0; i < clusterinfo.rs_list_size(); ++i) {
					const masterproto::ReplicateSetInfo& rs = clusterinfo.rs_list(i);
					StatusWith<ShardType> shardRes = ShardType::fromPB(rs);
					if (!shardRes.isOK()) {
						return shardRes.getStatus();
					} else {
						LOG(1) << "get one shard from master/etcd:" << shardRes.getValue().toString();
						shards.push_back(shardRes.getValue());
					}
				}
			}
		}
	} catch (const std::exception& e) {
		LOG(0) << "query master got exception:" << e.what();
		return Status(ErrorCodes::InternalError, e.what());
	}

	// do remove
	bool hasShardRemoved = false;
	for (std::vector<ShardType>::iterator itOld = _allShards.begin(); itOld != _allShards.end();) {
		bool isThisShardRemoved = true;
		for (std::vector<ShardType>::iterator itNew = shards.begin(); itNew != shards.end(); ++itNew) {
			if (itOld->getName() == itNew->getName()) {
				isThisShardRemoved = false;
				break;
			}
		}
		if (isThisShardRemoved) {
			hasShardRemoved = true;
			/*
			 * NOTE(zhenyipeng) We should confirm that there is no table route to the removed shard
			 * But as the shard is already deleted by cmongo-oss, so we just assume the removed shard has no route to it. 
			*/
			const std::string removedSetName = itOld->getName();
			LOG(0) << "set:" << removedSetName << ",host:" << itOld->getHost() << " has been removed.";
			itOld = _allShards.erase(itOld);
			grid.shardRegistry()->remove(removedSetName);
			LOG(0) << "remove set:" << removedSetName << "from shardRegistry succeed";
			continue;
		}
		++itOld;
	}
	/*if (hasShardRemoved) {
		return Status(ErrorCodes::InternalError, "shard remove is not supported");
	}*/

	// do add
	bool hasShardAdded = false;
	for (std::vector<ShardType>::iterator itNew = shards.begin(); itNew != shards.end(); ++itNew) {
		bool isThisShardAdded = true;
		for (std::vector<ShardType>::iterator itOld = _allShards.begin(); itOld != _allShards.end(); ++itOld) {
			if (itNew->getName() == itOld->getName()) {
				isThisShardAdded = false;
				break;
			}
		}
		if (isThisShardAdded) {
			hasShardAdded = true;
			LOG(0) << "shard:" << itNew->getName() << ",host: " << itNew->getHost()
				<< " is newly added, but the db&route in new shard will be ignored";
			_allShards.push_back(*itNew);
		}
	}

	if (!hasShardRemoved && !hasShardAdded) {
		LOG(0) << "no shard is removed or added, skip.";
	}

	return Status::OK();
}

Status CatalogManagerCmongo::_initRoute() {
	if (!_isShardCluster) {
		return Status::OK();
	}
	masterproto::GetClusterRoutesRawReq req;
	masterproto::GetClusterRoutesRawRsp rsp;
	req.set_allocated_header(new cmongoproto::ReqHeader());
	req.set_cluster_id(_clusterId);
	req.set_time_nano(-1);
	try {
		LOG(1)<<"begin GetClusterRoutesRaw";
		grpc::Status s = _masterClient->GetClusterRoutesRaw(req, &rsp);
		if (!s.ok()) {
			LOG(0)<<"GetClusterRoutes failed:"<<int(s.error_code())<<":"<<s.error_message();
			return Status(ErrorCodes::InternalError, s.error_message());
		} else {
			// TODO(deyukong): invariant is not good here, since it depends on master
			// but master always sets header, I believe!
			invariant(rsp.has_header());
			if (rsp.header().ret_code() != 0) {
				LOG(0)<<"GetClusterRoutes failed:"
					  <<int(rsp.header().ret_code())
					  <<":"
					  <<rsp.header().ret_msg();
				return Status(ErrorCodes::InternalError, rsp.header().ret_msg());
			} else {
				return _parseFromPBClusterRoutesRaw(rsp);
			}
		}
	} catch (const std::exception& e) {
		LOG(0)<<"query master got exception:"<<e.what();
		return Status(ErrorCodes::InternalError, e.what());
	}

} 

Status CatalogManagerCmongo::_parseFromPBClusterInfo(const masterproto::GetClusterInfoRsp& pbobj) {
	stdx::lock_guard<stdx::mutex> lk(_mutex);
	std::vector<ShardType> shards;
	const masterproto::ClusterInfo& clusterinfo = pbobj.cluster_info();
	for (int i = 0; i < clusterinfo.rs_list_size(); ++i) {
		const masterproto::ReplicateSetInfo& rs = clusterinfo.rs_list(i);
		StatusWith<ShardType> shardRes = ShardType::fromPB(rs);
		if (!shardRes.isOK()) {
			return shardRes.getStatus();
		} else {
			shards.push_back(shardRes.getValue());
		}
	}
	_allShards = shards;
	return Status::OK();
}

Status CatalogManagerCmongo::syncRoutesRaw(OperationContext *txn, const masterproto::GetClusterRoutesRawRsp& data) {
	if (!_isShardCluster) {
		return Status(ErrorCodes::InvalidOptions, "cluster not sharded, no routes to sync");
	}
	return _parseFromPBClusterRoutesRaw(data);
}

Status CatalogManagerCmongo::syncRoutes(OperationContext *txn, const masterproto::GetClusterRoutesRsp& data) {
	if (!_isShardCluster) {
		return Status(ErrorCodes::InvalidOptions, "cluster not sharded, no routes to sync");
	}
	return _parseFromPBClusterRoutes(data);
}

Status CatalogManagerCmongo::createDatabase(OperationContext *txn, const std::string& dbName) {
	if (!_isShardCluster) {
		return Status::OK();
	}
	if (_dbs.find(dbName) != _dbs.end()) {
		return Status::OK();
	}
	return Status(ErrorCodes::InvalidOptions, "namespace not exist");
}

Status CatalogManagerCmongo::dropDatabase(OperationContext *txn, const std::string& dbName) {
	masterproto::DropDatabaseReq req;
	masterproto::DropDatabaseRsp rsp;
	req.set_allocated_header(new cmongoproto::ReqHeader());
	req.set_cluster_id(_clusterId);
	req.set_db_name(dbName);
	try {
		grpc::Status s = _masterClient->DropDatabase(req, &rsp);
		if (!s.ok()) {
			LOG(0)<<"DropDatabase failed:"<<int(s.error_code())<<":"<<s.error_message();
			return Status(ErrorCodes::InternalError, s.error_message());
		} else {
			// TODO(deyukong): invariant is not good here, since it depends on master
			// but master always sets header, I believe!
			invariant(rsp.has_header());
			if (rsp.header().ret_code() != 0) {
				LOG(0)<<"DropDatabase failed:"
					  <<int(rsp.header().ret_code())
					  <<":"
					  <<rsp.header().ret_msg();
				return Status(ErrorCodes::InternalError, rsp.header().ret_msg());
			} else {
				return Status::OK();
			}
		}
	} catch (const std::exception& e) {
		LOG(0)<<"query master got exception:"<<e.what();
		return Status(ErrorCodes::InternalError, e.what());
	}
}

Status CatalogManagerCmongo::enableSharding(OperationContext* txn, const std::string& dbName) {
    invariant(nsIsDbOnly(dbName));

	if (!_isShardCluster) {
		return Status(ErrorCodes::InvalidOptions, "cluster not sharded, can't exec this command.");
	}
    return Status::OK();
}

Status CatalogManagerCmongo::_parseFromPBClusterRoutesRaw(const masterproto::GetClusterRoutesRawRsp& pbobj) {
	LOG(1)<<"begin _parseFromPBClusterRoutesRaw";
	masterproto::GetClusterRoutesRsp rsp;
	for (int i = 0; i < pbobj.routes_size(); i++) {
		LOG(1)<<"begin RawRouteStringToPBBytes";
		StatusWith<std::string> s = cmongo::RawRouteStringToPBBytes(pbobj.routes(i).ns(), pbobj.routes(i).raw_route());
		if (!s.getStatus().isOK()) {
			LOG(0) << "ERROR: RawRouteStringToPBBytes failed:" << s.getStatus().reason();
			return s.getStatus();
		}
		masterproto::TableRoutes* routes = rsp.add_routes();
		bool ok = routes->ParseFromString(s.getValue());
		if (!ok) {
			LOG(0) << "ERROR: TableRoutesRaw parse failed, len:" << s.getValue().length();
			return Status(ErrorCodes::InternalError, "TableRoutesRaw parse failed");
		}
		LOG(1)<<"end RawRouteStringToPBBytes";
	}
	return _parseFromPBClusterRoutes(rsp);
}

Status CatalogManagerCmongo::_parseFromPBClusterRoutes(const masterproto::GetClusterRoutesRsp& pbobj) {
	LOG(1)<<"begin _parseFromPBClusterRoutes";
	stdx::lock_guard<stdx::mutex> lk(_mutex);
	// shards must be inited before routes
	invariant(_allShards.size() >= 1);

	// NOTE(deyukong): cmongo does not have the concept of primary shard
	// use shard with the smallest alphabet-order
	std::string primary_shard = _allShards[0].getName();
	for (auto& s : _allShards) {
		primary_shard = std::min(primary_shard, s.getName());
	}

	std::map<std::string, std::set<std::string>> dbTables;
	for (int i = 0; i < pbobj.routes_size(); i++) {
		const masterproto::TableRoutes& route = pbobj.routes(i);
		NamespaceString ns(route.ns());
		const std::string& dbname = ns.db().toString();
		dbTables[dbname].insert(route.ns());
	}

	// NOTE(deyukong): remove non-existing databases
	DBConfigMap::iterator it = _dbs.begin();
	while(it != _dbs.end()) {
		if (dbTables.find(it->first) != dbTables.end()) {
			it++;
		} else {
			LOG(0)<<"db:"<<it->first<<" dropped";
			it = _dbs.erase(it);
		}
	}

	// NOTE(deyukong): remove non-existing tables
	for (auto it: _dbs) {
		std::set<std::string> existing_tables;
		it.second->getAllShardedCollections(existing_tables);
		std::set<std::string> pending_delete_tables;
		for (auto e : existing_tables) {
			if (dbTables[it.first].find(e) == dbTables[it.first].end()) {
				pending_delete_tables.insert(e);
			}
		}
		for (auto e: pending_delete_tables) {
			Status s = it.second->removeTable(e);
			if (!s.isOK()) {
				return s;
			} 
		}
	}

	for (int i = 0; i < pbobj.routes_size(); i++) {
		const masterproto::TableRoutes& route = pbobj.routes(i);
		NamespaceString ns(route.ns());
		const std::string& dbname = ns.db().toString();
		if(_dbs.find(dbname) == _dbs.end()) {
			_dbs[dbname] = std::shared_ptr<DBConfigCMongo>(new DBConfigCMongo(dbname, primary_shard, _isShardCluster));
		} 
		Status s = _dbs[dbname]->parseTableFromPB(route);
		if (!s.isOK()) {
			return s;
		}
	}
	return Status::OK();
}

void CatalogManagerCmongo::shutDown(OperationContext* txn, bool allowNetworking) {
	LOG(1)<<"CatalogManagerCmongo::shutDown() called.";
	_inShutdown = true;
}

Status CatalogManagerCmongo::shardCollection(OperationContext* txn,
                           const std::string& ns,
                           const ShardKeyPattern& fieldsAndOrder,
                           bool unique,
                           const std::vector<BSONObj>& initPoints,
                           const std::set<ShardId>& initShardIds) {
	masterproto::CreateTableReq req;
	masterproto::CreateTableRsp rsp;
	req.set_allocated_header(new cmongoproto::ReqHeader());
	req.set_cluster_id(_clusterId);
	req.set_table_type("sharded");
	req.set_namespace_(ns);
	req.set_is_uniq_key(unique);
	const BSONObj& shardkeys = fieldsAndOrder.toBSON();
	BSONObjIterator patternIt(shardkeys);
	while (patternIt.more()) {
		BSONElement patternEl = patternIt.next();
		if (!patternEl.isNumber()) {
            return Status(ErrorCodes::InvalidOptions, "only support range index");
		}
		req.add_shard_keys(patternEl.fieldName());
	}

	auto allShards = getAllShards(txn);
	if (!allShards.getStatus().isOK()) {
		return allShards.getStatus();
	} else {
		std::vector<ShardType> v = allShards.getValue().value;
		for (auto& vv : v) {
			req.add_shard_list(vv.getName());
		}
	}
	try {
		grpc::Status s = _masterClient->CreateTable(req, &rsp);
		if (!s.ok()) {
			LOG(0)<<"shard collection failed:"<<int(s.error_code())<<":"<<s.error_message();
			return Status(ErrorCodes::InternalError, s.error_message());
		} else {
			// TODO(deyukong): invariant is not good here, since it depends on master
			// but master always sets header, I believe!
			invariant(rsp.has_header());
			if (rsp.header().ret_code() != 0) {
				LOG(0)<<"shard collection failed:"
					  <<int(rsp.header().ret_code())
					  <<":"
					  <<rsp.header().ret_msg();
				return Status(ErrorCodes::InternalError, rsp.header().ret_msg());
			} else {
				return Status::OK();
			}
		}
	} catch (const std::exception& e) {
		LOG(0)<<"query master got exception:"<<e.what();
		return Status(ErrorCodes::InternalError, e.what());
	}
	return Status::OK();
}

StatusWith<ShardDrainingStatus> CatalogManagerCmongo::removeShard(OperationContext* txn, const std::string& name) {
	return Status(ErrorCodes::InternalError, "not implemented");
}

StatusWith<OpTimePair<DatabaseType>> CatalogManagerCmongo::getDatabase(OperationContext* txn,
                                                     const std::string& dbName) {
	return Status(ErrorCodes::InternalError, "not implemented");
}

StatusWith<OpTimePair<CollectionType>> CatalogManagerCmongo::getCollection(OperationContext* txn,
                                                         const std::string& collNs) {
	return Status(ErrorCodes::InternalError, "not implemented");
}

Status CatalogManagerCmongo::getCollections(OperationContext* txn,
                          const std::string* dbName,
                          std::vector<CollectionType>* collections,
                          repl::OpTime* optime) {
	return Status(ErrorCodes::InternalError, "not implemented");
}

Status CatalogManagerCmongo::dropCollection(OperationContext* txn, const NamespaceString& ns) {
	masterproto::DropTableReq req;
	masterproto::DropTableRsp rsp;
	req.set_allocated_header(new cmongoproto::ReqHeader());
	req.set_cluster_id(_clusterId);
	req.set_namespace_(ns.ns());
	try {
		grpc::Status s = _masterClient->DropTable(req, &rsp);
		if (!s.ok()) {
			LOG(0)<<"DropTable failed:"<<int(s.error_code())<<":"<<s.error_message();
			return Status(ErrorCodes::InternalError, s.error_message());
		} else {
			// TODO(deyukong): invariant is not good here, since it depends on master
			// but master always sets header, I believe!
			invariant(rsp.has_header());
			if (rsp.header().ret_code() != 0) {
				LOG(0)<<"DropTable failed:"
					  <<int(rsp.header().ret_code())
					  <<":"
					  <<rsp.header().ret_msg();
				return Status(ErrorCodes::InternalError, rsp.header().ret_msg());
			} else {
				return Status::OK();
			}
		}
	} catch (const std::exception& e) {
		LOG(0)<<"query master got exception:"<<e.what();
		return Status(ErrorCodes::InternalError, e.what());
	}
}

Status CatalogManagerCmongo::getDatabasesForShard(OperationContext* txn,
                                const std::string& shardName,
                                std::vector<std::string>* dbs) {
	return Status(ErrorCodes::InternalError, "not implemented");
}

Status CatalogManagerCmongo::getChunks(OperationContext* txn,
                     const BSONObj& query,
                     const BSONObj& sort,
                     boost::optional<int> limit,
                     std::vector<ChunkType>* chunks,
                     repl::OpTime* opTime) {
	return Status(ErrorCodes::InternalError, "not implemented");
}

Status CatalogManagerCmongo::getTagsForCollection(OperationContext* txn,
                                const std::string& collectionNs,
                                std::vector<TagsType>* tags) {
	return Status(ErrorCodes::InternalError, "not implemented");
}

StatusWith<std::string> CatalogManagerCmongo::getTagForChunk(OperationContext* txn,
                                           const std::string& collectionNs,
                                           const ChunkType& chunk) {
	return Status(ErrorCodes::InternalError, "not implemented");
}

StatusWith<OpTimePair<std::vector<ShardType>>> CatalogManagerCmongo::getAllShards(OperationContext* txn) {
	stdx::lock_guard<stdx::mutex> lk(_mutex);
	// NOTE(deyukong): make a deepcopy here
	// because I'm not quite sure whether it is by reference or by value here.
	std::vector<ShardType> shards = _allShards;
    return OpTimePair<std::vector<ShardType>>{shards};
}

bool CatalogManagerCmongo::runUserManagementWriteCommand(OperationContext* txn,
                                       const std::string& commandName,
                                       const std::string& dbname,
                                       const BSONObj& cmdObj,
                                       BSONObjBuilder* result) {
	LOG(0)<<"ERROR:CatalogManagerCmongo::runUserManagementWriteCommand called"; 
	return false;
}

bool CatalogManagerCmongo::runUserManagementReadCommand(OperationContext* txn,
                                      const std::string& dbname,
                                      const BSONObj& cmdObj,
                                      BSONObjBuilder* result) {
    return _runReadCommand(txn, dbname, cmdObj, result);
}

Status CatalogManagerCmongo::applyChunkOpsDeprecated(OperationContext* txn,
                                   const BSONArray& updateOps,
                                   const BSONArray& preCondition,
                                   const std::string& nss,
                                   const ChunkVersion& lastChunkVersion) {
	LOG(0)<<"ERROR: CatalogManagerCmongo::applyChunkOpsDeprecated called";
	return Status(ErrorCodes::InternalError, "not implemented");
}

StatusWith<SettingsType> CatalogManagerCmongo::getGlobalSettings(OperationContext* txn,
                                               const std::string& key) {
	LOG(0)<<"ERROR: CatalogManagerCmongo::getGlobalSettings called";
	return Status(ErrorCodes::InternalError, "not implemented");
}

void CatalogManagerCmongo::writeConfigServerDirect(OperationContext* txn,
                                 const BatchedCommandRequest& request,
                                 BatchedCommandResponse* response) {
	LOG(0)<<"ERROR: CatalogManagerCmongo::writeConfigServerDirect called";
	invariant(0);
}

Status CatalogManagerCmongo::insertConfigDocument(OperationContext* txn,
                                const std::string& ns,
                                const BSONObj& doc) {
	LOG(0)<<"ERROR: CatalogManagerCmongo::insertConfigDocument called";
	invariant(0);
	return Status(ErrorCodes::InternalError, "not implemented");
}

StatusWith<bool> CatalogManagerCmongo::updateConfigDocument(OperationContext* txn,
                                          const std::string& ns,
                                          const BSONObj& query,
                                          const BSONObj& update,
                                          bool upsert) {
	LOG(0)<<"ERROR: CatalogManagerCmongo::updateConfigDocument called";
	invariant(0);
	return Status(ErrorCodes::InternalError, "not implemented");
}

Status CatalogManagerCmongo::removeConfigDocuments(OperationContext* txn,
                                 const std::string& ns,
                                 const BSONObj& query) {
	LOG(0)<<"ERROR: CatalogManagerCmongo::removeConfigDocuments called";
	invariant(0);
	return Status(ErrorCodes::InternalError, "not implemented");
}

DistLockManager* CatalogManagerCmongo::getDistLockManager() {
	return nullptr;
}

Status CatalogManagerCmongo::initConfigVersion(OperationContext* txn) {
	// NOTE(deyukong): called when mongos starts, have to return a OK here
	return Status::OK();
}

Status CatalogManagerCmongo::appendInfoForConfigServerDatabases(OperationContext* txn,
                                              BSONArrayBuilder* builder) {
	// NOTE(deyukong): called when cmd.listDatabases is called
    // and dbs on configSvr is appended, here we just ignore
	return Status::OK();
}

bool CatalogManagerCmongo::isMetadataConsistentFromLastCheck(OperationContext* txn) {
	return true;
}

Status CatalogManagerCmongo::_checkDbDoesNotExist(OperationContext* txn,
                                const std::string& dbName,
                                DatabaseType* db) {
	LOG(0)<<"ERROR: CatalogManagerCmongo::_checkDbDoesNotExist called";
	invariant(0);
	return Status(ErrorCodes::InternalError, "not implemented");
}

StatusWith<std::string> CatalogManagerCmongo::_generateNewShardName(OperationContext* txn) {
	LOG(0)<<"ERROR: CatalogManagerCmongo::_generateNewShardName called";
	invariant(0);
	return Status(ErrorCodes::InternalError, "not implemented");
}

Status CatalogManagerCmongo::_createCappedConfigCollection(OperationContext* txn,
                                         StringData collName,
                                         int cappedSize) {
	LOG(0)<<"ERROR: CatalogManagerCmongo::_createCappedConfigCollection called";
	invariant(0);
	return Status(ErrorCodes::InternalError, "not implemented");
}

Status CatalogManagerCmongo::_startConfigServerChecker() {
	LOG(0)<<"ERROR: CatalogManagerCmongo::_startConfigServerChecker called";
	invariant(0);
	return Status(ErrorCodes::InternalError, "not implemented");
}

size_t CatalogManagerCmongo::_getShardCount(const BSONObj& query) const {
	LOG(0)<<"ERROR: CatalogManagerCmongo::_getShardCount called";
	invariant(0);
	return 0;
}

Status CatalogManagerCmongo::_checkConfigServersConsistent(const unsigned tries) const {
	return Status::OK();
}

void CatalogManagerCmongo::_consistencyChecker() {
	return;
}

bool CatalogManagerCmongo::_runReadCommand(OperationContext* txn,
                         const std::string& dbname,
                         const BSONObj& cmdObj,
                         BSONObjBuilder* result) {
	LOG(1)<<"_runReadCommand on shards as configSvrs:" << "db:" << dbname << "cmd:" << cmdObj;
	std::vector<ShardType> allshards;
    stdx::unique_lock<stdx::mutex> lk(_mutex);
	allshards = _allShards;
	lk.unlock();
	for (size_t i = 0; i < allshards.size(); ++i) {
		try {
			StatusWith<ConnectionString> s = ConnectionString::parse(allshards[i].getHost());
			invariant(s.getStatus().isOK());
			ScopedDbConnection conn(s.getValue(), 5);
			BSONObj cmdResult;
			const bool ok = conn->runCommand(dbname, cmdObj, cmdResult);
			result->appendElements(cmdResult);
			LOG(1)<<"_runReadCommand on:" << allshards[i].getHost() << " ok:" << ok << " result:" << cmdResult;
			conn.done();
			return ok;
		} catch (const DBException& ex) {
			LOG(0) << "WARN:_runReadCommand got exception" << ex;
			if(ShardRegistry::kAllRetriableErrors.count(ErrorCodes::fromInt(ex.getCode()))) {
				continue;
			}
		}
	}
	return false;
}

StatusWith<std::vector<BSONObj>> CatalogManagerCmongo::_findOnConfig(const std::string& ns,
                                                   const Query& query,
                                                   int limit) {
	LOG(0)<<"ERROR: CatalogManagerCmongo::_findOnConfig called";
	invariant(0);
	return Status(ErrorCodes::InternalError, "not implemented");
}

StatusWith<BSONObj> CatalogManagerCmongo::_findOneOnConfig(const std::string& ns,
					const Query& query) {
	LOG(0)<<"ERROR: CatalogManagerCmongo::_findOneOnConfig called";
	invariant(0);
	return Status(ErrorCodes::InternalError, "not implemented");
}

StatusWith<std::shared_ptr<DBConfig>> CatalogManagerCmongo::getDbConfig(OperationContext *txn, const std::string& dbname) {
	stdx::lock_guard<stdx::mutex> lk(_mutex);
	if (!_isShardCluster) {
		invariant(_allShards.size() == 1);
		std::shared_ptr<DBConfig> ret = std::shared_ptr<DBConfigCMongo>(new DBConfigCMongo(dbname, _allShards[0].getName(), _isShardCluster));
		return ret;
	}
	if (_dbs.find(dbname) == _dbs.end()) {
		return {ErrorCodes::NamespaceNotFound, str::stream() << "database "<< dbname << " not found"};
	}
	std::shared_ptr<DBConfig> ret = _dbs[dbname];
	return ret;

}
}
