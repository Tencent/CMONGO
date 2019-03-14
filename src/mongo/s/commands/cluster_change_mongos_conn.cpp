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

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kCommand

#include "mongo/platform/basic.h"

#include <list>
#include <set>
#include <vector>

#include "mongo/client/connpool.h"
#include "mongo/db/audit.h"
#include "mongo/db/auth/action_set.h"
#include "mongo/db/auth/action_type.h"
#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/client_basic.h"
#include "mongo/client/global_conn_pool.h"
#include "mongo/db/commands.h"
#include "mongo/db/hasher.h"
#include "mongo/db/write_concern_options.h"
#include "mongo/s/catalog/catalog_cache.h"
#include "mongo/s/catalog/catalog_manager.h"
#include "mongo/s/chunk_manager.h"
#include "mongo/s/client/shard_registry.h"
#include "mongo/s/client/shard_connection.h"
#include "mongo/s/cluster_write.h"
#include "mongo/s/config.h"
#include "mongo/s/grid.h"
#include "mongo/util/log.h"

namespace mongo {

class ChangeMongosConnCmd: public Command {
public:
    ChangeMongosConnCmd() : Command("changeMongosConn") {}

    virtual bool slaveOk() const {
        return true;
    }

    virtual bool adminOnly() const {
        return true;
    }

    virtual bool isWriteCommandForConfigServer() const {
        return false;
    }

    virtual void help(std::stringstream& help) const {
		help << "tune mongos->mongod connection params";
    }

    virtual Status checkAuthForCommand(ClientBasic* client,
                                       const std::string& dbname,
                                       const BSONObj& cmdObj) {
		// NOTE(deyukong): no privileges needed for proxyStats
		return Status::OK();
    }

	virtual bool run(OperationContext* txn,
					const std::string& dbname,
					BSONObj& cmdObj,
					int options,
					std::string& errmsg,
					BSONObjBuilder& result) {
		// currently only support minConnections/maxConnections tunning
		executor::ConnectionPool::Options opts;
		bool enableOverload = false;
		if(cmdObj["minAsioConnections"].eoo() || !cmdObj["minAsioConnections"].isNumber()) {
			opts.minConnections = 0;
		} else {
			opts.minConnections = cmdObj["minAsioConnections"].numberInt();
		}
		if (cmdObj["maxAsioConnections"].eoo() || !cmdObj["maxAsioConnections"].isNumber()) {
			opts.maxConnections = 0;
		} else {
			opts.maxConnections = cmdObj["maxAsioConnections"].numberInt();
		}
		if (cmdObj["enableOverload"].eoo() || !cmdObj["enableOverload"].isBoolean()) {
		    enableOverload = false;
		} else {
		    enableOverload = cmdObj["enableOverload"].boolean();
		}
		opts.overloadConfig.setEnableOverload(enableOverload);
        globalConnPool.setEnableOverload(enableOverload);
        shardConnectionPool.setEnableOverload(enableOverload);
		if (opts.overloadConfig.isOverloadEnabled()) {
		    // If overload is enabled, check overload settings
		    if (cmdObj["overloadAsioQueueSize"].eoo() || !cmdObj["overloadAsioQueueSize"].isNumber()) {
                opts.overloadConfig.setOverloadQueueSize(0);
            } else {
                opts.overloadConfig.setOverloadQueueSize(cmdObj["overloadAsioQueueSize"].numberInt());
            }
		    if (cmdObj["overloadAsioPoolSize"].eoo() || !cmdObj["overloadAsioPoolSize"].isNumber()) {
		        opts.overloadConfig.setOverloadInUsePoolSize(0);
		    } else {
		        opts.overloadConfig.setOverloadInUsePoolSize(cmdObj["overloadAsioPoolSize"].numberInt());
		    }
		    if (cmdObj["overloadAutoResizing"].eoo() || !cmdObj["overloadAutoResizing"].isBoolean()) {
		        opts.overloadConfig.setOverloadResizing(true);
		        globalConnPool.setOverloadResizing(true);
		        shardConnectionPool.setOverloadResizing(true);
		    } else {
		        bool enableResizing = cmdObj["overloadAutoResizing"].boolean();
		        opts.overloadConfig.setOverloadResizing(enableResizing);
		        globalConnPool.setOverloadResizing(enableResizing);
		        shardConnectionPool.setOverloadResizing(enableResizing);
		    }
		}
		Status s = grid.shardRegistry()->updateConnPoolOptions(opts);
		if (!s.isOK()) {
			LOG(0) << "ERROR: change mongos asio conn:" << cmdObj << " failed:" << s.reason();
			return appendCommandStatus(result, s);
		}
		if (!cmdObj["maxshardConnectionPoolSize"].eoo()) {
			int maxshardConnectionPoolSize = cmdObj["maxshardConnectionPoolSize"].numberInt();
			int oldSize = shardConnectionPool.getMaxPoolSize();
			if (oldSize != maxshardConnectionPoolSize) {
				LOG(0) << "shardConnectionPool size change from:"
						<< oldSize << " to:" << maxshardConnectionPoolSize;
				shardConnectionPool.setMaxPoolSize(maxshardConnectionPoolSize);
			}
		}
		if (!cmdObj["maxGlobalConnectionPoolSize"].eoo()) {
			int maxGlobalConnectionPoolSize = cmdObj["maxGlobalConnectionPoolSize"].numberInt();
			int oldSize = globalConnPool.getMaxPoolSize();
			if (oldSize != maxGlobalConnectionPoolSize) {
				LOG(0) << "globalConnectionPool size change from:"
						<< oldSize << " to:" << maxGlobalConnectionPoolSize;
				globalConnPool.setMaxPoolSize(maxGlobalConnectionPoolSize);
			}
		}
		if (enableOverload) {
		    // If overload is enabled, check the overload settings
		    if (!cmdObj["overloadGlobalQueueSize"].eoo()) {
		        int overloadGlobalQueueSize = cmdObj["overloadGlobalQueueSize"].numberInt();
		        if (overloadGlobalQueueSize <= 0 ||
		                (overloadGlobalQueueSize > globalConnPool.getNormalQueueSize() &&
		                        globalConnPool.getNormalQueueSize() > 0)) {
		            overloadGlobalQueueSize = globalConnPool.getNormalQueueSize();
		        }
		        if (globalConnPool.getOverloadQueueSize() != overloadGlobalQueueSize) {
		            LOG(0) << "overloadGlobalQueueSize change from:"
		                   << globalConnPool.getOverloadQueueSize() << " to: " << overloadGlobalQueueSize;
		            globalConnPool.setOverloadQueueSize(overloadGlobalQueueSize);
		        }
		    }
		    if (!cmdObj["overloadShardQueueSize"].eoo()) {
                int overloadShardQueueSize = cmdObj["overloadShardQueueSize"].numberInt();
                if (overloadShardQueueSize <= 0 ||
                        (overloadShardQueueSize > shardConnectionPool.getNormalQueueSize() &&
                                shardConnectionPool.getNormalQueueSize() > 0)) {
                    overloadShardQueueSize = shardConnectionPool.getNormalQueueSize();
                }
                if (shardConnectionPool.getOverloadQueueSize() != overloadShardQueueSize) {
                    LOG(0) << "overloadShardQueueSize change from:"
                           << shardConnectionPool.getOverloadQueueSize() << " to: " << overloadShardQueueSize;
                    shardConnectionPool.setOverloadQueueSize(overloadShardQueueSize);
                }
            }
		    if (!cmdObj["overloadGlobalPoolSize"].eoo()) {
		        int overloadGlobalPoolSize = cmdObj["overloadGlobalPoolSize"].numberInt();
		        if (globalConnPool.getOverloadInUsePoolSize() != overloadGlobalPoolSize) {
		            LOG(0) << "overloadGlobalPoolSize change from:"
		                   << globalConnPool.getOverloadInUsePoolSize() << " to: " << overloadGlobalPoolSize;
		            globalConnPool.setOverloadInUsePoolSize(overloadGlobalPoolSize);
		        }
		    }
            if (!cmdObj["overloadShardPoolSize"].eoo()) {
                int overloadShardPoolSize = cmdObj["overloadShardPoolSize"].numberInt();
                if (shardConnectionPool.getOverloadInUsePoolSize() != overloadShardPoolSize) {
                    LOG(0) << "overloadShardPoolSize change from:"
                           << shardConnectionPool.getOverloadInUsePoolSize() << " to: " << overloadShardPoolSize;
                    shardConnectionPool.setOverloadInUsePoolSize(overloadShardPoolSize);
                }
            }
		}
		return true;
	}
} changeMongosConnCmd;
}
