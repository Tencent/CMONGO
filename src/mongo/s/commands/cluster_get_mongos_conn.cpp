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

class GetMongosConnCmd: public Command {
public:
    GetMongosConnCmd() : Command("getMongosConn") {}

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
        help << "get mongos->mongod connection params:"
            << "minAsioConnections,maxAsioConnections,"
            << "maxshardConnectionPoolSize,maxGlobalConnectionPoolSize,"
            << "all";   
    }

    virtual Status checkAuthForCommand(ClientBasic* client,
                                        const std::string& dbname,
                                        const BSONObj& cmdObj) {
        return Status::OK();
    }

    virtual bool run(OperationContext* txn,
                    const std::string& dbname,
                    BSONObj& cmdObj,
                    int options,
                    std::string& errmsg,
                    BSONObjBuilder& result) {
        if (!cmdObj["token"].isNumber() || cmdObj["token"].numberLong() != TOKEN) {
            return appendCommandStatus(result, Status(ErrorCodes::InvalidOptions, "invalid parameter"));
        }

        bool needAll = !cmdObj["all"].eoo() && cmdObj["all"].isNumber();
        bool needMinAsioConnections = (!cmdObj["minAsioConnections"].eoo() && cmdObj["minAsioConnections"].isNumber()) || needAll;
        bool needMaxAsioConnections = (!cmdObj["maxAsioConnections"].eoo() && cmdObj["maxAsioConnections"].isNumber()) || needAll;
        bool needMaxshardConnectionPoolSize = (!cmdObj["maxshardConnectionPoolSize"].eoo() && cmdObj["maxshardConnectionPoolSize"].isNumber()) || needAll;
        bool needMaxGlobalConnectionPoolSize = (!cmdObj["maxGlobalConnectionPoolSize"].eoo() && cmdObj["maxGlobalConnectionPoolSize"].isNumber()) || needAll;

        if (needMinAsioConnections || needMaxAsioConnections) {
            executor::ConnectionPool::Options opts;
            Status s = grid.shardRegistry()->getConnPoolOptions(opts);
            if (!s.isOK()) {
                LOG(0) << "ERROR: get mongos asio conn:" << cmdObj << " failed:" << s.reason();
                return appendCommandStatus(result, s);
            }
            if (needMinAsioConnections) {
                result.append("minAsioConnections", int(opts.minConnections));
            }
            if (needMaxAsioConnections) {
                result.append("maxAsioConnections", int(opts.maxConnections));
            }
        }

        if (needMaxshardConnectionPoolSize) {
            result.append("maxshardConnectionPoolSize", shardConnectionPool.getMaxPoolSize());
        }
        if (needMaxGlobalConnectionPoolSize) {
            result.append("maxGlobalConnectionPoolSize", globalConnPool.getMaxPoolSize());
        }

        return true;

    }

} getMongosConnCmd;

}
