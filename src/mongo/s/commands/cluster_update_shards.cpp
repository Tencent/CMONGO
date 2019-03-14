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
class UpdateShardsCmd: public Command {
public:
    UpdateShardsCmd() : Command("updateShards") {}

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
        help << "update shards list from etcd";   
    }

    virtual Status checkAuthForCommand(ClientBasic* client,
                                        const std::string& dbname,
                                        const BSONObj& cmdObj) {
        return Status::OK();
    }

    //NOTE(zhenyipeng) Update shards list from etcd, includes remove and add shards
    virtual bool run(OperationContext* txn,
                    const std::string& dbname,
                    BSONObj& cmdObj,
                    int options,
                    std::string& errmsg,
                    BSONObjBuilder& result) {
        
        log() << "begin to update shard list from master/ectd";
        Status updateShardsResult = grid.catalogManager(txn)->updateShards(txn);
        if (!updateShardsResult.isOK()) {
            log() << "updateShards command failed, " << updateShardsResult.reason();
            return appendCommandStatus(result, updateShardsResult);
        }
        log() << "update shard list from master/etcd succeed";
        //result << "shardsUpdated" << updateShardsResult.reason();
        return true;
    }
} updateShardsCmd;
}
