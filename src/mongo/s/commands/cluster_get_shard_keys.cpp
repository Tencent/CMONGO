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

#include <string>

#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/commands.h"
#include "mongo/s/grid.h"
#include "mongo/s/catalog/catalog_cache.h"
#include "mongo/s/chunk_manager.h"
#include "mongo/util/flow_manager.h"
#include "mongo/util/log.h"

using std::string;

namespace mongo {
class GetShardKeysCmd: public Command {
public:
    GetShardKeysCmd(): Command("getShardKeys", false) {}

    virtual bool isWriteCommandForConfigServer() const {
        return false;
    }

    virtual bool slaveOk() const {
        return true;
    }

    virtual bool adminOnly() const {
        return false;
    }

    virtual void help(std::stringstream& help) const {
        help << "get shard keys of collection";
    }

    Status checkAuthForCommand(ClientBasic* client,
                               const std::string& dbname,
                               const BSONObj& cmdObj) final {
        NamespaceString nss(parseNs(dbname, cmdObj));
        return AuthorizationSession::get(client)->checkAuthForFind(nss, false);
    }

    virtual bool run(OperationContext* txn,
                     const std::string& dbname,
                     BSONObj& cmdObj,
                     int options,
                     std::string& errmsg,
                     BSONObjBuilder& result) {
        if (dbname == "admin" || dbname == "local") {
            return appendCommandStatus(result,
                    Status(ErrorCodes::InvalidNamespace,
                           "do not allow getShardKeys on admin or local"));
        }

        const string ns = parseNs(dbname, cmdObj);
        const NamespaceString nss(ns);
        if (!nss.isValid()) {
            return appendCommandStatus(result,
                    Status{ErrorCodes::InvalidNamespace,
                           str::stream() << "Invalid collection name: " << nss.ns()});
        }

        auto confStatus = grid.catalogCache()->getDatabase(txn, dbname);
        if (confStatus.getStatus() == ErrorCodes::NamespaceNotFound) {
            // If the database doesn't exist, we successfully return an empty result
            return true;
        } else if (!confStatus.isOK()) {
            return appendCommandStatus(result, confStatus.getStatus());
        }

        auto conf = confStatus.getValue();
        if (!conf->isShardingEnabled() || !conf->isSharded(ns)) {
            // If the collection doesn't exist or is not sharded, we successfully return an empty result
            return true;
        }

        auto chunkMgr = conf->getChunkManager(txn, ns);
        auto shardKeys = chunkMgr->getShardKeyPattern().getKeyPattern().toBSON();
        result.append("shardKeys", shardKeys);
        return true;
    }
} getShardKeys;
}
