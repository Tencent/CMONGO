// resize_oplog.cpp
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

#include <string>
#include <vector>

#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/auth/authorization_manager_global.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/catalog/apply_ops.h"
#include "mongo/db/client.h"
#include "mongo/db/commands.h"
#include "mongo/db/commands/dbhash.h"
#include "mongo/db/concurrency/write_conflict_exception.h"
#include "mongo/db/db_raii.h"
#include "mongo/db/dbdirectclient.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/matcher/matcher.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/repl/oplog.h"
#include "mongo/db/repl/repl_client_info.h"
#include "mongo/db/repl/replication_coordinator_global.h"
#include "mongo/db/catalog/collection.h"
#include "mongo/db/catalog/collection_catalog_entry.h"
#include "mongo/db/service_context.h"
#include "mongo/util/log.h"
#include "mongo/util/scopeguard.h"
#include "mongo/base/status.h"

namespace mongo {

using std::string;
using std::stringstream;

class CmdReplSetResizeOplog: public Command {
public:
    virtual bool slaveOk() const {
        return true;
    }
    virtual bool isWriteCommandForConfigServer() const {
		return false;
	}
    virtual void help(stringstream& help) const {
        help << "resize oplog size, support wiredTiger and rocks";
    }

    Status checkAuthForCommand(ClientBasic* client,
                               const std::string& dbname,
                               const BSONObj& cmdObj) override {
        bool isAuthorized = AuthorizationSession::get(client)->isAuthorizedForActionsOnResource(
            ResourcePattern::forClusterResource(), ActionType::replSetResizeOplog);

        return isAuthorized ? Status::OK() : Status(ErrorCodes::Unauthorized, "Unauthorized");
    }

    CmdReplSetResizeOplog() : Command("replSetResizeOplog") {}
    bool run(OperationContext* txn,
            const string& dbname,
            BSONObj& jsobj,
            int,
            string& errmsg,
            BSONObjBuilder& result) {
        StringData dbName("local");
        ScopedTransaction transaction(txn, MODE_IX);
        AutoGetDb autoDb(txn, dbName, MODE_X);
        Database* const db = autoDb.getDb();
        Collection* coll = db ? db->getCollection("local.oplog.rs") : nullptr;
        if (!coll) {
            return appendCommandStatus(result, Status(ErrorCodes::NamespaceNotFound, "ns does not exist"));
        }
        if (!coll->isCapped()) {
            return appendCommandStatus(result, Status(ErrorCodes::InternalError, "ns does not exist"));
        }
        if (!jsobj["size"].isNumber()) {
            return appendCommandStatus(result, Status(ErrorCodes::InvalidOptions, "invalid size field, size should be a number"));
        }

        long long size = jsobj["size"].numberLong();
            WriteUnitOfWork wunit(txn);
        Status status = coll->getRecordStore()->updateCappedSize(txn, size);
        if (!status.isOK()) {
            return appendCommandStatus(result, status);
        }
        CollectionCatalogEntry* entry = coll->getCatalogEntry();
        entry->updateCappedSize(txn, size);
        wunit.commit();
        LOG(0) << "resizeOplog success, currentSize:" << size;
        return appendCommandStatus(result, Status::OK());
    }
} cmdReplSetResizeOplog;
}
