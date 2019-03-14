// immutable_keys.cpp
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
#include "mongo/db/cmongo/cmongo_shard.h"

namespace mongo {

using std::string;
using std::stringstream;

class CmdRegistImmutableKey: public Command {
public:
    virtual bool slaveOk() const {
        return false;
    }
    virtual bool isWriteCommandForConfigServer() const {
		return false;
	}
    virtual void help(stringstream& help) const {
        help << "regist cmongo shardkeys";
    }
    virtual void addRequiredPrivileges(const std::string& dbname,
                                       const BSONObj& cmdObj,
                                       std::vector<Privilege>* out) {
		// TODO(deyukong): implements
		// currently, no privilege is required
	}
    CmdRegistImmutableKey() : Command("regImmutableKey") {}
    bool run(OperationContext* txn,
            const string& dbname,
            BSONObj& jsobj,
            int,
            string& errmsg,
            BSONObjBuilder& result) {

		bool ok = getCMongoShardingState()->insert(txn, jsobj["ns"].String(), jsobj["key"].Obj());
		if (!ok) {
			return appendCommandStatus(result, Status(ErrorCodes::InternalError, "cmongoshard insert record failed"));
		}
		return appendCommandStatus(result, Status::OK());
    }
} cmdRegistImmutableKey;

class CmdUnRegistImmutableKey: public Command {
public:
	virtual bool slaveOk() const {
		return false;
	}
	virtual bool isWriteCommandForConfigServer() const {
		return false;
	}
	virtual void help(stringstream& help) const {
		help << "unregist cmongo shardkeys";
	}
    virtual void addRequiredPrivileges(const std::string& dbname,
                                       const BSONObj& cmdObj,
                                       std::vector<Privilege>* out) {
		// TODO(deyukong): implements
		// currently, no privilege is required
	}
    CmdUnRegistImmutableKey() : Command("unregImmutableKey") {}
    bool run(OperationContext* txn,
            const string& dbname,
            BSONObj& jsobj,
            int,
            string& errmsg,
            BSONObjBuilder& result) {

		bool ok = getCMongoShardingState()->remove(txn, jsobj["ns"].String());
		if (!ok) {
			return appendCommandStatus(result, Status(ErrorCodes::InternalError, "cmongoshard remove record failed"));
		}
		return appendCommandStatus(result, Status::OK());
    }
} cmdUnRegistImmutableKey;
}
