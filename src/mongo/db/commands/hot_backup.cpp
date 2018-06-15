// hot_backup.cpp

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
#include "mongo/db/service_context.h"
#include "mongo/util/log.h"
#include "mongo/util/scopeguard.h"
#include "mongo/db/storage/storage_engine.h"
#include "mongo/util/time_support.h"

namespace mongo {

using std::string;
using std::stringstream;

class CmdHotBackup : public Command  {
public:
    virtual bool slaveOk() const {
        return true;
    }
    virtual bool isWriteCommandForConfigServer() const {
		return false;
	}
    virtual void help(stringstream& help) const {
        help << "hot backup with checkpoint, only support wiredTiger";
    }
    virtual void addRequiredPrivileges(const std::string& dbname,
                                       const BSONObj& cmdObj,
                                       std::vector<Privilege>* out) {
	}

    CmdHotBackup() : Command("hotBackup") {}
    bool run(OperationContext* txn,
            const string& dbname,
            BSONObj& jsobj,
            int,
            string& errmsg,
            BSONObjBuilder& result) {
        Status status = Status::OK();
        string stage = jsobj["stage"].String();
        int64_t token = jsobj["token"].numberLong();
        StorageEngine* storageEngine = getGlobalServiceContext()->getGlobalStorageEngine();
        
        if(stage == "begin") {
			ScopedTransaction transaction(txn, MODE_X);
			Lock::GlobalWrite global(txn->lockState());
			try {
				// NOTE(deyukong): thie ensures the count and datasize
				// synced and can be seen by backup checkpoint
				storageEngine->flushAllFiles(true);
			} catch (std::exception& e) {
				LOG(0) << "error doing flushAll: " << e.what();
				return appendCommandStatus(result, {ErrorCodes::InternalError,
						str::stream() << "flushAllFiles failed:" << e.what()});
			}
			auto res = storageEngine->beginHotBackup(txn);
			if(!res.isOK()) {
				return appendCommandStatus(result, res.getStatus());
			}

			result.appendElements(res.getValue());
			return true;
        } else if(stage == "continue") {
			auto status = storageEngine->continueHotBackup(txn, token);
			return appendCommandStatus(result, status);
        } else if(stage == "end") {
			storageEngine->endHotBackup(txn, token);
			return appendCommandStatus(result, Status::OK());
        } else {
			LOG(0) <<  "invaild type: " << stage;       
        }

        return appendCommandStatus(result, {ErrorCodes::FailedToParse,
                                            str::stream() << "stage field parse with invaild type"});
    }
} cmdHotBackup;
}
