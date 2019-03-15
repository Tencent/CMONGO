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

#include "mongo/db/commands.h"
#include "mongo/util/log.h"
#include "mongo/util/net/listen.h"

namespace mongo {
class MongosLimitConns: public Command {
public:
	MongosLimitConns(): Command("mongosLimitConns", false) {}

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
        help << "limit mongos conns";
    }

    Status checkAuthForCommand(ClientBasic* client,
                               const std::string& dbname,
                               const BSONObj& cmdObj) final {
		return Status::OK();
    }

    virtual bool run(OperationContext* txn,
                     const std::string& dbname,
                     BSONObj& cmdObj,
                     int options,
                     std::string& errmsg,
                     BSONObjBuilder& result) {
        LOG(1) << "mongosLimitConns begin";
		if (!cmdObj["size"].isNumber()) {
            return appendCommandStatus(result, Status(ErrorCodes::InvalidOptions, "invalid size field, size should be a number"));
		}
        if (!cmdObj["token"].isNumber() || cmdObj["token"].numberLong() != TOKEN) {
            return appendCommandStatus(result, Status(ErrorCodes::InvalidOptions, "invalid parameter"));
        }
		long long size = cmdObj["size"].numberLong();
		if (size == 0) {
			Listener::globalTicketHolder.resizeToCurrentUsed();
		} else if (size <= Listener::globalTicketHolder.used()) {
            return appendCommandStatus(result, Status(ErrorCodes::InvalidOptions, "not allowed to set to size smaller than used"));
		} else {
			Listener::globalTicketHolder.resize(size);
		}
        LOG(1) << "mongosLimitConns success, currentSize:" << size;
        return appendCommandStatus(result, Status::OK());
	}
} mongosLimitConns;
}
