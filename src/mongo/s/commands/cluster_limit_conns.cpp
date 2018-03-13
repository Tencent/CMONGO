// cluster_limit_conns.cpp

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
#include "mongo/transport/service_entry_point_impl.h"

namespace mongo {
class MongosLimitConns: public BasicCommand {
public:
    MongosLimitConns(): BasicCommand("mongosLimitConns") {}

    virtual bool slaveOk() const {
        return true;
    }

    virtual bool adminOnly() const {
        return true;
    }

    virtual bool supportsWriteConcern(const BSONObj& cmd) const override {
        return false;
    }

    AllowedOnSecondary secondaryAllowed(ServiceContext*) const final {
        return AllowedOnSecondary::kAlways;
    }

    virtual bool isWriteCommandForConfigServer() const {
        return false;
    }

    std::string help() const final {
        return "limit mongos conns";
    }

    Status checkAuthForCommand(Client* client,
                               const std::string& dbname,
                               const BSONObj& cmdObj) const final {
        return Status::OK();
    }   

    virtual bool run(OperationContext* opCtx,
                     const std::string& dbname,
                     const BSONObj& cmdObj,
                     BSONObjBuilder& result) {
        LOG(1) << "mongosLimitConns begin";
		auto serviceEntryPoint = opCtx->getServiceContext()->getServiceEntryPoint();
		invariant(serviceEntryPoint);
		auto entry_impl = dynamic_cast<ServiceEntryPointImpl*>(serviceEntryPoint);
		invariant(entry_impl);
        if (!cmdObj["size"].isNumber()) {
            return CommandHelpers::appendCommandStatus(result, Status(ErrorCodes::InvalidOptions, "invalid size field, size should be a number"));
        }   
        size_t size = static_cast<size_t>(cmdObj["size"].numberLong());
        if (size == 0) {
			entry_impl->resizeMaxConnToCurrentUsed();
        } else if (size <= entry_impl->numOpenSessions()) {
            return CommandHelpers::appendCommandStatus(result, Status(ErrorCodes::InvalidOptions, "not allowed to set to size smaller than used"));
        } else {
			entry_impl->resizeMaxConns(size);
        }   
        LOG(1) << "mongosLimitConns success, currentSize:" << entry_impl->numOpenSessions();
        return CommandHelpers::appendCommandStatus(result, Status::OK());
    }   
} mongosLimitConns;
}
