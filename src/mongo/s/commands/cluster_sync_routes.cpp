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
#include "mongo/s/grid.h"
#include "mongo/s/catalog/catalog_manager.h"
#include "mongo/grpc/masterproto/master.pb.h"
#include "mongo/util/log.h"

namespace mongo {
class SyncRoutesV2Cmd: public Command {
public:
    SyncRoutesV2Cmd() : Command("sync_cmongo_routes_v2") {}

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
        help << "sync cmongo routes";
    }

    Status checkAuthForCommand(ClientBasic* client,
                               const std::string& dbname,
                               const BSONObj& cmdObj) final {
		// NOTE(deyukong): no privileges needed for syncRoutes
		return Status::OK();
    }

    virtual bool run(OperationContext* txn,
                     const std::string& dbname,
                     BSONObj& cmdObj,
                     int options,
                     std::string& errmsg,
                     BSONObjBuilder& result) {
		BSONElement e = cmdObj["pb_bytes"];
		if (e.type() != mongo::BinData) {
			LOG(0) << "ERROR: SyncRoutesCmd got wrong  pb_bytes type:" << int(e.type());
			auto s = Status(ErrorCodes::BadValue, str::stream()
				   << "ERROR: SyncRoutesCmd got wrong  pb_bytes type:"
				   << e.type());
			return appendCommandStatus(result, s);
		}
		int len = 0;
		const char *binData = e.binData(len);
		masterproto::GetClusterRoutesRawRsp msg;
		// TODO(deyukong): set protobuf error handler
		bool ok = msg.ParseFromString(std::string(binData, len));
		if (!ok) {
			LOG(0) << "ERROR: GetClusterRoutesRawRsp::ParseFromString parse failed";
			auto s = Status(ErrorCodes::BadValue, "ERROR: GetClusterRoutesRawRsp::ParseFromString parse failed");
			return appendCommandStatus(result, s);
		}

		Status s = grid.catalogManager(txn)->syncRoutesRaw(txn, msg);
		if (!s.isOK()) {
			LOG(0) << "ERROR: syncRoutes failed:" << s;
		}
		return appendCommandStatus(result, s);
	}
} syncRoutesCmdV2;

class SyncRoutesCmd: public Command {
public:
    SyncRoutesCmd() : Command("sync_cmongo_routes") {}

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
        help << "sync cmongo routes";
    }

    Status checkAuthForCommand(ClientBasic* client,
                               const std::string& dbname,
                               const BSONObj& cmdObj) final {
		// NOTE(deyukong): no privileges needed for syncRoutes
		return Status::OK();
    }

    virtual bool run(OperationContext* txn,
                     const std::string& dbname,
                     BSONObj& cmdObj,
                     int options,
                     std::string& errmsg,
                     BSONObjBuilder& result) {
		BSONElement e = cmdObj["pb_bytes"];
		if (e.type() != mongo::BinData) {
			LOG(0) << "ERROR: SyncRoutesCmd got wrong  pb_bytes type:" << int(e.type());
			auto s = Status(ErrorCodes::BadValue, str::stream()
				   << "ERROR: SyncRoutesCmd got wrong  pb_bytes type:"
				   << e.type());
			return appendCommandStatus(result, s);
		}

		int len = 0;
		const char *binData = e.binData(len);
		masterproto::GetClusterRoutesRsp msg;
		// TODO(deyukong): set protobuf error handler
		bool ok = msg.ParseFromString(std::string(binData, len));
		if (!ok) {
			LOG(0) << "ERROR: GetClusterRoutesRsp::ParseFromString parse failed";
			auto s = Status(ErrorCodes::BadValue, "ERROR: GetClusterRoutesRsp::ParseFromString parse failed");
			return appendCommandStatus(result, s);
		}

		Status s = grid.catalogManager(txn)->syncRoutes(txn, msg);
		if (!s.isOK()) {
			LOG(0) << "ERROR: syncRoutes failed:" << s;
		}
		return appendCommandStatus(result, s);
	}
} syncRoutesCmd;
}



