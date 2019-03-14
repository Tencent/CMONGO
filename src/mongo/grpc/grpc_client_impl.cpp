/*

 Copyright (C) 2017 wolf(deyukong@tencent.com)

*/

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kNetwork


#include <grpc/grpc.h>
#include <grpc++/channel.h>
#include <grpc++/client_context.h>
#include <grpc++/create_channel.h>
#include <grpc++/security/credentials.h>
#include "mongo/grpc/grpc_client.h"
#include "mongo/grpc/cmongoproto/cmongo.pb.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/log.h"

namespace grpc {
namespace {
// NOTE(deyukong): from cmongo/utils/error.go
// it is better to be declared into protobuf
const uint32_t ERR_MASTER_NOT_PRIMARY = 30031;
}


Status MasterClient::AddResource(const masterproto::AddResourceReq& req, masterproto::AddResourceRsp* rsp) {
    invariant(rsp != nullptr);
    EnsureInited();

    ClientContext context;
    std::chrono::system_clock::time_point deadline =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context.set_deadline(deadline);
    Status s = stub_->AddResource(&context, req, rsp);

    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() == ERR_MASTER_NOT_PRIMARY) {
        LOG(0)<<"MasterClient::AddResource failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    } else {
        return s;
    }

    ReInit();

    ClientContext context1;
    std::chrono::system_clock::time_point deadline1 =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context1.set_deadline(deadline1);
    s = stub_->AddResource(&context1, req, rsp);
    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() != 0) {
        LOG(0)<<"MasterClient::AddResource failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    }
    return s;
}

Status MasterClient::BlockResource(const masterproto::BlockResourceReq& req, masterproto::BlockResourceRsp* rsp) {
    invariant(rsp != nullptr);
    EnsureInited();

    ClientContext context;
    std::chrono::system_clock::time_point deadline =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context.set_deadline(deadline);
    Status s = stub_->BlockResource(&context, req, rsp);

    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() == ERR_MASTER_NOT_PRIMARY) {
        LOG(0)<<"MasterClient::BlockResource failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    } else {
        return s;
    }

    ReInit();

    ClientContext context1;
    std::chrono::system_clock::time_point deadline1 =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context1.set_deadline(deadline1);
    s = stub_->BlockResource(&context1, req, rsp);
    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() != 0) {
        LOG(0)<<"MasterClient::BlockResource failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    }
    return s;
}

Status MasterClient::UnblockResource(const masterproto::UnblockResourceReq& req, masterproto::UnblockResourceRsp* rsp) {
    invariant(rsp != nullptr);
    EnsureInited();

    ClientContext context;
    std::chrono::system_clock::time_point deadline =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context.set_deadline(deadline);
    Status s = stub_->UnblockResource(&context, req, rsp);

    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() == ERR_MASTER_NOT_PRIMARY) {
        LOG(0)<<"MasterClient::UnblockResource failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    } else {
        return s;
    }

    ReInit();

    ClientContext context1;
    std::chrono::system_clock::time_point deadline1 =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context1.set_deadline(deadline1);
    s = stub_->UnblockResource(&context1, req, rsp);
    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() != 0) {
        LOG(0)<<"MasterClient::UnblockResource failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    }
    return s;
}

Status MasterClient::DelResource(const masterproto::DelResourceReq& req, masterproto::DelResourceRsp* rsp) {
    invariant(rsp != nullptr);
    EnsureInited();

    ClientContext context;
    std::chrono::system_clock::time_point deadline =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context.set_deadline(deadline);
    Status s = stub_->DelResource(&context, req, rsp);

    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() == ERR_MASTER_NOT_PRIMARY) {
        LOG(0)<<"MasterClient::DelResource failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    } else {
        return s;
    }

    ReInit();

    ClientContext context1;
    std::chrono::system_clock::time_point deadline1 =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context1.set_deadline(deadline1);
    s = stub_->DelResource(&context1, req, rsp);
    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() != 0) {
        LOG(0)<<"MasterClient::DelResource failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    }
    return s;
}

Status MasterClient::QueryResource(const masterproto::QueryResourceReq& req, masterproto::QueryResourceRsp* rsp) {
    invariant(rsp != nullptr);
    EnsureInited();

    ClientContext context;
    std::chrono::system_clock::time_point deadline =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context.set_deadline(deadline);
    Status s = stub_->QueryResource(&context, req, rsp);

    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() == ERR_MASTER_NOT_PRIMARY) {
        LOG(0)<<"MasterClient::QueryResource failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    } else {
        return s;
    }

    ReInit();

    ClientContext context1;
    std::chrono::system_clock::time_point deadline1 =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context1.set_deadline(deadline1);
    s = stub_->QueryResource(&context1, req, rsp);
    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() != 0) {
        LOG(0)<<"MasterClient::QueryResource failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    }
    return s;
}

Status MasterClient::MigrateContainer(const masterproto::MigrateContainerReq& req, masterproto::MigrateContainerRsp* rsp) {
    invariant(rsp != nullptr);
    EnsureInited();

    ClientContext context;
    std::chrono::system_clock::time_point deadline =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context.set_deadline(deadline);
    Status s = stub_->MigrateContainer(&context, req, rsp);

    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() == ERR_MASTER_NOT_PRIMARY) {
        LOG(0)<<"MasterClient::MigrateContainer failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    } else {
        return s;
    }

    ReInit();

    ClientContext context1;
    std::chrono::system_clock::time_point deadline1 =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context1.set_deadline(deadline1);
    s = stub_->MigrateContainer(&context1, req, rsp);
    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() != 0) {
        LOG(0)<<"MasterClient::MigrateContainer failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    }
    return s;
}

Status MasterClient::MigrateMachine(const masterproto::MigrateMachineReq& req, masterproto::MigrateMachineRsp* rsp) {
    invariant(rsp != nullptr);
    EnsureInited();

    ClientContext context;
    std::chrono::system_clock::time_point deadline =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context.set_deadline(deadline);
    Status s = stub_->MigrateMachine(&context, req, rsp);

    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() == ERR_MASTER_NOT_PRIMARY) {
        LOG(0)<<"MasterClient::MigrateMachine failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    } else {
        return s;
    }

    ReInit();

    ClientContext context1;
    std::chrono::system_clock::time_point deadline1 =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context1.set_deadline(deadline1);
    s = stub_->MigrateMachine(&context1, req, rsp);
    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() != 0) {
        LOG(0)<<"MasterClient::MigrateMachine failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    }
    return s;
}

Status MasterClient::QueryMachine(const masterproto::QueryMachineReq& req, masterproto::QueryMachineRsp* rsp) {
    invariant(rsp != nullptr);
    EnsureInited();

    ClientContext context;
    std::chrono::system_clock::time_point deadline =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context.set_deadline(deadline);
    Status s = stub_->QueryMachine(&context, req, rsp);

    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() == ERR_MASTER_NOT_PRIMARY) {
        LOG(0)<<"MasterClient::QueryMachine failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    } else {
        return s;
    }

    ReInit();

    ClientContext context1;
    std::chrono::system_clock::time_point deadline1 =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context1.set_deadline(deadline1);
    s = stub_->QueryMachine(&context1, req, rsp);
    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() != 0) {
        LOG(0)<<"MasterClient::QueryMachine failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    }
    return s;
}

Status MasterClient::AddContainer(const masterproto::AddContainerReq& req, masterproto::AddContainerRsp* rsp) {
    invariant(rsp != nullptr);
    EnsureInited();

    ClientContext context;
    std::chrono::system_clock::time_point deadline =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context.set_deadline(deadline);
    Status s = stub_->AddContainer(&context, req, rsp);

    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() == ERR_MASTER_NOT_PRIMARY) {
        LOG(0)<<"MasterClient::AddContainer failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    } else {
        return s;
    }

    ReInit();

    ClientContext context1;
    std::chrono::system_clock::time_point deadline1 =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context1.set_deadline(deadline1);
    s = stub_->AddContainer(&context1, req, rsp);
    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() != 0) {
        LOG(0)<<"MasterClient::AddContainer failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    }
    return s;
}

Status MasterClient::DelContainer(const masterproto::DelContainerReq& req, masterproto::DelContainerRsp* rsp) {
    invariant(rsp != nullptr);
    EnsureInited();

    ClientContext context;
    std::chrono::system_clock::time_point deadline =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context.set_deadline(deadline);
    Status s = stub_->DelContainer(&context, req, rsp);

    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() == ERR_MASTER_NOT_PRIMARY) {
        LOG(0)<<"MasterClient::DelContainer failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    } else {
        return s;
    }

    ReInit();

    ClientContext context1;
    std::chrono::system_clock::time_point deadline1 =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context1.set_deadline(deadline1);
    s = stub_->DelContainer(&context1, req, rsp);
    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() != 0) {
        LOG(0)<<"MasterClient::DelContainer failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    }
    return s;
}

Status MasterClient::AddContainerAsync(const masterproto::AddContainerAsyncReq& req, masterproto::AddContainerAsyncRsp* rsp) {
    invariant(rsp != nullptr);
    EnsureInited();

    ClientContext context;
    std::chrono::system_clock::time_point deadline =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context.set_deadline(deadline);
    Status s = stub_->AddContainerAsync(&context, req, rsp);

    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() == ERR_MASTER_NOT_PRIMARY) {
        LOG(0)<<"MasterClient::AddContainerAsync failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    } else {
        return s;
    }

    ReInit();

    ClientContext context1;
    std::chrono::system_clock::time_point deadline1 =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context1.set_deadline(deadline1);
    s = stub_->AddContainerAsync(&context1, req, rsp);
    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() != 0) {
        LOG(0)<<"MasterClient::AddContainerAsync failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    }
    return s;
}

Status MasterClient::CreateCluster(const masterproto::CreateClusterReq& req, masterproto::CreateClusterRsp* rsp) {
    invariant(rsp != nullptr);
    EnsureInited();

    ClientContext context;
    std::chrono::system_clock::time_point deadline =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context.set_deadline(deadline);
    Status s = stub_->CreateCluster(&context, req, rsp);

    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() == ERR_MASTER_NOT_PRIMARY) {
        LOG(0)<<"MasterClient::CreateCluster failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    } else {
        return s;
    }

    ReInit();

    ClientContext context1;
    std::chrono::system_clock::time_point deadline1 =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context1.set_deadline(deadline1);
    s = stub_->CreateCluster(&context1, req, rsp);
    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() != 0) {
        LOG(0)<<"MasterClient::CreateCluster failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    }
    return s;
}

Status MasterClient::UpgradeClusterModule(const masterproto::UpgradeClusterModuleReq& req, masterproto::UpgradeClusterModuleRsp* rsp) {
    invariant(rsp != nullptr);
    EnsureInited();

    ClientContext context;
    std::chrono::system_clock::time_point deadline =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context.set_deadline(deadline);
    Status s = stub_->UpgradeClusterModule(&context, req, rsp);

    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() == ERR_MASTER_NOT_PRIMARY) {
        LOG(0)<<"MasterClient::UpgradeClusterModule failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    } else {
        return s;
    }

    ReInit();

    ClientContext context1;
    std::chrono::system_clock::time_point deadline1 =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context1.set_deadline(deadline1);
    s = stub_->UpgradeClusterModule(&context1, req, rsp);
    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() != 0) {
        LOG(0)<<"MasterClient::UpgradeClusterModule failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    }
    return s;
}

Status MasterClient::InfoCluster(const masterproto::InfoClusterReq& req, masterproto::InfoClusterRsp* rsp) {
    invariant(rsp != nullptr);
    EnsureInited();

    ClientContext context;
    std::chrono::system_clock::time_point deadline =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context.set_deadline(deadline);
    Status s = stub_->InfoCluster(&context, req, rsp);

    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() == ERR_MASTER_NOT_PRIMARY) {
        LOG(0)<<"MasterClient::InfoCluster failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    } else {
        return s;
    }

    ReInit();

    ClientContext context1;
    std::chrono::system_clock::time_point deadline1 =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context1.set_deadline(deadline1);
    s = stub_->InfoCluster(&context1, req, rsp);
    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() != 0) {
        LOG(0)<<"MasterClient::InfoCluster failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    }
    return s;
}

Status MasterClient::DelCluster(const masterproto::DelClusterReq& req, masterproto::DelClusterRsp* rsp) {
    invariant(rsp != nullptr);
    EnsureInited();

    ClientContext context;
    std::chrono::system_clock::time_point deadline =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context.set_deadline(deadline);
    Status s = stub_->DelCluster(&context, req, rsp);

    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() == ERR_MASTER_NOT_PRIMARY) {
        LOG(0)<<"MasterClient::DelCluster failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    } else {
        return s;
    }

    ReInit();

    ClientContext context1;
    std::chrono::system_clock::time_point deadline1 =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context1.set_deadline(deadline1);
    s = stub_->DelCluster(&context1, req, rsp);
    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() != 0) {
        LOG(0)<<"MasterClient::DelCluster failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    }
    return s;
}

Status MasterClient::ListCluster(const masterproto::ListClusterReq& req, masterproto::ListClusterRsp* rsp) {
    invariant(rsp != nullptr);
    EnsureInited();

    ClientContext context;
    std::chrono::system_clock::time_point deadline =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context.set_deadline(deadline);
    Status s = stub_->ListCluster(&context, req, rsp);

    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() == ERR_MASTER_NOT_PRIMARY) {
        LOG(0)<<"MasterClient::ListCluster failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    } else {
        return s;
    }

    ReInit();

    ClientContext context1;
    std::chrono::system_clock::time_point deadline1 =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context1.set_deadline(deadline1);
    s = stub_->ListCluster(&context1, req, rsp);
    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() != 0) {
        LOG(0)<<"MasterClient::ListCluster failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    }
    return s;
}

Status MasterClient::ChangePassword(const masterproto::ChPasswdReq& req, masterproto::ChPasswdRsp* rsp) {
    invariant(rsp != nullptr);
    EnsureInited();

    ClientContext context;
    std::chrono::system_clock::time_point deadline =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context.set_deadline(deadline);
    Status s = stub_->ChangePassword(&context, req, rsp);

    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() == ERR_MASTER_NOT_PRIMARY) {
        LOG(0)<<"MasterClient::ChangePassword failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    } else {
        return s;
    }

    ReInit();

    ClientContext context1;
    std::chrono::system_clock::time_point deadline1 =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context1.set_deadline(deadline1);
    s = stub_->ChangePassword(&context1, req, rsp);
    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() != 0) {
        LOG(0)<<"MasterClient::ChangePassword failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    }
    return s;
}

Status MasterClient::JobStatus(const masterproto::JobStatusReq& req, masterproto::JobStatusRsp* rsp) {
    invariant(rsp != nullptr);
    EnsureInited();

    ClientContext context;
    std::chrono::system_clock::time_point deadline =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context.set_deadline(deadline);
    Status s = stub_->JobStatus(&context, req, rsp);

    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() == ERR_MASTER_NOT_PRIMARY) {
        LOG(0)<<"MasterClient::JobStatus failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    } else {
        return s;
    }

    ReInit();

    ClientContext context1;
    std::chrono::system_clock::time_point deadline1 =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context1.set_deadline(deadline1);
    s = stub_->JobStatus(&context1, req, rsp);
    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() != 0) {
        LOG(0)<<"MasterClient::JobStatus failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    }
    return s;
}

Status MasterClient::ResizeCluster(const masterproto::ResizeClusterReq& req, masterproto::ResizeClusterRsp* rsp) {
    invariant(rsp != nullptr);
    EnsureInited();

    ClientContext context;
    std::chrono::system_clock::time_point deadline =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context.set_deadline(deadline);
    Status s = stub_->ResizeCluster(&context, req, rsp);

    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() == ERR_MASTER_NOT_PRIMARY) {
        LOG(0)<<"MasterClient::ResizeCluster failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    } else {
        return s;
    }

    ReInit();

    ClientContext context1;
    std::chrono::system_clock::time_point deadline1 =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context1.set_deadline(deadline1);
    s = stub_->ResizeCluster(&context1, req, rsp);
    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() != 0) {
        LOG(0)<<"MasterClient::ResizeCluster failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    }
    return s;
}

Status MasterClient::BackupCluster(const masterproto::BackupClusterReq& req, masterproto::BackupClusterRsp* rsp) {
    invariant(rsp != nullptr);
    EnsureInited();

    ClientContext context;
    std::chrono::system_clock::time_point deadline =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context.set_deadline(deadline);
    Status s = stub_->BackupCluster(&context, req, rsp);

    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() == ERR_MASTER_NOT_PRIMARY) {
        LOG(0)<<"MasterClient::BackupCluster failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    } else {
        return s;
    }

    ReInit();

    ClientContext context1;
    std::chrono::system_clock::time_point deadline1 =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context1.set_deadline(deadline1);
    s = stub_->BackupCluster(&context1, req, rsp);
    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() != 0) {
        LOG(0)<<"MasterClient::BackupCluster failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    }
    return s;
}

Status MasterClient::QueryBackupList(const masterproto::QueryBackupListReq& req, masterproto::QueryBackupListRsp* rsp) {
    invariant(rsp != nullptr);
    EnsureInited();

    ClientContext context;
    std::chrono::system_clock::time_point deadline =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context.set_deadline(deadline);
    Status s = stub_->QueryBackupList(&context, req, rsp);

    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() == ERR_MASTER_NOT_PRIMARY) {
        LOG(0)<<"MasterClient::QueryBackupList failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    } else {
        return s;
    }

    ReInit();

    ClientContext context1;
    std::chrono::system_clock::time_point deadline1 =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context1.set_deadline(deadline1);
    s = stub_->QueryBackupList(&context1, req, rsp);
    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() != 0) {
        LOG(0)<<"MasterClient::QueryBackupList failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    }
    return s;
}

Status MasterClient::CheckRestoreTs(const masterproto::CheckRestoreTsReq& req, masterproto::CheckRestoreTsRsp* rsp) {
    invariant(rsp != nullptr);
    EnsureInited();

    ClientContext context;
    std::chrono::system_clock::time_point deadline =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context.set_deadline(deadline);
    Status s = stub_->CheckRestoreTs(&context, req, rsp);

    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() == ERR_MASTER_NOT_PRIMARY) {
        LOG(0)<<"MasterClient::CheckRestoreTs failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    } else {
        return s;
    }

    ReInit();

    ClientContext context1;
    std::chrono::system_clock::time_point deadline1 =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context1.set_deadline(deadline1);
    s = stub_->CheckRestoreTs(&context1, req, rsp);
    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() != 0) {
        LOG(0)<<"MasterClient::CheckRestoreTs failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    }
    return s;
}

Status MasterClient::RestoreCluster(const masterproto::RestoreClusterReq& req, masterproto::RestoreClusterRsp* rsp) {
    invariant(rsp != nullptr);
    EnsureInited();

    ClientContext context;
    std::chrono::system_clock::time_point deadline =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context.set_deadline(deadline);
    Status s = stub_->RestoreCluster(&context, req, rsp);

    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() == ERR_MASTER_NOT_PRIMARY) {
        LOG(0)<<"MasterClient::RestoreCluster failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    } else {
        return s;
    }

    ReInit();

    ClientContext context1;
    std::chrono::system_clock::time_point deadline1 =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context1.set_deadline(deadline1);
    s = stub_->RestoreCluster(&context1, req, rsp);
    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() != 0) {
        LOG(0)<<"MasterClient::RestoreCluster failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    }
    return s;
}

Status MasterClient::ExchangeCluster(const masterproto::ExchangeClusterReq& req, masterproto::ExchangeClusterRsp* rsp) {
    invariant(rsp != nullptr);
    EnsureInited();

    ClientContext context;
    std::chrono::system_clock::time_point deadline =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context.set_deadline(deadline);
    Status s = stub_->ExchangeCluster(&context, req, rsp);

    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() == ERR_MASTER_NOT_PRIMARY) {
        LOG(0)<<"MasterClient::ExchangeCluster failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    } else {
        return s;
    }

    ReInit();

    ClientContext context1;
    std::chrono::system_clock::time_point deadline1 =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context1.set_deadline(deadline1);
    s = stub_->ExchangeCluster(&context1, req, rsp);
    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() != 0) {
        LOG(0)<<"MasterClient::ExchangeCluster failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    }
    return s;
}

Status MasterClient::IsolationCluster(const masterproto::IsolationClusterReq& req, masterproto::IsolationClusterRsp* rsp) {
    invariant(rsp != nullptr);
    EnsureInited();

    ClientContext context;
    std::chrono::system_clock::time_point deadline =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context.set_deadline(deadline);
    Status s = stub_->IsolationCluster(&context, req, rsp);

    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() == ERR_MASTER_NOT_PRIMARY) {
        LOG(0)<<"MasterClient::IsolationCluster failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    } else {
        return s;
    }

    ReInit();

    ClientContext context1;
    std::chrono::system_clock::time_point deadline1 =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context1.set_deadline(deadline1);
    s = stub_->IsolationCluster(&context1, req, rsp);
    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() != 0) {
        LOG(0)<<"MasterClient::IsolationCluster failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    }
    return s;
}

Status MasterClient::AddReplicateSet(const masterproto::AddRsReq& req, masterproto::AddRsRsp* rsp) {
    invariant(rsp != nullptr);
    EnsureInited();

    ClientContext context;
    std::chrono::system_clock::time_point deadline =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context.set_deadline(deadline);
    Status s = stub_->AddReplicateSet(&context, req, rsp);

    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() == ERR_MASTER_NOT_PRIMARY) {
        LOG(0)<<"MasterClient::AddReplicateSet failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    } else {
        return s;
    }

    ReInit();

    ClientContext context1;
    std::chrono::system_clock::time_point deadline1 =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context1.set_deadline(deadline1);
    s = stub_->AddReplicateSet(&context1, req, rsp);
    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() != 0) {
        LOG(0)<<"MasterClient::AddReplicateSet failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    }
    return s;
}

Status MasterClient::DelReplicateSet(const masterproto::DelRsReq& req, masterproto::DelRsRsp* rsp) {
    invariant(rsp != nullptr);
    EnsureInited();

    ClientContext context;
    std::chrono::system_clock::time_point deadline =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context.set_deadline(deadline);
    Status s = stub_->DelReplicateSet(&context, req, rsp);

    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() == ERR_MASTER_NOT_PRIMARY) {
        LOG(0)<<"MasterClient::DelReplicateSet failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    } else {
        return s;
    }

    ReInit();

    ClientContext context1;
    std::chrono::system_clock::time_point deadline1 =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context1.set_deadline(deadline1);
    s = stub_->DelReplicateSet(&context1, req, rsp);
    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() != 0) {
        LOG(0)<<"MasterClient::DelReplicateSet failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    }
    return s;
}

Status MasterClient::QueryOpLogStatus(const masterproto::OpLogStatusReq& req, masterproto::OpLogStatusRsp* rsp) {
    invariant(rsp != nullptr);
    EnsureInited();

    ClientContext context;
    std::chrono::system_clock::time_point deadline =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context.set_deadline(deadline);
    Status s = stub_->QueryOpLogStatus(&context, req, rsp);

    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() == ERR_MASTER_NOT_PRIMARY) {
        LOG(0)<<"MasterClient::QueryOpLogStatus failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    } else {
        return s;
    }

    ReInit();

    ClientContext context1;
    std::chrono::system_clock::time_point deadline1 =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context1.set_deadline(deadline1);
    s = stub_->QueryOpLogStatus(&context1, req, rsp);
    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() != 0) {
        LOG(0)<<"MasterClient::QueryOpLogStatus failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    }
    return s;
}

Status MasterClient::ShowDb(const masterproto::ShowDbReq& req, masterproto::ShowDbRsp* rsp) {
    invariant(rsp != nullptr);
    EnsureInited();

    ClientContext context;
    std::chrono::system_clock::time_point deadline =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context.set_deadline(deadline);
    Status s = stub_->ShowDb(&context, req, rsp);

    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() == ERR_MASTER_NOT_PRIMARY) {
        LOG(0)<<"MasterClient::ShowDb failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    } else {
        return s;
    }

    ReInit();

    ClientContext context1;
    std::chrono::system_clock::time_point deadline1 =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context1.set_deadline(deadline1);
    s = stub_->ShowDb(&context1, req, rsp);
    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() != 0) {
        LOG(0)<<"MasterClient::ShowDb failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    }
    return s;
}

Status MasterClient::ShowColls(const masterproto::ShowCollsReq& req, masterproto::ShowCollsRsp* rsp) {
    invariant(rsp != nullptr);
    EnsureInited();

    ClientContext context;
    std::chrono::system_clock::time_point deadline =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context.set_deadline(deadline);
    Status s = stub_->ShowColls(&context, req, rsp);

    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() == ERR_MASTER_NOT_PRIMARY) {
        LOG(0)<<"MasterClient::ShowColls failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    } else {
        return s;
    }

    ReInit();

    ClientContext context1;
    std::chrono::system_clock::time_point deadline1 =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context1.set_deadline(deadline1);
    s = stub_->ShowColls(&context1, req, rsp);
    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() != 0) {
        LOG(0)<<"MasterClient::ShowColls failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    }
    return s;
}

Status MasterClient::GetSlowQuery(const masterproto::GetSlowQueryReq& req, masterproto::GetSlowQueryRsp* rsp) {
    invariant(rsp != nullptr);
    EnsureInited();

    ClientContext context;
    std::chrono::system_clock::time_point deadline =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context.set_deadline(deadline);
    Status s = stub_->GetSlowQuery(&context, req, rsp);

    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() == ERR_MASTER_NOT_PRIMARY) {
        LOG(0)<<"MasterClient::GetSlowQuery failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    } else {
        return s;
    }

    ReInit();

    ClientContext context1;
    std::chrono::system_clock::time_point deadline1 =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context1.set_deadline(deadline1);
    s = stub_->GetSlowQuery(&context1, req, rsp);
    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() != 0) {
        LOG(0)<<"MasterClient::GetSlowQuery failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    }
    return s;
}

Status MasterClient::QrySlowInfo(const masterproto::QrySlowInfoReq& req, masterproto::QrySlowInfoRsp* rsp) {
    invariant(rsp != nullptr);
    EnsureInited();

    ClientContext context;
    std::chrono::system_clock::time_point deadline =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context.set_deadline(deadline);
    Status s = stub_->QrySlowInfo(&context, req, rsp);

    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() == ERR_MASTER_NOT_PRIMARY) {
        LOG(0)<<"MasterClient::QrySlowInfo failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    } else {
        return s;
    }

    ReInit();

    ClientContext context1;
    std::chrono::system_clock::time_point deadline1 =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context1.set_deadline(deadline1);
    s = stub_->QrySlowInfo(&context1, req, rsp);
    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() != 0) {
        LOG(0)<<"MasterClient::QrySlowInfo failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    }
    return s;
}

Status MasterClient::CreateUser(const masterproto::CreateUserReq& req, masterproto::CreateUserRsp* rsp) {
    invariant(rsp != nullptr);
    EnsureInited();

    ClientContext context;
    std::chrono::system_clock::time_point deadline =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context.set_deadline(deadline);
    Status s = stub_->CreateUser(&context, req, rsp);

    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() == ERR_MASTER_NOT_PRIMARY) {
        LOG(0)<<"MasterClient::CreateUser failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    } else {
        return s;
    }

    ReInit();

    ClientContext context1;
    std::chrono::system_clock::time_point deadline1 =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context1.set_deadline(deadline1);
    s = stub_->CreateUser(&context1, req, rsp);
    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() != 0) {
        LOG(0)<<"MasterClient::CreateUser failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    }
    return s;
}

Status MasterClient::QueryUserList(const masterproto::QueryUserListReq& req, masterproto::QueryUserListRsp* rsp) {
    invariant(rsp != nullptr);
    EnsureInited();

    ClientContext context;
    std::chrono::system_clock::time_point deadline =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context.set_deadline(deadline);
    Status s = stub_->QueryUserList(&context, req, rsp);

    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() == ERR_MASTER_NOT_PRIMARY) {
        LOG(0)<<"MasterClient::QueryUserList failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    } else {
        return s;
    }

    ReInit();

    ClientContext context1;
    std::chrono::system_clock::time_point deadline1 =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context1.set_deadline(deadline1);
    s = stub_->QueryUserList(&context1, req, rsp);
    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() != 0) {
        LOG(0)<<"MasterClient::QueryUserList failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    }
    return s;
}

Status MasterClient::DeleteUser(const masterproto::DeleteUserReq& req, masterproto::DeleteUserRsp* rsp) {
    invariant(rsp != nullptr);
    EnsureInited();

    ClientContext context;
    std::chrono::system_clock::time_point deadline =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context.set_deadline(deadline);
    Status s = stub_->DeleteUser(&context, req, rsp);

    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() == ERR_MASTER_NOT_PRIMARY) {
        LOG(0)<<"MasterClient::DeleteUser failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    } else {
        return s;
    }

    ReInit();

    ClientContext context1;
    std::chrono::system_clock::time_point deadline1 =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context1.set_deadline(deadline1);
    s = stub_->DeleteUser(&context1, req, rsp);
    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() != 0) {
        LOG(0)<<"MasterClient::DeleteUser failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    }
    return s;
}

Status MasterClient::SetUserAuth(const masterproto::SetUserAuthReq& req, masterproto::SetUserAuthRsp* rsp) {
    invariant(rsp != nullptr);
    EnsureInited();

    ClientContext context;
    std::chrono::system_clock::time_point deadline =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context.set_deadline(deadline);
    Status s = stub_->SetUserAuth(&context, req, rsp);

    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() == ERR_MASTER_NOT_PRIMARY) {
        LOG(0)<<"MasterClient::SetUserAuth failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    } else {
        return s;
    }

    ReInit();

    ClientContext context1;
    std::chrono::system_clock::time_point deadline1 =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context1.set_deadline(deadline1);
    s = stub_->SetUserAuth(&context1, req, rsp);
    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() != 0) {
        LOG(0)<<"MasterClient::SetUserAuth failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    }
    return s;
}

Status MasterClient::CreateTable(const masterproto::CreateTableReq& req, masterproto::CreateTableRsp* rsp) {
    invariant(rsp != nullptr);
    EnsureInited();

    ClientContext context;
    std::chrono::system_clock::time_point deadline =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context.set_deadline(deadline);
    Status s = stub_->CreateTable(&context, req, rsp);

    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() == ERR_MASTER_NOT_PRIMARY) {
        LOG(0)<<"MasterClient::CreateTable failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    } else {
        return s;
    }

    ReInit();

    ClientContext context1;
    std::chrono::system_clock::time_point deadline1 =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context1.set_deadline(deadline1);
    s = stub_->CreateTable(&context1, req, rsp);
    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() != 0) {
        LOG(0)<<"MasterClient::CreateTable failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    }
    return s;
}

Status MasterClient::DropTable(const masterproto::DropTableReq& req, masterproto::DropTableRsp* rsp) {
    invariant(rsp != nullptr);
    EnsureInited();

    ClientContext context;
    std::chrono::system_clock::time_point deadline =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context.set_deadline(deadline);
    Status s = stub_->DropTable(&context, req, rsp);

    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() == ERR_MASTER_NOT_PRIMARY) {
        LOG(0)<<"MasterClient::DropTable failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    } else {
        return s;
    }

    ReInit();

    ClientContext context1;
    std::chrono::system_clock::time_point deadline1 =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context1.set_deadline(deadline1);
    s = stub_->DropTable(&context1, req, rsp);
    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() != 0) {
        LOG(0)<<"MasterClient::DropTable failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    }
    return s;
}

Status MasterClient::DropDatabase(const masterproto::DropDatabaseReq& req, masterproto::DropDatabaseRsp* rsp) {
    invariant(rsp != nullptr);
    EnsureInited();

    ClientContext context;
    std::chrono::system_clock::time_point deadline =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context.set_deadline(deadline);
    Status s = stub_->DropDatabase(&context, req, rsp);

    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() == ERR_MASTER_NOT_PRIMARY) {
        LOG(0)<<"MasterClient::DropDatabase failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    } else {
        return s;
    }

    ReInit();

    ClientContext context1;
    std::chrono::system_clock::time_point deadline1 =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context1.set_deadline(deadline1);
    s = stub_->DropDatabase(&context1, req, rsp);
    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() != 0) {
        LOG(0)<<"MasterClient::DropDatabase failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    }
    return s;
}

Status MasterClient::EnsureIndex(const masterproto::EnsureIndexReq& req, masterproto::EnsureIndexRsp* rsp) {
    invariant(rsp != nullptr);
    EnsureInited();

    ClientContext context;
    std::chrono::system_clock::time_point deadline =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context.set_deadline(deadline);
    Status s = stub_->EnsureIndex(&context, req, rsp);

    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() == ERR_MASTER_NOT_PRIMARY) {
        LOG(0)<<"MasterClient::EnsureIndex failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    } else {
        return s;
    }

    ReInit();

    ClientContext context1;
    std::chrono::system_clock::time_point deadline1 =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context1.set_deadline(deadline1);
    s = stub_->EnsureIndex(&context1, req, rsp);
    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() != 0) {
        LOG(0)<<"MasterClient::EnsureIndex failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    }
    return s;
}

Status MasterClient::DropIndex(const masterproto::DropIndexReq& req, masterproto::DropIndexRsp* rsp) {
    invariant(rsp != nullptr);
    EnsureInited();

    ClientContext context;
    std::chrono::system_clock::time_point deadline =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context.set_deadline(deadline);
    Status s = stub_->DropIndex(&context, req, rsp);

    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() == ERR_MASTER_NOT_PRIMARY) {
        LOG(0)<<"MasterClient::DropIndex failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    } else {
        return s;
    }

    ReInit();

    ClientContext context1;
    std::chrono::system_clock::time_point deadline1 =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context1.set_deadline(deadline1);
    s = stub_->DropIndex(&context1, req, rsp);
    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() != 0) {
        LOG(0)<<"MasterClient::DropIndex failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    }
    return s;
}

Status MasterClient::DropIndexName(const masterproto::DropIndexNameReq& req, masterproto::DropIndexNameRsp* rsp) {
    invariant(rsp != nullptr);
    EnsureInited();

    ClientContext context;
    std::chrono::system_clock::time_point deadline =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context.set_deadline(deadline);
    Status s = stub_->DropIndexName(&context, req, rsp);

    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() == ERR_MASTER_NOT_PRIMARY) {
        LOG(0)<<"MasterClient::DropIndexName failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    } else {
        return s;
    }

    ReInit();

    ClientContext context1;
    std::chrono::system_clock::time_point deadline1 =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context1.set_deadline(deadline1);
    s = stub_->DropIndexName(&context1, req, rsp);
    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() != 0) {
        LOG(0)<<"MasterClient::DropIndexName failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    }
    return s;
}

Status MasterClient::ReportMigrateInfo(const masterproto::ReportMigrateInfoReq& req, masterproto::ReportMigrateInfoRsp* rsp) {
    invariant(rsp != nullptr);
    EnsureInited();

    ClientContext context;
    std::chrono::system_clock::time_point deadline =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context.set_deadline(deadline);
    Status s = stub_->ReportMigrateInfo(&context, req, rsp);

    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() == ERR_MASTER_NOT_PRIMARY) {
        LOG(0)<<"MasterClient::ReportMigrateInfo failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    } else {
        return s;
    }

    ReInit();

    ClientContext context1;
    std::chrono::system_clock::time_point deadline1 =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context1.set_deadline(deadline1);
    s = stub_->ReportMigrateInfo(&context1, req, rsp);
    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() != 0) {
        LOG(0)<<"MasterClient::ReportMigrateInfo failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    }
    return s;
}

Status MasterClient::Migrate(const masterproto::MigrateReq& req, masterproto::MigrateRsp* rsp) {
    invariant(rsp != nullptr);
    EnsureInited();

    ClientContext context;
    std::chrono::system_clock::time_point deadline =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context.set_deadline(deadline);
    Status s = stub_->Migrate(&context, req, rsp);

    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() == ERR_MASTER_NOT_PRIMARY) {
        LOG(0)<<"MasterClient::Migrate failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    } else {
        return s;
    }

    ReInit();

    ClientContext context1;
    std::chrono::system_clock::time_point deadline1 =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context1.set_deadline(deadline1);
    s = stub_->Migrate(&context1, req, rsp);
    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() != 0) {
        LOG(0)<<"MasterClient::Migrate failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    }
    return s;
}

Status MasterClient::CleanMigrate(const masterproto::CleanMigrateReq& req, masterproto::CleanMigrateRsp* rsp) {
    invariant(rsp != nullptr);
    EnsureInited();

    ClientContext context;
    std::chrono::system_clock::time_point deadline =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context.set_deadline(deadline);
    Status s = stub_->CleanMigrate(&context, req, rsp);

    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() == ERR_MASTER_NOT_PRIMARY) {
        LOG(0)<<"MasterClient::CleanMigrate failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    } else {
        return s;
    }

    ReInit();

    ClientContext context1;
    std::chrono::system_clock::time_point deadline1 =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context1.set_deadline(deadline1);
    s = stub_->CleanMigrate(&context1, req, rsp);
    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() != 0) {
        LOG(0)<<"MasterClient::CleanMigrate failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    }
    return s;
}

Status MasterClient::GetMigrateStatus(const masterproto::GetMigrateStatusReq& req, masterproto::GetMigrateStatusRsp* rsp) {
    invariant(rsp != nullptr);
    EnsureInited();

    ClientContext context;
    std::chrono::system_clock::time_point deadline =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context.set_deadline(deadline);
    Status s = stub_->GetMigrateStatus(&context, req, rsp);

    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() == ERR_MASTER_NOT_PRIMARY) {
        LOG(0)<<"MasterClient::GetMigrateStatus failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    } else {
        return s;
    }

    ReInit();

    ClientContext context1;
    std::chrono::system_clock::time_point deadline1 =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context1.set_deadline(deadline1);
    s = stub_->GetMigrateStatus(&context1, req, rsp);
    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() != 0) {
        LOG(0)<<"MasterClient::GetMigrateStatus failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    }
    return s;
}

Status MasterClient::IsTableInMigrating(const masterproto::IsTableInMigratingReq& req, masterproto::IsTableInMigratingRsp* rsp) {
    invariant(rsp != nullptr);
    EnsureInited();

    ClientContext context;
    std::chrono::system_clock::time_point deadline =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context.set_deadline(deadline);
    Status s = stub_->IsTableInMigrating(&context, req, rsp);

    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() == ERR_MASTER_NOT_PRIMARY) {
        LOG(0)<<"MasterClient::IsTableInMigrating failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    } else {
        return s;
    }

    ReInit();

    ClientContext context1;
    std::chrono::system_clock::time_point deadline1 =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context1.set_deadline(deadline1);
    s = stub_->IsTableInMigrating(&context1, req, rsp);
    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() != 0) {
        LOG(0)<<"MasterClient::IsTableInMigrating failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    }
    return s;
}

Status MasterClient::RestoreFiles(const masterproto::RestoreFilesReq& req, masterproto::RestoreFilesRes* rsp) {
    invariant(rsp != nullptr);
    EnsureInited();

    ClientContext context;
    std::chrono::system_clock::time_point deadline =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context.set_deadline(deadline);
    Status s = stub_->RestoreFiles(&context, req, rsp);

    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() == ERR_MASTER_NOT_PRIMARY) {
        LOG(0)<<"MasterClient::RestoreFiles failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    } else {
        return s;
    }

    ReInit();

    ClientContext context1;
    std::chrono::system_clock::time_point deadline1 =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context1.set_deadline(deadline1);
    s = stub_->RestoreFiles(&context1, req, rsp);
    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() != 0) {
        LOG(0)<<"MasterClient::RestoreFiles failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    }
    return s;
}

Status MasterClient::GenBalanceTask(const masterproto::GenBalanceTaskReq& req, masterproto::GenBalanceTaskRsp* rsp) {
    invariant(rsp != nullptr);
    EnsureInited();

    ClientContext context;
    std::chrono::system_clock::time_point deadline =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context.set_deadline(deadline);
    Status s = stub_->GenBalanceTask(&context, req, rsp);

    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() == ERR_MASTER_NOT_PRIMARY) {
        LOG(0)<<"MasterClient::GenBalanceTask failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    } else {
        return s;
    }

    ReInit();

    ClientContext context1;
    std::chrono::system_clock::time_point deadline1 =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context1.set_deadline(deadline1);
    s = stub_->GenBalanceTask(&context1, req, rsp);
    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() != 0) {
        LOG(0)<<"MasterClient::GenBalanceTask failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    }
    return s;
}

Status MasterClient::Balance(const masterproto::BalanceReq& req, masterproto::BalanceRsp* rsp) {
    invariant(rsp != nullptr);
    EnsureInited();

    ClientContext context;
    std::chrono::system_clock::time_point deadline =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context.set_deadline(deadline);
    Status s = stub_->Balance(&context, req, rsp);

    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() == ERR_MASTER_NOT_PRIMARY) {
        LOG(0)<<"MasterClient::Balance failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    } else {
        return s;
    }

    ReInit();

    ClientContext context1;
    std::chrono::system_clock::time_point deadline1 =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context1.set_deadline(deadline1);
    s = stub_->Balance(&context1, req, rsp);
    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() != 0) {
        LOG(0)<<"MasterClient::Balance failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    }
    return s;
}

Status MasterClient::ChangeRouteStat(const masterproto::ChangeRouteStateReq& req, masterproto::ChangeRouteStateRsp* rsp) {
    invariant(rsp != nullptr);
    EnsureInited();

    ClientContext context;
    std::chrono::system_clock::time_point deadline =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context.set_deadline(deadline);
    Status s = stub_->ChangeRouteStat(&context, req, rsp);

    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() == ERR_MASTER_NOT_PRIMARY) {
        LOG(0)<<"MasterClient::ChangeRouteStat failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    } else {
        return s;
    }

    ReInit();

    ClientContext context1;
    std::chrono::system_clock::time_point deadline1 =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context1.set_deadline(deadline1);
    s = stub_->ChangeRouteStat(&context1, req, rsp);
    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() != 0) {
        LOG(0)<<"MasterClient::ChangeRouteStat failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    }
    return s;
}

Status MasterClient::ChangeRouteRs(const masterproto::ChangeRouteRsReq& req, masterproto::ChangeRouteRsRsp* rsp) {
    invariant(rsp != nullptr);
    EnsureInited();

    ClientContext context;
    std::chrono::system_clock::time_point deadline =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context.set_deadline(deadline);
    Status s = stub_->ChangeRouteRs(&context, req, rsp);

    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() == ERR_MASTER_NOT_PRIMARY) {
        LOG(0)<<"MasterClient::ChangeRouteRs failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    } else {
        return s;
    }

    ReInit();

    ClientContext context1;
    std::chrono::system_clock::time_point deadline1 =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context1.set_deadline(deadline1);
    s = stub_->ChangeRouteRs(&context1, req, rsp);
    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() != 0) {
        LOG(0)<<"MasterClient::ChangeRouteRs failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    }
    return s;
}

Status MasterClient::GetClusterRoutes(const masterproto::GetClusterRoutesReq& req, masterproto::GetClusterRoutesRsp* rsp) {
    invariant(rsp != nullptr);
    EnsureInited();

    ClientContext context;
    std::chrono::system_clock::time_point deadline =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context.set_deadline(deadline);
    Status s = stub_->GetClusterRoutes(&context, req, rsp);

    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() == ERR_MASTER_NOT_PRIMARY) {
        LOG(0)<<"MasterClient::GetClusterRoutes failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    } else {
        return s;
    }

    ReInit();

    ClientContext context1;
    std::chrono::system_clock::time_point deadline1 =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context1.set_deadline(deadline1);
    s = stub_->GetClusterRoutes(&context1, req, rsp);
    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() != 0) {
        LOG(0)<<"MasterClient::GetClusterRoutes failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    }
    return s;
}

Status MasterClient::GetIntersectMigrateJobs(const masterproto::GetIntersectMigrateJobsReq& req, masterproto::GetIntersectMigrateJobsRsp* rsp) {
    invariant(rsp != nullptr);
    EnsureInited();

    ClientContext context;
    std::chrono::system_clock::time_point deadline =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context.set_deadline(deadline);
    Status s = stub_->GetIntersectMigrateJobs(&context, req, rsp);

    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() == ERR_MASTER_NOT_PRIMARY) {
        LOG(0)<<"MasterClient::GetIntersectMigrateJobs failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    } else {
        return s;
    }

    ReInit();

    ClientContext context1;
    std::chrono::system_clock::time_point deadline1 =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context1.set_deadline(deadline1);
    s = stub_->GetIntersectMigrateJobs(&context1, req, rsp);
    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() != 0) {
        LOG(0)<<"MasterClient::GetIntersectMigrateJobs failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    }
    return s;
}

Status MasterClient::GetClusterInfo(const masterproto::GetClusterInfoReq& req, masterproto::GetClusterInfoRsp* rsp) {
    invariant(rsp != nullptr);
    EnsureInited();

    ClientContext context;
    std::chrono::system_clock::time_point deadline =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context.set_deadline(deadline);
    Status s = stub_->GetClusterInfo(&context, req, rsp);

    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() == ERR_MASTER_NOT_PRIMARY) {
        LOG(0)<<"MasterClient::GetClusterInfo failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    } else {
        return s;
    }

    ReInit();

    ClientContext context1;
    std::chrono::system_clock::time_point deadline1 =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context1.set_deadline(deadline1);
    s = stub_->GetClusterInfo(&context1, req, rsp);
    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() != 0) {
        LOG(0)<<"MasterClient::GetClusterInfo failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    }
    return s;
}

Status MasterClient::SetProxyNotifyInfo(const masterproto::SetProxyNotifyInfoReq& req, masterproto::SetProxyNotifyInfoRsp* rsp) {
    invariant(rsp != nullptr);
    EnsureInited();

    ClientContext context;
    std::chrono::system_clock::time_point deadline =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context.set_deadline(deadline);
    Status s = stub_->SetProxyNotifyInfo(&context, req, rsp);

    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() == ERR_MASTER_NOT_PRIMARY) {
        LOG(0)<<"MasterClient::SetProxyNotifyInfo failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    } else {
        return s;
    }

    ReInit();

    ClientContext context1;
    std::chrono::system_clock::time_point deadline1 =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context1.set_deadline(deadline1);
    s = stub_->SetProxyNotifyInfo(&context1, req, rsp);
    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() != 0) {
        LOG(0)<<"MasterClient::SetProxyNotifyInfo failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    }
    return s;
}

Status MasterClient::GetProxyNotifyInfo(const masterproto::GetProxyNotifyInfoReq& req, masterproto::GetProxyNotifyInfoRsp* rsp) {
    invariant(rsp != nullptr);
    EnsureInited();

    ClientContext context;
    std::chrono::system_clock::time_point deadline =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context.set_deadline(deadline);
    Status s = stub_->GetProxyNotifyInfo(&context, req, rsp);

    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() == ERR_MASTER_NOT_PRIMARY) {
        LOG(0)<<"MasterClient::GetProxyNotifyInfo failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    } else {
        return s;
    }

    ReInit();

    ClientContext context1;
    std::chrono::system_clock::time_point deadline1 =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context1.set_deadline(deadline1);
    s = stub_->GetProxyNotifyInfo(&context1, req, rsp);
    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() != 0) {
        LOG(0)<<"MasterClient::GetProxyNotifyInfo failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    }
    return s;
}

Status MasterClient::SetVersionInfo(const masterproto::SetVersionReq& req, masterproto::SetVersionRsp* rsp) {
    invariant(rsp != nullptr);
    EnsureInited();

    ClientContext context;
    std::chrono::system_clock::time_point deadline =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context.set_deadline(deadline);
    Status s = stub_->SetVersionInfo(&context, req, rsp);

    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() == ERR_MASTER_NOT_PRIMARY) {
        LOG(0)<<"MasterClient::SetVersionInfo failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    } else {
        return s;
    }

    ReInit();

    ClientContext context1;
    std::chrono::system_clock::time_point deadline1 =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context1.set_deadline(deadline1);
    s = stub_->SetVersionInfo(&context1, req, rsp);
    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() != 0) {
        LOG(0)<<"MasterClient::SetVersionInfo failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    }
    return s;
}

Status MasterClient::GetVersionInfo(const masterproto::GetVersionReq& req, masterproto::GetVersionRsp* rsp) {
    invariant(rsp != nullptr);
    EnsureInited();

    ClientContext context;
    std::chrono::system_clock::time_point deadline =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context.set_deadline(deadline);
    Status s = stub_->GetVersionInfo(&context, req, rsp);

    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() == ERR_MASTER_NOT_PRIMARY) {
        LOG(0)<<"MasterClient::GetVersionInfo failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    } else {
        return s;
    }

    ReInit();

    ClientContext context1;
    std::chrono::system_clock::time_point deadline1 =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context1.set_deadline(deadline1);
    s = stub_->GetVersionInfo(&context1, req, rsp);
    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() != 0) {
        LOG(0)<<"MasterClient::GetVersionInfo failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    }
    return s;
}

Status MasterClient::ResizeOplog(const masterproto::ResizeOplogReq& req, masterproto::ResizeOplogRsp* rsp) {
    invariant(rsp != nullptr);
    EnsureInited();

    ClientContext context;
    std::chrono::system_clock::time_point deadline =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context.set_deadline(deadline);
    Status s = stub_->ResizeOplog(&context, req, rsp);

    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() == ERR_MASTER_NOT_PRIMARY) {
        LOG(0)<<"MasterClient::ResizeOplog failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    } else {
        return s;
    }

    ReInit();

    ClientContext context1;
    std::chrono::system_clock::time_point deadline1 =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context1.set_deadline(deadline1);
    s = stub_->ResizeOplog(&context1, req, rsp);
    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() != 0) {
        LOG(0)<<"MasterClient::ResizeOplog failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    }
    return s;
}

Status MasterClient::RestartContainer(const masterproto::RestartContainerReq& req, masterproto::RestartContainerRsp* rsp) {
    invariant(rsp != nullptr);
    EnsureInited();

    ClientContext context;
    std::chrono::system_clock::time_point deadline =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context.set_deadline(deadline);
    Status s = stub_->RestartContainer(&context, req, rsp);

    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() == ERR_MASTER_NOT_PRIMARY) {
        LOG(0)<<"MasterClient::RestartContainer failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    } else {
        return s;
    }

    ReInit();

    ClientContext context1;
    std::chrono::system_clock::time_point deadline1 =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context1.set_deadline(deadline1);
    s = stub_->RestartContainer(&context1, req, rsp);
    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() != 0) {
        LOG(0)<<"MasterClient::RestartContainer failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    }
    return s;
}

Status MasterClient::SetClusterMaxConns(const masterproto::SetClusterMaxConnsReq& req, masterproto::SetClusterMaxConnsRsp* rsp) {
    invariant(rsp != nullptr);
    EnsureInited();

    ClientContext context;
    std::chrono::system_clock::time_point deadline =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context.set_deadline(deadline);
    Status s = stub_->SetClusterMaxConns(&context, req, rsp);

    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() == ERR_MASTER_NOT_PRIMARY) {
        LOG(0)<<"MasterClient::SetClusterMaxConns failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    } else {
        return s;
    }

    ReInit();

    ClientContext context1;
    std::chrono::system_clock::time_point deadline1 =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context1.set_deadline(deadline1);
    s = stub_->SetClusterMaxConns(&context1, req, rsp);
    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() != 0) {
        LOG(0)<<"MasterClient::SetClusterMaxConns failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    }
    return s;
}

Status MasterClient::GetRegionBackupConfig(const masterproto::GetRegionBackupConfigReq& req, masterproto::GetRegionBackupConfigRsp* rsp) {
    invariant(rsp != nullptr);
    EnsureInited();

    ClientContext context;
    std::chrono::system_clock::time_point deadline =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context.set_deadline(deadline);
    Status s = stub_->GetRegionBackupConfig(&context, req, rsp);

    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() == ERR_MASTER_NOT_PRIMARY) {
        LOG(0)<<"MasterClient::GetRegionBackupConfig failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    } else {
        return s;
    }

    ReInit();

    ClientContext context1;
    std::chrono::system_clock::time_point deadline1 =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context1.set_deadline(deadline1);
    s = stub_->GetRegionBackupConfig(&context1, req, rsp);
    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() != 0) {
        LOG(0)<<"MasterClient::GetRegionBackupConfig failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    }
    return s;
}

Status MasterClient::SetRegionBackupConfig(const masterproto::SetRegionBackupConfigReq& req, masterproto::SetRegionBackupConfigRsp* rsp) {
    invariant(rsp != nullptr);
    EnsureInited();

    ClientContext context;
    std::chrono::system_clock::time_point deadline =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context.set_deadline(deadline);
    Status s = stub_->SetRegionBackupConfig(&context, req, rsp);

    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() == ERR_MASTER_NOT_PRIMARY) {
        LOG(0)<<"MasterClient::SetRegionBackupConfig failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    } else {
        return s;
    }

    ReInit();

    ClientContext context1;
    std::chrono::system_clock::time_point deadline1 =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context1.set_deadline(deadline1);
    s = stub_->SetRegionBackupConfig(&context1, req, rsp);
    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() != 0) {
        LOG(0)<<"MasterClient::SetRegionBackupConfig failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    }
    return s;
}

Status MasterClient::GetClusterBackupConfig(const masterproto::GetClusterBackupConfigReq& req, masterproto::GetClusterBackupConfigRsp* rsp) {
    invariant(rsp != nullptr);
    EnsureInited();

    ClientContext context;
    std::chrono::system_clock::time_point deadline =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context.set_deadline(deadline);
    Status s = stub_->GetClusterBackupConfig(&context, req, rsp);

    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() == ERR_MASTER_NOT_PRIMARY) {
        LOG(0)<<"MasterClient::GetClusterBackupConfig failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    } else {
        return s;
    }

    ReInit();

    ClientContext context1;
    std::chrono::system_clock::time_point deadline1 =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context1.set_deadline(deadline1);
    s = stub_->GetClusterBackupConfig(&context1, req, rsp);
    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() != 0) {
        LOG(0)<<"MasterClient::GetClusterBackupConfig failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    }
    return s;
}

Status MasterClient::SetClusterBackupConfig(const masterproto::SetClusterBackupConfigReq& req, masterproto::SetClusterBackupConfigRsp* rsp) {
    invariant(rsp != nullptr);
    EnsureInited();

    ClientContext context;
    std::chrono::system_clock::time_point deadline =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context.set_deadline(deadline);
    Status s = stub_->SetClusterBackupConfig(&context, req, rsp);

    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() == ERR_MASTER_NOT_PRIMARY) {
        LOG(0)<<"MasterClient::SetClusterBackupConfig failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    } else {
        return s;
    }

    ReInit();

    ClientContext context1;
    std::chrono::system_clock::time_point deadline1 =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context1.set_deadline(deadline1);
    s = stub_->SetClusterBackupConfig(&context1, req, rsp);
    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() != 0) {
        LOG(0)<<"MasterClient::SetClusterBackupConfig failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    }
    return s;
}

Status MasterClient::DelClusterBackupConfig(const masterproto::DelClusterBackupConfigReq& req, masterproto::DelClusterBackupConfigRsp* rsp) {
    invariant(rsp != nullptr);
    EnsureInited();

    ClientContext context;
    std::chrono::system_clock::time_point deadline =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context.set_deadline(deadline);
    Status s = stub_->DelClusterBackupConfig(&context, req, rsp);

    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() == ERR_MASTER_NOT_PRIMARY) {
        LOG(0)<<"MasterClient::DelClusterBackupConfig failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    } else {
        return s;
    }

    ReInit();

    ClientContext context1;
    std::chrono::system_clock::time_point deadline1 =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context1.set_deadline(deadline1);
    s = stub_->DelClusterBackupConfig(&context1, req, rsp);
    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() != 0) {
        LOG(0)<<"MasterClient::DelClusterBackupConfig failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    }
    return s;
}

Status MasterClient::GetClusterRoutesRaw(const masterproto::GetClusterRoutesRawReq& req, masterproto::GetClusterRoutesRawRsp* rsp) {
    invariant(rsp != nullptr);
    EnsureInited();

    ClientContext context;
    std::chrono::system_clock::time_point deadline =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context.set_deadline(deadline);
    Status s = stub_->GetClusterRoutesRaw(&context, req, rsp);

    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() == ERR_MASTER_NOT_PRIMARY) {
        LOG(0)<<"MasterClient::GetClusterRoutesRaw failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    } else {
        return s;
    }

    ReInit();

    ClientContext context1;
    std::chrono::system_clock::time_point deadline1 =
        std::chrono::system_clock::now() + std::chrono::seconds(3);
    context1.set_deadline(deadline1);
    s = stub_->GetClusterRoutesRaw(&context1, req, rsp);
    // TODO(deyukong): shouldnt dependent on the remote side
    // although it should not happen
    if (s.ok()) {
        invariant(rsp->has_header());
    }
    if (!s.ok() || rsp->header().ret_code() != 0) {
        LOG(0)<<"MasterClient::GetClusterRoutesRaw failed:"
              <<int(s.error_code())
              <<":"
              <<s.error_message()
              <<":"
              <<rsp->DebugString();
    }
    return s;
}

}