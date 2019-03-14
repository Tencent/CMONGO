/*

 Copyright (C) 2017 wolf(deyukong@tencent.com)

*/

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kNetwork

#include <iostream>
#include <vector>
#include <memory>
#include <grpc/grpc.h>
#include <grpc++/channel.h>
#include <grpc++/client_context.h>
#include <grpc++/create_channel.h>
#include <grpc++/security/credentials.h>
#include "mongo/grpc/grpc_client.h"
#include "mongo/grpc/cmongoproto/cmongo.pb.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/log.h"


using std::vector;
using std::string;
using std::shared_ptr;
using std::unique_ptr;

namespace grpc {

MasterClient::MasterClient(const vector<string>& etcd_endpoints)
	:meta_handler_(new etcd::EtcdWrapper(etcd_endpoints)),
	inited_(false) {
}

MasterClient::MasterClient(const std::string& etcd_endpoints)
	:meta_handler_(new etcd::EtcdWrapper(etcd_endpoints)),
	inited_(false) {
}

void MasterClient::EnsureInitedInLock() {
	string master_host = meta_handler_->GetValue("/master/primary");
	LOG(0)<<"MasterClient::EnsureInited with master_host:" << master_host;
	shared_ptr<Channel> chan = CreateChannel(
		master_host,
		InsecureChannelCredentials()
	);
	// the old one will be destroyed after the new one is assigned, it is guarenteed by unique_ptr
	// TODO(deyukong): stub options?
	stub_ = masterproto::Master::NewStub(chan);
	inited_ = true;
}

void MasterClient::EnsureInited() {
	mongo::stdx::lock_guard<mongo::stdx::mutex> lk(_mutex);
	if (inited_) {
		return;
	}
	EnsureInitedInLock();
}

void MasterClient::ReInit() {
	mongo::stdx::lock_guard<mongo::stdx::mutex> lk(_mutex);
	inited_ = false;
	EnsureInitedInLock();
}

}
