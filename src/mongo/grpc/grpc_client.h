/*

 Copyright (C) 2017 wolf(deyukong@tencent.com)

*/

#ifndef __MONGO_GRPC_GRPC_CLIENT_H__
#define __MONGO_GRPC_GRPC_CLIENT_H__

#include <vector>
#include <iostream>
#include "mongo/grpc/masterproto/master.pb.h"
#include "mongo/grpc/masterproto/master.grpc.pb.h"
#include "mongo/util/etcd_wrapper.h"
#include "mongo/stdx/mutex.h"

namespace grpc {

/**
    NOTE(deyukong): every api(except the constructor) is threadsafe
	every api may throw exception
*/
class MasterClient {
public:
    MasterClient(const std::vector<std::string>& etcd_endpoints);
    MasterClient(const std::string& etcd_endpoints);
	
	Status AddResource(const masterproto::AddResourceReq& req, masterproto::AddResourceRsp* rsp);
	
	Status BlockResource(const masterproto::BlockResourceReq& req, masterproto::BlockResourceRsp* rsp);
	
	Status UnblockResource(const masterproto::UnblockResourceReq& req, masterproto::UnblockResourceRsp* rsp);
	
	Status DelResource(const masterproto::DelResourceReq& req, masterproto::DelResourceRsp* rsp);
	
	Status QueryResource(const masterproto::QueryResourceReq& req, masterproto::QueryResourceRsp* rsp);
	
	Status MigrateContainer(const masterproto::MigrateContainerReq& req, masterproto::MigrateContainerRsp* rsp);
	
	Status MigrateMachine(const masterproto::MigrateMachineReq& req, masterproto::MigrateMachineRsp* rsp);
	
	Status QueryMachine(const masterproto::QueryMachineReq& req, masterproto::QueryMachineRsp* rsp);
	
	Status AddContainer(const masterproto::AddContainerReq& req, masterproto::AddContainerRsp* rsp);
	
	Status DelContainer(const masterproto::DelContainerReq& req, masterproto::DelContainerRsp* rsp);
	
	Status AddContainerAsync(const masterproto::AddContainerAsyncReq& req, masterproto::AddContainerAsyncRsp* rsp);
	
	Status CreateCluster(const masterproto::CreateClusterReq& req, masterproto::CreateClusterRsp* rsp);
	
	Status UpgradeClusterModule(const masterproto::UpgradeClusterModuleReq& req, masterproto::UpgradeClusterModuleRsp* rsp);
	
	Status InfoCluster(const masterproto::InfoClusterReq& req, masterproto::InfoClusterRsp* rsp);
	
	Status DelCluster(const masterproto::DelClusterReq& req, masterproto::DelClusterRsp* rsp);
	
	Status ListCluster(const masterproto::ListClusterReq& req, masterproto::ListClusterRsp* rsp);
	
	Status ChangePassword(const masterproto::ChPasswdReq& req, masterproto::ChPasswdRsp* rsp);
	
	Status JobStatus(const masterproto::JobStatusReq& req, masterproto::JobStatusRsp* rsp);
	
	Status ResizeCluster(const masterproto::ResizeClusterReq& req, masterproto::ResizeClusterRsp* rsp);
	
	Status BackupCluster(const masterproto::BackupClusterReq& req, masterproto::BackupClusterRsp* rsp);
	
	Status QueryBackupList(const masterproto::QueryBackupListReq& req, masterproto::QueryBackupListRsp* rsp);
	
	Status CheckRestoreTs(const masterproto::CheckRestoreTsReq& req, masterproto::CheckRestoreTsRsp* rsp);
	
	Status RestoreCluster(const masterproto::RestoreClusterReq& req, masterproto::RestoreClusterRsp* rsp);
	
	Status ExchangeCluster(const masterproto::ExchangeClusterReq& req, masterproto::ExchangeClusterRsp* rsp);
	
	Status IsolationCluster(const masterproto::IsolationClusterReq& req, masterproto::IsolationClusterRsp* rsp);
	
	Status AddReplicateSet(const masterproto::AddRsReq& req, masterproto::AddRsRsp* rsp);
	
	Status DelReplicateSet(const masterproto::DelRsReq& req, masterproto::DelRsRsp* rsp);
	
	Status QueryOpLogStatus(const masterproto::OpLogStatusReq& req, masterproto::OpLogStatusRsp* rsp);
	
	Status ShowDb(const masterproto::ShowDbReq& req, masterproto::ShowDbRsp* rsp);
	
	Status ShowColls(const masterproto::ShowCollsReq& req, masterproto::ShowCollsRsp* rsp);
	
	Status GetSlowQuery(const masterproto::GetSlowQueryReq& req, masterproto::GetSlowQueryRsp* rsp);
	
	Status QrySlowInfo(const masterproto::QrySlowInfoReq& req, masterproto::QrySlowInfoRsp* rsp);
	
	Status CreateUser(const masterproto::CreateUserReq& req, masterproto::CreateUserRsp* rsp);
	
	Status QueryUserList(const masterproto::QueryUserListReq& req, masterproto::QueryUserListRsp* rsp);
	
	Status DeleteUser(const masterproto::DeleteUserReq& req, masterproto::DeleteUserRsp* rsp);
	
	Status SetUserAuth(const masterproto::SetUserAuthReq& req, masterproto::SetUserAuthRsp* rsp);
	
	Status CreateTable(const masterproto::CreateTableReq& req, masterproto::CreateTableRsp* rsp);
	
	Status DropTable(const masterproto::DropTableReq& req, masterproto::DropTableRsp* rsp);
	
	Status DropDatabase(const masterproto::DropDatabaseReq& req, masterproto::DropDatabaseRsp* rsp);
	
	Status EnsureIndex(const masterproto::EnsureIndexReq& req, masterproto::EnsureIndexRsp* rsp);
	
	Status DropIndex(const masterproto::DropIndexReq& req, masterproto::DropIndexRsp* rsp);
	
	Status DropIndexName(const masterproto::DropIndexNameReq& req, masterproto::DropIndexNameRsp* rsp);
	
	Status ReportMigrateInfo(const masterproto::ReportMigrateInfoReq& req, masterproto::ReportMigrateInfoRsp* rsp);
	
	Status Migrate(const masterproto::MigrateReq& req, masterproto::MigrateRsp* rsp);
	
	Status CleanMigrate(const masterproto::CleanMigrateReq& req, masterproto::CleanMigrateRsp* rsp);
	
	Status GetMigrateStatus(const masterproto::GetMigrateStatusReq& req, masterproto::GetMigrateStatusRsp* rsp);
	
	Status IsTableInMigrating(const masterproto::IsTableInMigratingReq& req, masterproto::IsTableInMigratingRsp* rsp);
	
	Status RestoreFiles(const masterproto::RestoreFilesReq& req, masterproto::RestoreFilesRes* rsp);
	
	Status GenBalanceTask(const masterproto::GenBalanceTaskReq& req, masterproto::GenBalanceTaskRsp* rsp);
	
	Status Balance(const masterproto::BalanceReq& req, masterproto::BalanceRsp* rsp);
	
	Status ChangeRouteStat(const masterproto::ChangeRouteStateReq& req, masterproto::ChangeRouteStateRsp* rsp);
	
	Status ChangeRouteRs(const masterproto::ChangeRouteRsReq& req, masterproto::ChangeRouteRsRsp* rsp);
	
	Status GetClusterRoutes(const masterproto::GetClusterRoutesReq& req, masterproto::GetClusterRoutesRsp* rsp);
	
	Status GetIntersectMigrateJobs(const masterproto::GetIntersectMigrateJobsReq& req, masterproto::GetIntersectMigrateJobsRsp* rsp);
	
	Status GetClusterInfo(const masterproto::GetClusterInfoReq& req, masterproto::GetClusterInfoRsp* rsp);
	
	Status SetProxyNotifyInfo(const masterproto::SetProxyNotifyInfoReq& req, masterproto::SetProxyNotifyInfoRsp* rsp);
	
	Status GetProxyNotifyInfo(const masterproto::GetProxyNotifyInfoReq& req, masterproto::GetProxyNotifyInfoRsp* rsp);
	
	Status SetVersionInfo(const masterproto::SetVersionReq& req, masterproto::SetVersionRsp* rsp);
	
	Status GetVersionInfo(const masterproto::GetVersionReq& req, masterproto::GetVersionRsp* rsp);
	
	Status ResizeOplog(const masterproto::ResizeOplogReq& req, masterproto::ResizeOplogRsp* rsp);
	
	Status RestartContainer(const masterproto::RestartContainerReq& req, masterproto::RestartContainerRsp* rsp);
	
	Status SetClusterMaxConns(const masterproto::SetClusterMaxConnsReq& req, masterproto::SetClusterMaxConnsRsp* rsp);
	
	Status GetRegionBackupConfig(const masterproto::GetRegionBackupConfigReq& req, masterproto::GetRegionBackupConfigRsp* rsp);
	
	Status SetRegionBackupConfig(const masterproto::SetRegionBackupConfigReq& req, masterproto::SetRegionBackupConfigRsp* rsp);
	
	Status GetClusterBackupConfig(const masterproto::GetClusterBackupConfigReq& req, masterproto::GetClusterBackupConfigRsp* rsp);
	
	Status SetClusterBackupConfig(const masterproto::SetClusterBackupConfigReq& req, masterproto::SetClusterBackupConfigRsp* rsp);
	
	Status DelClusterBackupConfig(const masterproto::DelClusterBackupConfigReq& req, masterproto::DelClusterBackupConfigRsp* rsp);
	
	Status GetClusterRoutesRaw(const masterproto::GetClusterRoutesRawReq& req, masterproto::GetClusterRoutesRawRsp* rsp);
	
private:
    void EnsureInited();
	void EnsureInitedInLock();
    void ReInit();
    std::unique_ptr<etcd::EtcdWrapper> meta_handler_;
    std::unique_ptr<masterproto::Master::Stub> stub_;
    bool inited_;
    mongo::stdx::mutex _mutex;
};
}
#endif //__MONGO_GRPC_GRPC_CLIENT_H__