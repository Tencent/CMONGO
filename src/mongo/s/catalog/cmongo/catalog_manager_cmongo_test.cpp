//catalog_manager_cmongo_test.cpp
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
#include "mongo/unittest/unittest.h"
#include "mongo/base/status_with.h"
#include "mongo/s/catalog/cmongo/catalog_manager_cmongo_test.h"
#include "mongo/s/chunk_manager.h"
#include "mongo/s/mongos_options.h"

namespace mongo{

// fake, void link error
MongosGlobalParams mongosGlobalParams;

Status CatalogManagerCmongoTest::injectRoute(const masterproto::GetClusterRoutesRsp& pbobj, CatalogManagerCmongo* value) {
	return value->_parseFromPBClusterRoutes(pbobj);
}

Status CatalogManagerCmongoTest::injectShard(const masterproto::GetClusterInfoRsp& pbobj, CatalogManagerCmongo* value) {
	return value->_parseFromPBClusterInfo(pbobj);
}

void init(CatalogManagerCmongo *mock) {
	std::unique_ptr<CatalogManagerCmongoTest> handler(new CatalogManagerCmongoTest());
	masterproto::GetClusterInfoRsp obj1;
	masterproto::ClusterInfo* cluster_info = new masterproto::ClusterInfo();
	obj1.set_allocated_cluster_info(cluster_info);
	masterproto::ReplicateSetInfo *info = cluster_info->add_rs_list();
	info->set_rsname("testcluster_0");
	masterproto::ContainerInfo *c_info = info->add_mongod_list();
	c_info->set_cluster_id("testcluster");
	c_info->set_replicate_set("testcluster_0");
	c_info->set_container_name("127.0.0.1:7000:12345");
	info = cluster_info->add_rs_list();
	info->set_rsname("testcluster_1");
	c_info = info->add_mongod_list();
	c_info->set_cluster_id("testcluster");
	c_info->set_replicate_set("testcluster_1");
	c_info->set_container_name("127.0.0.1:7001:12345");
	invariant(handler.get()->injectShard(obj1, mock).isOK());

	masterproto::GetClusterRoutesRsp obj;
	masterproto::TableRoutes* routes = obj.add_routes();
	routes->set_ns("testdb.testtable");
	// shardkey of {'a':1, 'b':1, 'c':1}
	routes->add_shard_key("a");
	routes->add_shard_key("b");
	routes->add_shard_key("c");
	routes->set_route_type("sharded");
	for (size_t i = 0; i < 8192; i++) {
		masterproto::RouteChunk* chunk = routes->add_routes();
		if (i < 4096) {
			chunk->set_shard_name("testcluster_0");
		} else {
			chunk->set_shard_name("testcluster_1");
		}
		chunk->set_state(0);
	}
	invariant(handler.get()->injectRoute(obj, mock).isOK());
}

}

using namespace mongo;

TEST(CATALOG_MANAGER_CMONGO_TEST, route) {
	masterproto::GetClusterRoutesRsp obj;
	masterproto::TableRoutes* routes = obj.add_routes();

	routes->set_ns("testdb.testtable");

	// shardkey of {'a':1, 'b':1, 'c':1}
	routes->add_shard_key("a");
	routes->add_shard_key("b");
	routes->add_shard_key("c");
	routes->set_route_type("sharded");
	for (size_t i = 0; i < 8192; i++) {
		masterproto::RouteChunk* chunk = routes->add_routes();
		if (i < 4096) {
			chunk->set_shard_name("testcluster_0");
		} else {
			chunk->set_shard_name("testcluster_1");
		}
		chunk->set_state(0);
	}
	std::unique_ptr<CatalogManagerCmongoTest> handler(new CatalogManagerCmongoTest());
	std::unique_ptr<CatalogManagerCmongo> mock(new CatalogManagerCmongo("", "testcluster", true));

	masterproto::GetClusterInfoRsp obj1;
	masterproto::ClusterInfo* cluster_info = new masterproto::ClusterInfo();
	obj1.set_allocated_cluster_info(cluster_info);
	masterproto::ReplicateSetInfo *info = cluster_info->add_rs_list();
	info->set_rsname("testcluster_0");
	masterproto::ContainerInfo *c_info = info->add_mongod_list();
	c_info->set_cluster_id("testcluster");
	c_info->set_replicate_set("testcluster_0");
	c_info->set_container_name("127.0.0.1:7000:12345");

	info = cluster_info->add_rs_list();
	info->set_rsname("testcluster_1");
	c_info = info->add_mongod_list();
	c_info->set_cluster_id("testcluster");
	c_info->set_replicate_set("testcluster_1");
	c_info->set_container_name("127.0.0.1:7001:12345");
	ASSERT_TRUE(handler.get()->injectShard(obj1, mock.get()).isOK());
	ASSERT_TRUE(handler.get()->injectRoute(obj, mock.get()).isOK());

	auto allShards = mock->getAllShards(nullptr);
	ASSERT_TRUE(allShards.getStatus().isOK());
	ASSERT_EQUALS(allShards.getValue().value.size(), 2);
}

TEST(CATALOG_MANAGER_CMONGO_TEST, DbConfigSharded) {
	std::unique_ptr<CatalogManagerCmongo> mock(new CatalogManagerCmongo("", "testcluster", true));
	init(mock.get());
	auto dbConfig = mock->getDbConfig(nullptr, "testdb");
	ASSERT_TRUE(dbConfig.getStatus().isOK());
	ASSERT_TRUE(dbConfig.getValue()->isSharded("testdb.testtable"));
	// testdb.testtable1 not exists
	ASSERT_FALSE(dbConfig.getValue()->isSharded("testdb.testtable1"));

	std::shared_ptr<ChunkManager> manager;
	std::shared_ptr<Shard> primary;
	dbConfig.getValue()->getChunkManagerOrPrimary(nullptr,
			"testdb.testtable",
			manager,
			primary);
	ASSERT_FALSE(manager.get() == nullptr);
	ASSERT_TRUE(primary.get() == nullptr);

	std::set<std::string> set_ns;
	dbConfig.getValue()->getAllShardedCollections(set_ns);
	ASSERT_EQUALS(set_ns.size(), 1);
	ASSERT_EQUALS(*(set_ns.begin()), "testdb.testtable");

	BSONObj obj = BSON("a" << 1 << "b" << 1 << "c" << 1);
	std::set<ShardId> shards;
	manager->getShardIdsForQuery(nullptr, obj, &shards);
	ASSERT_EQUALS(shards.size(), 1);

	BSONObj obj1 = BSON("a" << 1 << "b" << 1);
	shards.clear();
	manager->getShardIdsForQuery(nullptr, obj1, &shards);
	ASSERT_EQUALS(shards.size(), 2);
	ASSERT_TRUE(shards.find("testcluster_0") != shards.end());
	ASSERT_TRUE(shards.find("testcluster_1") != shards.end());

	ASSERT_TRUE(manager->numChunks() == 8192);

	const ChunkMap& chunks = manager->getChunkMap();
	for (ssize_t i = 0; i < 8192; i++) {
		BSONObj key = BSON("_" << int(i));
		auto it = chunks.find(key);
		ASSERT_TRUE(it != chunks.end());
		ASSERT_TRUE(it->second->getMin() == key);
		ASSERT_TRUE(it->second->getMax() == key);
		if (i < 4096) {
			ASSERT_TRUE(it->second->getShardId() == "testcluster_0");
		} else {
			ASSERT_TRUE(it->second->getShardId() == "testcluster_1");
		}
	}
}
