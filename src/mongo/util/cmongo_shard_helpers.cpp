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
#include "mongo/util/cgo/export.h"

#include <string>
#include "cmongo_shard_helpers.h"
#include "mongo/bson/oid.h"
#include "mongo/base/data_view.h"
#include "mongo/util/mongoutils/str.h"
#include "mongo/util/stringutils.h"
#include "mongo/base/status-inl.h"
#include "mongo/base/status_with.h"

using namespace mongo;

namespace cmongo {

std::string cmongoString(const BSONElement& obj) {
	switch(obj.type()) {
		case mongo::NumberDouble:
			return cmongo::formatFloat64(obj.number());
		case mongo::String:
			return obj.str();
		case mongo::BinData: {
			int len;
			const char *data = obj.binDataClean(len);
			return cmongo::formatBinary(data, len);
		}
		case mongo::jstOID:
			return cmongo::formatOID(obj.__oid());
		case mongo::Bool:
			return cmongo::formatBool(obj.boolean());
		case mongo::NumberInt: {
			StringBuilder s;
			s << obj._numberInt();
			return s.str();
		}
		case mongo::NumberLong: {
			StringBuilder s;
			s << obj._numberLong();
			return s.str();
		}
		default:
			invariant(0);
	}
}

bool isCMongoSKType(const BSONElement& obj) {
	switch (obj.type()) {
		case mongo::BinData: {
			if (obj.binDataType() == BinDataGeneral || obj.binDataType()  == ByteArrayDeprecated) {
				return true;
			} else {
				return false;
			}
		}
		case mongo::NumberDouble:
		case mongo::String:
		case mongo::jstOID:
		case mongo::Bool:
		case mongo::NumberInt:
		case mongo::NumberLong:
			return true;
		default:
			return false;
	}
}

std::string formatBool(bool v) {
	GoUint8 ret_flag = 0;
	const size_t bufsize = 1024;
	std::unique_ptr<char []> buf(new char[bufsize]);
	memset(buf.get(), 0, bufsize);
	GoSlice input1 = {static_cast<void *>(buf.get()), bufsize, bufsize};
	FormatBool(v, &input1, &ret_flag);
	invariant(ret_flag);
	return std::string(buf.get());
}

std::string formatFloat32(float v) {
	GoUint8 ret_flag = 0;
	const size_t bufsize = 1024;
	std::unique_ptr<char []>  buf(new char[bufsize]);
	memset(buf.get(), 0, bufsize);
	GoSlice input1 = {static_cast<void *>(buf.get()), bufsize, bufsize};
	FormatFloat32(v, &input1, &ret_flag);
	invariant(ret_flag);
	return std::string(buf.get());
}

std::string formatFloat64(double v) {
	GoUint8 ret_flag = 0;
	const size_t bufsize = 1024;
	std::unique_ptr<char []>  buf(new char[bufsize]);
	memset(buf.get(), 0, bufsize);
	GoSlice input1 = {static_cast<void *>(buf.get()), bufsize, bufsize};
	FormatFloat64(v, &input1, &ret_flag);
	invariant(ret_flag);
	return std::string(buf.get());
}

std::string formatOID(const OID& v) {
	ConstDataView view = v.view();
	const char *bytes = view.view();
	GoString input = {bytes, OID::kOIDSize};
	GoUint8 ret_flag = 0;
	// 20 for ObjectId()
	GoInt size = OID::kOIDSize*2+22;
	std::unique_ptr<char []>  buf(new char[size]);
	memset(buf.get(), 0, size);
	GoSlice input1 = {static_cast<void *>(buf.get()), size, size};
	FormatOID(input, &input1, &ret_flag);
	invariant(ret_flag);
	return std::string(buf.get());
}

std::string formatBinary(const char *v, size_t size) {
	GoString input = {v, static_cast<GoInt>(size)};
	GoUint8 ret_flag = 0;
	// here, go format binary uses hex.Dump
	// it is a mistaken of hex.Encode
	// the firstOne outputs like hexdump -C, which is about 5 times as large as the original data
	GoInt out_size = 512;
	while(!ret_flag && out_size <= 4096) {
		std::unique_ptr<char []>  buf(new char[out_size]);
		memset(buf.get(), 0, out_size);
		GoSlice input1 = {static_cast<void *>(buf.get()), out_size, out_size};
		FormatByteArray(input, &input1, &ret_flag);
		if(ret_flag) {
			return std::string(buf.get());
		}
		out_size *= 2;
	}
	invariant(0);
	return "";
}

Status createKeyFile(const std::string& path, const std::string& cluster) {
	GoString input1 = {path.c_str(), static_cast<GoInt>(path.length())};
	GoString input2 = {cluster.c_str(), static_cast<GoInt>(cluster.length())};
	GoUint8 ret_flag = 0;
	CreateKeyFile(input1, input2, &ret_flag);
	if (!ret_flag) {
		return Status(ErrorCodes::InternalError, "create keyfile failed");
	}
	return Status::OK();
}

uint32_t calcChunkId(const std::string& v) {
	GoString s;
	s.p = v.c_str();
	s.n = v.length();
	return ChunkId(s);
}

Status validateShardKeys(const BSONObj& sk) {
	BSONObjIterator patternIt(sk);
	while(patternIt.more()) {
		BSONElement patternEl = patternIt.next();
		if (isCMongoSKType(patternEl)) {
			continue;
		}
		return Status(ErrorCodes::InternalError, str::stream()
				<< "invalid type of shard_key:" << patternEl << ",type:" << patternEl.type());
	}
	return Status::OK();
}

StatusWith<uint32_t> calcChunkId(const BSONObj& sk) {
	Status s = validateShardKeys(sk);
	if (!s.isOK()) {
		return s;
	}
	std::vector<std::string> ret;
	BSONObjIterator patternIt(sk);
	while(patternIt.more()) {
		BSONElement patternEl = patternIt.next();
		ret.push_back(cmongoString(patternEl));
	}
	std::string joined_string;
	joinStringDelim(ret, &joined_string, '_');
	return calcChunkId(joined_string);
}

//RawStringToTableRoute(ns string, rawstring string, ret *[]byte, ok *bool, errbuf *[]byte)
StatusWith<std::string> RawRouteStringToPBBytes(const std::string& ns, const std::string& raw_str) {
	GoString go_ns = {ns.c_str(), static_cast<GoInt>(ns.length())};
	GoString go_rawstring = {raw_str.c_str(), static_cast<GoInt>(raw_str.length())};

	GoUint8 ret_flag = 0;
	const size_t retbufsize = 4*1024*1024;
	std::unique_ptr<char []>  retbuf(new char[retbufsize]);
	memset(retbuf.get(), 0, retbufsize);
	GoSlice  retslice = {static_cast<void *>(retbuf.get()), retbufsize, retbufsize};

	const size_t errbufsize = 1024;
	std::unique_ptr<char []>  errbuf(new char[errbufsize]);
	memset(errbuf.get(), 0, errbufsize);
	GoSlice  errslice = {static_cast<void *>(errbuf.get()), errbufsize, errbufsize};

	GoInt64 ret_size = 0;
	RawStringToTableRoute(go_ns, go_rawstring, &retslice, &ret_flag, &ret_size, &errslice);
	if (!ret_flag) {
		return Status(ErrorCodes::InternalError, std::string(errbuf.get()));
	}
	return std::string(retbuf.get(), ret_size);
}

void forkServerOrDie() {
	GoStyleFork();
}
}
