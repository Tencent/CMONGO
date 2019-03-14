/*

 The MIT License (MIT)

 Copyright (C) 2017 DeyuKong

 Permission is hereby granted, free of charge, to any person obtaining a copy
 of this software and associated documentation files (the "Software"), to deal
 in the Software without restriction, including without limitation the rights
 to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 copies of the Software, and to permit persons to whom the Software is
 furnished to do so, subject to the following conditions:

 The above copyright notice and this permission notice shall be included in
 all copies or substantial portions of the Software.

 THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 THE SOFTWARE.

*/

#ifndef __MONGO_UTIL_CMONGO_SHARD_HELPERS_H_INCLUDED__
#define __MONGO_UTIL_CMONGO_SHARD_HELPERS_H_INCLUDED__

#include <string>

#include "mongo/bson/oid.h"
#include "mongo/base/status-inl.h"
#include "mongo/base/status_with.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonelement.h"

namespace cmongo {
// TODO(deyukong): make sure how many times actually strings' constructor 
// is called, it should be ok since c++11's rvalue-reference can do rvo
std::string formatBool(bool);
std::string formatFloat32(float);
std::string formatFloat64(double);
std::string formatOID(const mongo::OID&);
std::string formatBinary(const char *buf, size_t size);
uint32_t calcChunkId(const std::string&);
mongo::Status validateShardKeys(const mongo::BSONObj& sk);
mongo::StatusWith<uint32_t> calcChunkId(const mongo::BSONObj& sk);
std::string cmongoString(const mongo::BSONElement& obj);
bool isCMongoSKType(const mongo::BSONElement& obj);
mongo::Status createKeyFile(const std::string& path, const std::string& clustername);
mongo::StatusWith<std::string> RawRouteStringToPBBytes(const std::string& ns, const std::string& raw_str);
void forkServerOrDie();
}
#endif //__MONGO_UTIL_CMONGO_SHARD_HELPERS_H_INCLUDED__
