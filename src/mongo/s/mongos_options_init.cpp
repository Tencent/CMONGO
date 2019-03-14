/*
 *    Copyright (C) 2013 10gen Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects
 *    for all of the code used other than as permitted herein. If you modify
 *    file(s) with this exception, you may extend this exception to your
 *    version of the file(s), but you are not obligated to do so. If you do not
 *    wish to do so, delete this exception statement from your version. If you
 *    delete this exception statement from all source files in the program,
 *    then also delete it in the license file.
 */

#include "mongo/s/mongos_options.h"

#include <iostream>

#include "mongo/util/options_parser/startup_option_init.h"
#include "mongo/util/options_parser/startup_options.h"
#include "mongo/util/quick_exit.h"
#include "mongo/util/options_parser/value.h"
#include "mongo/util/cmongo_shard_helpers.h"

namespace mongo {
MONGO_GENERAL_STARTUP_OPTIONS_REGISTER(MongosOptions)(InitializerContext* context) {
    return addMongosOptions(&moe::startupOptions);
}

MONGO_STARTUP_OPTIONS_VALIDATE(MongosOptions)(InitializerContext* context) {
    if (!handlePreValidationMongosOptions(moe::startupOptionsParsed, context->args())) {
        quickExit(EXIT_SUCCESS);
    }
    // Run validation, but tell the Environment that we don't want it to be set as "valid",
    // since we may be making it invalid in the canonicalization process.
    Status ret = moe::startupOptionsParsed.validate(false /*setValid*/);
    if (!ret.isOK()) {
        return ret;
    }
    ret = validateMongosOptions(moe::startupOptionsParsed);
    if (!ret.isOK()) {
        return ret;
    }
    ret = canonicalizeMongosOptions(&moe::startupOptionsParsed);
    if (!ret.isOK()) {
        return ret;
    }
    ret = moe::startupOptionsParsed.validate();
    if (!ret.isOK()) {
        return ret;
    }
    return Status::OK();
}

namespace {
using namespace mongo::optionenvironment;

std::string guessKeyfilePath(std::string pattern) {
	size_t pos = pattern.find("/log");
	if (pos == std::string::npos) {
		return "/tmp/keyfile";
	} else {
		std::string prefix = pattern.substr(0, pos);
		return prefix + "/conf/keyfile";
	}
}

Status createKeyfile(const moe::Environment& params) {
	if (!params.count("mongo.clusterName")) {
		return Status(ErrorCodes::BadValue, "cmongo mongo.clusterName not set");
	}
	if (!params.count("security.keyFile")) {
		return Status(ErrorCodes::BadValue, "cmongo security.keyFile not set");
	}
	std::string path = params["security.keyFile"].as<std::string>();
	std::string cluster = params["mongo.clusterName"].as<std::string>();
	return cmongo::createKeyFile(path, cluster);
}

Status injectCMongoOptions(moe::Environment* params) {
	params->set("systemLog.logAppend", Value(true));
	params->set("systemLog.logRotate", Value("rename"));
	params->set("systemLog.destination", Value("file"));
	params->set("systemLog.timeStampFormat", Value("ctime"));
	params->set("processManagement.fork", Value(true));
	// NOTE(deyukong): mongos panics if configDB not set, so mock it.
	params->set("sharding.configDB", Value("127.0.0.1:27017,127.0.0.1:27018,127.0.0.1:27019"));
	if (params->count("logLevel")) {
		std::string v = (*params)["logLevel"].as<std::string>();
		if (v == "info") {
			params->set("systemLog.verbosity", Value(0));
		} else {
			params->set("systemLog.verbosity", Value(1));
		}
	} else {
		return Status(ErrorCodes::BadValue, "cmongo logLevel not set");
	}
	if (params->count("logPath")) {
		std::string realPath = (*params)["logPath"].as<std::string>() + "/proxy.log";
		params->set("systemLog.path", Value(realPath));
	} else {
		return Status(ErrorCodes::BadValue, "cmongo logPath not set");
	}
	if (params->count("pidFile")) {
		params->set("processManagement.pidFilePath", (*params)["pidFile"]);
	} else {
		return Status(ErrorCodes::BadValue, "cmongo pidFile not set");
	}
	if (params->count("proxy.port")) {
		params->set("net.port", (*params)["proxy.port"]);
	} else {
		return Status(ErrorCodes::BadValue, "cmongo proxy.port not set");
	}
	if (params->count("proxy.maxConnections")) {
		params->set("net.maxIncomingConnections", (*params)["proxy.maxConnections"]);
	} else {
		return Status(ErrorCodes::BadValue, "cmongo proxy.maxConnections not set");
	}
	std::string keyfilePath = guessKeyfilePath((*params)["logPath"].as<std::string>());
	params->set("security.keyFile", Value(keyfilePath));
	params->dump();
	// TODO(deyukong): keyfile
	return Status::OK();
}
}

MONGO_INITIALIZER_GENERAL(MongosOptions_Store,
                          ("BeginStartupOptionStorage"),
                          ("EndStartupOptionStorage"))
(InitializerContext* context) {
	Status ret = injectCMongoOptions(&moe::startupOptionsParsed);
	
	if (!ret.isOK()) {
		std::cerr << ret.toString() << std::endl;
		quickExit(EXIT_BADOPTIONS);
	}

	ret = createKeyfile(moe::startupOptionsParsed);
	if (!ret.isOK()) {
		std::cerr << ret.toString() << std::endl;
		quickExit(EXIT_BADOPTIONS);
	}

    ret = storeMongosOptions(moe::startupOptionsParsed, context->args());
    if (!ret.isOK()) {
        std::cerr << ret.toString() << std::endl;
        std::cerr << "try '" << context->args()[0] << " --help' for more information" << std::endl;
        quickExit(EXIT_BADOPTIONS);
    }
    return Status::OK();
}

}  // namespace mongo
