/**
 *    Copyright (C) 2008-2015 MongoDB Inc.
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

#pragma once

#include "mongo/s/catalog/type_chunk.h"
#include "mongo/s/chunk_version.h"
#include "mongo/s/client/shard.h"
#include "mongo/util/mongoutils/str.h"

namespace mongo {

class ChunkManager;
class OperationContext;
struct WriteConcernOptions;

/**
   config.chunks
   { ns : "alleyinsider.fs.chunks" , min : {} , max : {} , server : "localhost:30001" }

   x is in a shard iff
   min <= x < max
 */
class Chunk {
public:
    enum SplitPointMode {
        // Determines the split points that will make the current chunk smaller than
        // the current chunk size setting. Gives empty result if chunk is not big enough.
        normal,

        // Will get a split which approximately splits the chunk into 2 halves,
        // regardless of the size of the chunk.
        atMedian,

        // Behaves like normal, with additional special heuristics for "top chunks"
        // (the 1 or 2 chunks in the extreme ends of the chunk key space).
        autoSplitInternal
    };

	virtual ~Chunk();

    virtual const BSONObj& getMin() const = 0;

    virtual const BSONObj& getMax() const = 0;


    // Returns true if this chunk contains the given shard key, and false otherwise
    //
    // Note: this function takes an extracted *key*, not an original document
    // (the point may be computed by, say, hashing a given field or projecting
    //  to a subset of fields).
    virtual bool containsKey(const BSONObj& shardKey) const = 0;

    //
    // chunk version support
    //

    virtual void appendShortVersion(const char* name, BSONObjBuilder& b) const = 0;

    virtual ChunkVersion getLastmod() const = 0;

    virtual void setLastmod(ChunkVersion v) = 0;

    //
    // split support
    //

    virtual long long getBytesWritten() const = 0;

    // Const since _dataWritten is mutable and a heuristic
    // TODO: Split data tracking and chunk information
    virtual void setBytesWritten(long long bytesWritten) const = 0;

    /**
     * if the amount of data written nears the max size of a shard
     * then we check the real size, and if its too big, we split
     * @return if something was split
     */
    virtual bool splitIfShould(OperationContext* txn, long dataWritten) const = 0;

    /**
     * Splits this chunk at a non-specificed split key to be chosen by the
     * mongod holding this chunk.
     *
     * @param mode
     * @param res the object containing details about the split execution
     * @param resultingSplits the number of resulting split points. Set to NULL to ignore.
     *
     * @throws UserException
     */
    virtual Status split(OperationContext* txn,
                 SplitPointMode mode,
                 size_t* resultingSplits,
                 BSONObj* res) const = 0;

    /**
     * Splits this chunk at the given key (or keys)
     *
     * @param splitPoints the vector of keys that should be used to divide this chunk
     * @param res the object containing details about the split execution
     *
     * @throws UserException
     */
    virtual Status multiSplit(OperationContext* txn,
                      const std::vector<BSONObj>& splitPoints,
                      BSONObj* res) const = 0;

    /**
     * Asks the mongod holding this chunk to find a key that approximately divides this chunk in two
     *
     * @param medianKey the key that divides this chunk, if there is one, or empty
     */
    virtual void pickMedianKey(OperationContext* txn, BSONObj& medianKey) const = 0;

    /**
     * Ask the mongod holding this chunk to figure out the split points.
     * @param splitPoints vector to be filled in
     * @param chunkSize chunk size to target in bytes
     * @param maxPoints limits the number of split points that are needed, zero is max (optional)
     * @param maxObjs limits the number of objects in each chunk, zero is as max (optional)
     */
    virtual void pickSplitVector(OperationContext* txn,
                         std::vector<BSONObj>& splitPoints,
                         long long chunkSize,
                         int maxPoints = 0,
                         int maxObjs = 0) const = 0;

    //
    // migration support
    //

    /**
     * Issues a migrate request for this chunk
     *
     * @param to shard to move this chunk to
     * @param chunSize maximum number of bytes beyond which the migrate should no go trhough
     * @param writeConcern detailed write concern. NULL means the default write concern.
     * @param waitForDelete whether chunk move should wait for cleanup or return immediately
     * @param maxTimeMS max time for the migrate request
     * @param res the object containing details about the migrate execution
     * @return true if move was successful
     */
    virtual bool moveAndCommit(OperationContext* txn,
                       const ShardId& to,
                       long long chunkSize,
                       const WriteConcernOptions* writeConcern,
                       bool waitForDelete,
                       int maxTimeMS,
                       BSONObj& res) const = 0;

    /**
     * marks this chunk as a jumbo chunk
     * that means the chunk will be inelligble for migrates
     */
    virtual void markAsJumbo(OperationContext* txn) const = 0;

    virtual bool isJumbo() const = 0;


    //
    // accessors and helpers
    //

    virtual std::string toString() const = 0;

    friend std::ostream& operator<<(std::ostream& out, const Chunk& c) {
        return (out << c.toString());
    }

    //
    // public constants
    //

    static long long MaxChunkSize;
    static int MaxObjectPerChunk;
    static bool ShouldAutoSplit;


    // chunk equality is determined by comparing the min and max bounds of the chunk
    virtual bool operator==(const Chunk& s) const = 0;
    virtual bool operator!=(const Chunk& s) const = 0;

    virtual ShardId getShardId() const = 0;

    virtual const ChunkManager* getManager() const = 0;

};


/**
   config.chunks
   { ns : "alleyinsider.fs.chunks" , min : {} , max : {} , server : "localhost:30001" }

   x is in a shard iff
   min <= x < max
 */
class ChunkDefault :public Chunk{
    MONGO_DISALLOW_COPYING(ChunkDefault);

public:

    ChunkDefault(OperationContext* txn, const ChunkManager* info, const ChunkType& from);
    ChunkDefault(const ChunkManager* info,
          const BSONObj& min,
          const BSONObj& max,
          const ShardId& shardId,
          ChunkVersion lastmod = ChunkVersion());

    //
    // chunk boundary support
    //

    const BSONObj& getMin() const final {
        return _min;
    }
    const BSONObj& getMax() const final {
        return _max;
    }

    // Returns true if this chunk contains the given shard key, and false otherwise
    //
    // Note: this function takes an extracted *key*, not an original document
    // (the point may be computed by, say, hashing a given field or projecting
    //  to a subset of fields).
    bool containsKey(const BSONObj& shardKey) const final;

    static std::string genID(const std::string& ns, const BSONObj& min);

    //
    // chunk version support
    //

    void appendShortVersion(const char* name, BSONObjBuilder& b) const final;

    ChunkVersion getLastmod() const final {
        return _lastmod;
    }
    void setLastmod(ChunkVersion v) final {
        _lastmod = v;
    }

    //
    // split support
    //

    long long getBytesWritten() const final {
        return _dataWritten;
    }
    // Const since _dataWritten is mutable and a heuristic
    // TODO: Split data tracking and chunk information
    void setBytesWritten(long long bytesWritten) const final {
        _dataWritten = bytesWritten;
    }

    /**
     * if the amount of data written nears the max size of a shard
     * then we check the real size, and if its too big, we split
     * @return if something was split
     */
    bool splitIfShould(OperationContext* txn, long dataWritten) const final;

    /**
     * Splits this chunk at a non-specificed split key to be chosen by the
     * mongod holding this chunk.
     *
     * @param mode
     * @param res the object containing details about the split execution
     * @param resultingSplits the number of resulting split points. Set to NULL to ignore.
     *
     * @throws UserException
     */
    Status split(OperationContext* txn,
                 SplitPointMode mode,
                 size_t* resultingSplits,
                 BSONObj* res) const final;

    /**
     * Splits this chunk at the given key (or keys)
     *
     * @param splitPoints the vector of keys that should be used to divide this chunk
     * @param res the object containing details about the split execution
     *
     * @throws UserException
     */
    Status multiSplit(OperationContext* txn,
                      const std::vector<BSONObj>& splitPoints,
                      BSONObj* res) const final;

    /**
     * Asks the mongod holding this chunk to find a key that approximately divides this chunk in two
     *
     * @param medianKey the key that divides this chunk, if there is one, or empty
     */
    void pickMedianKey(OperationContext* txn, BSONObj& medianKey) const final;

    /**
     * Ask the mongod holding this chunk to figure out the split points.
     * @param splitPoints vector to be filled in
     * @param chunkSize chunk size to target in bytes
     * @param maxPoints limits the number of split points that are needed, zero is max (optional)
     * @param maxObjs limits the number of objects in each chunk, zero is as max (optional)
     */
    void pickSplitVector(OperationContext* txn,
                         std::vector<BSONObj>& splitPoints,
                         long long chunkSize,
                         int maxPoints = 0,
                         int maxObjs = 0) const final;

    //
    // migration support
    //

    /**
     * Issues a migrate request for this chunk
     *
     * @param to shard to move this chunk to
     * @param chunSize maximum number of bytes beyond which the migrate should no go trhough
     * @param writeConcern detailed write concern. NULL means the default write concern.
     * @param waitForDelete whether chunk move should wait for cleanup or return immediately
     * @param maxTimeMS max time for the migrate request
     * @param res the object containing details about the migrate execution
     * @return true if move was successful
     */
    bool moveAndCommit(OperationContext* txn,
                       const ShardId& to,
                       long long chunkSize,
                       const WriteConcernOptions* writeConcern,
                       bool waitForDelete,
                       int maxTimeMS,
                       BSONObj& res) const final;

    /**
     * marks this chunk as a jumbo chunk
     * that means the chunk will be inelligble for migrates
     */
    void markAsJumbo(OperationContext* txn) const final;

    bool isJumbo() const final{
        return _jumbo;
    }

    /**
     * Attempt to refresh maximum chunk size from config.
     */
    static void refreshChunkSize(OperationContext* txn);

    /**
     * sets MaxChunkSize
     * 1 <= newMaxChunkSize <= 1024
     * @return true if newMaxChunkSize is valid and was set
     */
    static bool setMaxChunkSizeSizeMB(int newMaxChunkSize);

    //
    // accessors and helpers
    //

    std::string toString() const final;

    // chunk equality is determined by comparing the min and max bounds of the chunk
    bool operator==(const Chunk& s) const final;
    bool operator!=(const Chunk& s) const final {
        return !(*this == s);
    }

    ShardId getShardId() const final {
        return _shardId;
    }
    const ChunkManager* getManager() const final {
        return _manager;
    }

private:
    /**
     * Returns the connection string for the shard on which this chunk resides.
     */
    ConnectionString _getShardConnectionString(OperationContext* txn) const;

    // if min/max key is pos/neg infinity
    bool _minIsInf() const;
    bool _maxIsInf() const;

    // The chunk manager, which owns this chunk. Not owned by the chunk.
    const ChunkManager* _manager;

    BSONObj _min;
    BSONObj _max;
    ShardId _shardId;
    ChunkVersion _lastmod;
    mutable bool _jumbo;

    // transient stuff

    mutable long long _dataWritten;

    // methods, etc..

    /**
     * Returns the split point that will result in one of the chunk having exactly one
     * document. Also returns an empty document if the split point cannot be determined.
     *
     * @param doSplitAtLower determines which side of the split will have exactly one document.
     *        True means that the split point chosen will be closer to the lower bound.
     *
     * Warning: this assumes that the shard key is not "special"- that is, the shardKeyPattern
     *          is simply an ordered list of ascending/descending field names. Examples:
     *          {a : 1, b : -1} is not special. {a : "hashed"} is.
     */
    BSONObj _getExtremeKey(OperationContext* txn, bool doSplitAtLower) const;

    /**
     * Determines the appropriate split points for this chunk.
     *
     * @param atMedian perform a single split at the middle of this chunk.
     * @param splitPoints out parameter containing the chosen split points. Can be empty.
     */
    void determineSplitPoints(OperationContext* txn,
                              bool atMedian,
                              std::vector<BSONObj>* splitPoints) const;

    /**
     * initializes _dataWritten with a random value so that a mongos restart
     * wouldn't cause delay in splitting
     */
    static int mkDataWritten();
};

class ChunkCMongo :public Chunk{
    MONGO_DISALLOW_COPYING(ChunkCMongo);

public:
	ChunkCMongo(const ChunkManager* info, const ShardId& shardId, uint32_t stat, uint32_t chunkid);
    //
    // chunk boundary support
    //

    const BSONObj& getMin() const final {
        return _min;
    }
    const BSONObj& getMax() const final {
        return _max;
    }

    // Returns true if this chunk contains the given shard key, and false otherwise
    //
    // Note: this function takes an extracted *key*, not an original document
    // (the point may be computed by, say, hashing a given field or projecting
    //  to a subset of fields).
    bool containsKey(const BSONObj& shardKey) const final;

    //
    // chunk version support
    //

    void appendShortVersion(const char* name, BSONObjBuilder& b) const final;

    ChunkVersion getLastmod() const final {
        return _lastmod;
    }
    void setLastmod(ChunkVersion v) final {
        _lastmod = v;
    }

    //
    // split support
    //

    long long getBytesWritten() const final {
        return _dataWritten;
    }
    // Const since _dataWritten is mutable and a heuristic
    // TODO: Split data tracking and chunk information
    void setBytesWritten(long long bytesWritten) const final {
        _dataWritten = bytesWritten;
    }

    /**
     * if the amount of data written nears the max size of a shard
     * then we check the real size, and if its too big, we split
     * @return if something was split
     */
    bool splitIfShould(OperationContext* txn, long dataWritten) const final;

    /**
     * Splits this chunk at a non-specificed split key to be chosen by the
     * mongod holding this chunk.
     *
     * @param mode
     * @param res the object containing details about the split execution
     * @param resultingSplits the number of resulting split points. Set to NULL to ignore.
     *
     * @throws UserException
     */
    Status split(OperationContext* txn,
                 SplitPointMode mode,
                 size_t* resultingSplits,
                 BSONObj* res) const final;

    /**
     * Splits this chunk at the given key (or keys)
     *
     * @param splitPoints the vector of keys that should be used to divide this chunk
     * @param res the object containing details about the split execution
     *
     * @throws UserException
     */
    Status multiSplit(OperationContext* txn,
                      const std::vector<BSONObj>& splitPoints,
                      BSONObj* res) const final;

    /**
     * Asks the mongod holding this chunk to find a key that approximately divides this chunk in two
     *
     * @param medianKey the key that divides this chunk, if there is one, or empty
     */
    void pickMedianKey(OperationContext* txn, BSONObj& medianKey) const final;

    /**
     * Ask the mongod holding this chunk to figure out the split points.
     * @param splitPoints vector to be filled in
     * @param chunkSize chunk size to target in bytes
     * @param maxPoints limits the number of split points that are needed, zero is max (optional)
     * @param maxObjs limits the number of objects in each chunk, zero is as max (optional)
     */
    void pickSplitVector(OperationContext* txn,
                         std::vector<BSONObj>& splitPoints,
                         long long chunkSize,
                         int maxPoints = 0,
                         int maxObjs = 0) const final;

    //
    // migration support
    //

    /**
     * Issues a migrate request for this chunk
     *
     * @param to shard to move this chunk to
     * @param chunSize maximum number of bytes beyond which the migrate should no go trhough
     * @param writeConcern detailed write concern. NULL means the default write concern.
     * @param waitForDelete whether chunk move should wait for cleanup or return immediately
     * @param maxTimeMS max time for the migrate request
     * @param res the object containing details about the migrate execution
     * @return true if move was successful
     */
    bool moveAndCommit(OperationContext* txn,
                       const ShardId& to,
                       long long chunkSize,
                       const WriteConcernOptions* writeConcern,
                       bool waitForDelete,
                       int maxTimeMS,
                       BSONObj& res) const final;

    /**
     * marks this chunk as a jumbo chunk
     * that means the chunk will be inelligble for migrates
     */
    void markAsJumbo(OperationContext* txn) const final;

    bool isJumbo() const final{
        return _jumbo;
    }

    //
    // accessors and helpers
    //

    std::string toString() const final;

    // chunk equality is determined by comparing the min and max bounds of the chunk
    bool operator==(const Chunk& s) const final;
    bool operator!=(const Chunk& s) const final {
        return !(*this == s);
    }

    ShardId getShardId() const final {
        uassert(13205,
                str::stream() << "Chunk: " << _chunkId << "forbide RW.",
                (_state == 0));
        return _shardId;
    }
    const ChunkManager* getManager() const final {
        return _manager;
    }

private:
    // The chunk manager, which owns this chunk. Not owned by the chunk.
    const ChunkManager* _manager;

    BSONObj _min;
    BSONObj _max;
    ShardId _shardId;
    ChunkVersion _lastmod;
    mutable bool _jumbo;
    mutable long long _dataWritten;
	uint32_t _chunkId;
	uint32_t _state;
};

typedef std::shared_ptr<const Chunk> ChunkPtr;

}  // namespace mongo
