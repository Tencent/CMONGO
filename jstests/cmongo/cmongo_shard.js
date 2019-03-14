var testdb = db.getSiblingDB("test");
var admindb = db.getSiblingDB("admin");
admindb.runCommand({'unregImmutableKey':1, 'ns': 'test.testshard'});

admindb.cmongo_shard.drop();
testdb.testshard.drop();


result = admindb.runCommand({'regImmutableKey':1, 'ns': 'test.testshard', 'key':{'a':1}});
assert.eq(result.ok, 1);
testdb.testshard.insert({'a':'kdy'});
result = testdb.testshard.update({'a':'kdy'}, {'a':'kdy1'});
assert.eq(result.hasWriteError(), true);
assert.eq(result.getWriteError().code, 66);

assert.eq(admindb.cmongo_shard.count(), 1);
result = admindb.runCommand({'unregImmutableKey':1, 'ns': 'test.testshard'});
assert.eq(result.ok, 1);

result = testdb.testshard.update({'a':'kdy'}, {'a':'kdy1'});
assert.eq(result.nMatched, 1);

admindb.cmongo_shard.drop();

testdb.testshard.drop();
