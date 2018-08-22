-- Empty Cassandra CQL file to load data for unit tests
-- Required so that the Cassandra Server for tests is able to start
-- (the load phase of the plugin tries to load this file after server start)
-- At this point, we do not need to inject special data at startup: all is handled in the unit test code
describe keyspaces; -- Need at list one effective CQL line
