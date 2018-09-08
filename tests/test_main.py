import copy
import json
import unittest

from mock import patch

from influxdb_aggregation import main
from tests.influx_mock import make_client


class AggregatorTests(unittest.TestCase):
    db_config = {
        "database": "test",
        "host": "test_host",
        "port": 0,
        "default_policy": {
            "rollup": "20m",
            "retention": "2h0m0s",
            "replication": 1,
            "shard_duration": "4h"
        },
        "desired_policies": [
            {
                "rollup": "20m",
                "retention": "24h0m0s",
                "replication": 1,
                "shard_duration": "4h"
            }
        ]
    }
    measurements = [{"name": "test_measurement"}]
    expected_policy_result = [
        {
            "name": "input",
            "duration": "2h0m0s",
            "shardGroupDuration": "1h0m0s",
            "replicaN": 1,
            "default": True
        },
        {
            "name": "rollup_20m",
            "duration": "24h0m0s",
            "shardGroupDuration": "4h",
            "replicaN": 1,
            "default": False
        }
    ]
    expected_policies = {
        "input": {
            "name": "input",
            "duration": "2h0m0s",
            "shardGroupDuration": "1h0m0s",
            "replicaN": 1,
            "default": True
        },
        "rollup_20m": {
            "name": "rollup_20m",
            "duration": "24h0m0s",
            "shardGroupDuration": "4h",
            "replicaN": 1,
            "default": False
        }
    }
    expected_query_result = [
        {
            "name": "test_measurement_rollup_20m",
            "query": "CREATE CONTINUOUS QUERY test_measurement_rollup_20m "
                     "ON test BEGIN SELECT mean(value) AS value, "
                     "max(value) AS max_value, min(value) AS min_value "
                     "INTO test.rollup_20m.test_measurement "
                     "FROM test.input.test_measurement "
                     "GROUP BY *, time(20m) END",
        },
    ]
    expected_queries = {
        "test_measurement_rollup_20m":
            "CREATE CONTINUOUS QUERY test_measurement_rollup_20m "
            "ON test BEGIN SELECT mean(value) AS value, "
            "max(value) AS max_value, min(value) AS min_value "
            "INTO test.rollup_20m.test_measurement "
            "FROM test.input.test_measurement "
            "GROUP BY *, time(20m) END",
    }
    expected_policy_info = {
        'rollup_20m': {
            'create': 'CREATE RETENTION POLICY rollup_20m ON test '
                      'DURATION 24h0m0s REPLICATION 1 SHARD DURATION 4h',
            'update': 'ALTER RETENTION POLICY rollup_20m ON test DURATION '
                      '24h0m0s REPLICATION 1 SHARD DURATION 4h',
            'rollup': '20m', 'retention': '24h0m0s', 'replication': 1,
            'shard_duration': '4h'}, 'input': {
            'create': 'CREATE RETENTION POLICY rollup_20m ON test '
                      'DURATION 2h0m0s REPLICATION 1 SHARD DURATION 4h '
                      'DEFAULT',
            'update': 'ALTER RETENTION POLICY rollup_20m ON test '
                      'DURATION 2h0m0s REPLICATION 1 SHARD DURATION 4h '
                      'DEFAULT',
            'rollup': '20m', 'retention': '2h0m0s', 'replication': 1,
            'shard_duration': '4h'}
    }
    expected_query_info = {
        "test_measurement_rollup_20m": {
            "measurement": "test_measurement",
            "query": "CREATE CONTINUOUS QUERY test_measurement_rollup_20m "
                     "ON test BEGIN SELECT mean(value) AS value, "
                     "max(value) AS max_value, min(value) AS min_value "
                     "INTO test.rollup_20m.test_measurement "
                     "FROM test.input.test_measurement "
                     "GROUP BY *, time(20m) END",
            "replication": 1,
            "retention": "24h0m0s",
            "rollup": "20m",
            "shard_duration": "4h"
        }
    }

    def assertJsonEqual(self, first, second):
        self.assertEqual(
            json.dumps(first, sort_keys=True, indent=2),
            json.dumps(second, sort_keys=True, indent=2)
        )

    def setUp(self):
        self.client_patcher = patch('influxdb_aggregation.main.InfluxDBClient')
        self.client = make_client(
            measurements=copy.deepcopy(self.measurements),
            retention_policies=copy.deepcopy(self.expected_policy_result),
            continuous_queries=copy.deepcopy(self.expected_query_result)
        )
        self.patched_client = self.client_patcher.start()
        self.patched_client.return_value = self.client

    def tearDown(self):
        self.client_patcher.stop()

    def test_data(self):
        data = main.get_database_state(self.client, self.db_config)

        existing_policies, existing_queries, policy_info, query_info = data
        self.assertJsonEqual(existing_policies, self.expected_policies)
        self.assertJsonEqual(existing_queries, self.expected_queries)
        self.assertJsonEqual(policy_info, self.expected_policy_info)
        self.assertJsonEqual(query_info, self.expected_query_info)

    def test_database_handler(self):
        main.process_database(copy.deepcopy(self.db_config))
        self.client.other_query.assert_not_called()
        self.client.create_query.assert_not_called()
        self.client.drop_query.assert_not_called()
        self.client.alter_query.assert_not_called()

    def test_database_handler_missing_policy(self):
        config = copy.deepcopy(self.db_config)
        config["desired_policies"].append({
            "rollup": "2m",
            "retention": "24h0m0s",
            "replication": 1,
            "shard_duration": "4h"
        })
        main.process_database(config)
        self.client.create_query.assert_any_call(
            "RETENTION POLICY rollup_2m ON test "
            "DURATION 24h0m0s REPLICATION 1 SHARD DURATION 4h"
        )
        self.client.create_query.assert_any_call(
            "CONTINUOUS QUERY test_measurement_rollup_2m ON test BEGIN "
            "SELECT mean(value) AS value, max(value) AS max_value, "
            "min(value) AS min_value INTO test.rollup_2m.test_measurement "
            "FROM test.input.test_measurement GROUP BY *, time(2m) END"
        )
        self.client.other_query.assert_not_called()
        self.client.alter_query.assert_not_called()
        self.client.drop_query.assert_not_called()

    def test_database_handler_update_policy(self):
        config = copy.deepcopy(self.db_config)
        config["desired_policies"][0]["retention"] = "25h0m0s"

        main.process_database(config)

        self.client.alter_query.assert_any_call(
            "RETENTION POLICY rollup_20m ON test DURATION 25h0m0s "
            "REPLICATION 1 SHARD DURATION 4h"
        )
        self.client.other_query.assert_not_called()
        self.client.create_query.assert_not_called()
        self.client.drop_query.assert_not_called()

    def test_database_handler_delete_policy(self):
        config = copy.deepcopy(self.db_config)
        config["desired_policies"].pop()

        main.process_database(config)

        self.client.drop_query.assert_any_call(
            "RETENTION POLICY \"rollup_20m\" ON \"test\""
        )
        self.client.other_query.assert_not_called()
        self.client.create_query.assert_not_called()
        self.client.alter_query.assert_not_called()

    def test_database_handler_alter_query(self):
        continuous_queries = copy.deepcopy(self.expected_query_result)
        continuous_queries[0]["query"] = "CREATE CONTINUOUS QUERY Needs update"

        self.client = make_client(
            measurements=copy.deepcopy(self.measurements),
            retention_policies=copy.deepcopy(self.expected_policy_result),
            continuous_queries=continuous_queries
        )
        self.patched_client.return_value = self.client

        main.process_database(copy.deepcopy(self.db_config))

        self.client.drop_query.assert_any_call(
            "CONTINUOUS QUERY test_measurement_rollup_20m ON test"
        )
        self.client.create_query.assert_any_call(
            "CONTINUOUS QUERY test_measurement_rollup_20m ON test "
            "BEGIN SELECT mean(value) AS value, max(value) AS max_value, "
            "min(value) AS min_value INTO test.rollup_20m.test_measurement "
            "FROM test.input.test_measurement GROUP BY *, time(20m) END"
        )
        self.client.other_query.assert_not_called()
        self.client.alter_query.assert_not_called()
