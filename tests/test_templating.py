import unittest

from influxdb_aggregation import templating


class TemplatingTests(unittest.TestCase):
    policy = {
        "rollup": "20m",
        "retention": "24h0m0s",
        "replication": 1,
        "shard_duration": "4h"
    }
    database = "test_database"
    measurement = "test_measurement"

    def test_policy_name(self):
        policy = {"rollup": "20m"}
        self.assertEqual(templating.policy_name(policy), "rollup_20m")

        policy = {"rollup": "2m"}
        self.assertEqual(templating.policy_name(policy), "rollup_2m")

        policy = {}
        self.assertEqual(templating.policy_name(policy), "input")

    def test_strip_query(self):
        q = """
        SELECT * FROM  Query
        NOT IMPORTANT   Only
        whitespace;
        """

        expected = "SELECT * FROM Query NOT IMPORTANT Only whitespace;"

        self.assertEqual(templating.strip_query(q), expected)

    def test_policy_query(self):
        expected = "CREATE RETENTION POLICY rollup_20m ON test_database " \
                   "DURATION 24h0m0s REPLICATION 1 SHARD DURATION 4h"

        self.assertEqual(
            templating.policy_query(self.policy, self.database),
            expected
        )

    def test_policy_update_query(self):
        expected = "ALTER RETENTION POLICY rollup_20m ON test_database " \
                   "DURATION 24h0m0s REPLICATION 1 SHARD DURATION 4h"

        self.assertEqual(
            templating.policy_update_query(self.policy, self.database),
            expected
        )

    def test_continuous_query_name(self):
        expected = "test_measurement_rollup_20m"

        self.assertEqual(
            templating.continuous_query_name(self.policy, self.measurement),
            expected
        )

    def test_continuous_query_query(self):
        expected = "SELECT mean(value) AS value, max(value) AS max_value, " \
                   "min(value) AS min_value " \
                   "INTO test_database.rollup_20m.test_measurement " \
                   "FROM test_database.input.test_measurement " \
                   "GROUP BY *, time(20m)"
        self.assertEqual(
            templating.continuous_query_query(
                self.policy, self.measurement, self.database
            ),
            expected
        )

    def test_continuous_query_create(self):
        expected = "CREATE CONTINUOUS QUERY test_measurement_rollup_20m " \
                   "ON test_database BEGIN SELECT mean(value) AS value, " \
                   "max(value) AS max_value, min(value) AS min_value " \
                   "INTO test_database.rollup_20m.test_measurement " \
                   "FROM test_database.input.test_measurement " \
                   "GROUP BY *, time(20m) END"

        self.assertEqual(
            templating.continuous_query_create(
                self.policy, self.measurement, self.database
            ),
            expected
        )
