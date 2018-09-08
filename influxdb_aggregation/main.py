#!/usr/bin/env python
import logging

from influxdb import InfluxDBClient

from influxdb_aggregation import templating as tpl
from influxdb_aggregation.conf import config

logger = logging.getLogger(__name__)


def get_database_state(client, db_config):
    """
    Queries the database to get the current state of affairs,
    and renders "desired policies"

    :param client: Influx client (connection)
    :param db_config: configuration dictionary for this database
    :return: existing_policies, existing_queries, policy_info, query_info
    """
    measurements = [
        m['name'] for m
        in client.query('SHOW MEASUREMENTS').get_points()
    ]

    queries = list(client.query('SHOW CONTINUOUS QUERIES').get_points())

    policies = list(client.query('SHOW RETENTION POLICIES').get_points())

    policy_info = {
        tpl.policy_name(policy): dict(
            create=tpl.policy_query(policy, db_config['database']),
            update=tpl.policy_update_query(policy, db_config['database']),
            **policy
        )
        for policy in db_config['desired_policies']
    }

    policy_info["input"] = dict(
        create=tpl.policy_query(
            db_config['default_policy'],
            db_config['database']
        ) + " DEFAULT",
        update=tpl.policy_update_query(
            db_config['default_policy'],
            db_config['database']
        ) + " DEFAULT",
        **db_config['default_policy']
    )

    query_info = {
        tpl.continuous_query_name(policy, measurement): dict(
            query=tpl.continuous_query_create(
                policy,
                measurement,
                db_config['database']
            ),
            measurement=measurement,
            **policy
        )
        for policy in db_config['desired_policies']
        for measurement in measurements
    }

    existing_policies = {p['name']: p for p in policies}

    existing_queries = {q["name"]: q["query"] for q in queries}

    return existing_policies, existing_queries, policy_info, query_info


def process_database(db_config):
    """
    Handles the policy+query management for one database.

    :param db_config:
    :return:
    """
    client = InfluxDBClient(
        host=db_config['host'],
        database=db_config['database'],
        port=db_config['port']
    )

    state = get_database_state(client, db_config)
    existing_policies, existing_queries, policy_info, query_info = state

    for policy in policy_info:
        if policy not in existing_policies:
            logger.info("Creating {}".format(policy))
            client.query(policy_info[policy]["create"])
        else:
            current = existing_policies[policy]
            desired = policy_info[policy]
            if current["duration"] != desired["retention"]:
                logger.info("Updating policy {}".format(policy))
                client.query(desired["update"])

    for policy in existing_policies:
        if policy not in policy_info:
            logger.info("Deleting policy {}".format(policy))
            query = "DROP RETENTION POLICY \"{}\" ON \"{}\"".format(
                policy, db_config['database']
            )
            client.query(query)

    for query in query_info:
        if query not in existing_queries:
            logger.info("Creating query {}".format(query))
            client.query(query_info[query]["query"])

    for query in existing_queries:
        if query in query_info:
            current = existing_queries[query]
            desired = query_info[query]
            if current != desired["query"]:
                logger.info("Re-Creating query {}".format(query))
                client.query(
                    "DROP CONTINUOUS QUERY {} ON {}"
                    "".format(query, db_config['database']))
                client.query(desired["query"])


if __name__ == '__main__':
    for db_config in config['configs']:
        process_database(db_config)
