import re

from influxdb_aggregation.conf import config

__all__ = ['policy_name', 'policy_query', 'policy_update_query',
           'continuous_query_name', 'continuous_query_create']


def policy_name(policy):
    """
    Renders the name of a policy

    :param policy: Policy config dictionary
    :return: Policy name
    """
    return (
        config['policy_name_template'].format(rollup=policy["rollup"])
        if "rollup" in policy
        else "input"
    )


def strip_query(query):
    """
    Strips a query of whitespaces, this form is more useful for comparision.

    :param query: Initial query
    :return: Stripped query
    """
    return re.sub(r'[\s]+', ' ', query).strip()


def policy_query(policy, database):
    """
    Renders a stripped policy creation query

    :param policy: Policy config dictionary
    :param database: Name of the database
    :return: Stripped policy query
    """
    return strip_query(
        config['policy_template'].format(
            policy=policy_name(policy),
            database=database,
            **policy
        )
    )


def policy_update_query(policy, database):
    """
    Renders a policy update query, issued to get the retention policy into
    the desired state.

    :param policy: Policy config dictionary
    :param database: Name of the database
    :return: Stripped policy update query
    """
    return strip_query(
        config['policy_update_template'].format(
            policy=policy_name(policy),
            database=database,
            **policy
        )
    )


def continuous_query_name(policy, measurement):
    """
    Renders the name of a continuous query

    :param policy: Policy config dictionary
    :param measurement: Name of measurement to query
    :return:
    """
    return config['query_name_template'].format(
        policy=policy_name(policy),
        measurement=measurement
    )


def continuous_query_query(policy, measurement, database):
    """
    Renders a continuous query.

    :param policy: Policy config dictionary
    :param measurement: Name of measurement to query
    :param database: Name of the database
    :return: Stripped continuous query
    """
    return strip_query(
        config['continuous_query_template'].format(
            measurement=measurement,
            policy=policy_name(policy),
            database=database,
            **policy
        )
    )


def continuous_query_create(policy, measurement, database):
    """
    Renders a create query for a continous query

    :param policy: Policy config dictionary
    :param measurement: Name of measurement to query
    :param database: Name of the database
    :return: Stripped continuous query creation query
    """
    return strip_query(
        config['create_continuous_query_template'].format(
            measurement=measurement,
            policy=policy_name(policy),
            database=database,
            query=continuous_query_query(policy, measurement, database),
            **policy
        )
    )