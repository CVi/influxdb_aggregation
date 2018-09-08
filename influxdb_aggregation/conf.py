import yaml

__main__ = ['config']

default_policy = """
default_policy:
  # 2 days
  retention: 48h0m0s
  replication: 1
  shard_duration: 1h
database: prometheus
host: 127.0.0.1
port: 8086

desired_policies:
  - rollup: 5m
    # 50 weeks
    retention: 8400h0m0s
    shard_duration: 1d
  - rollup: 1h
    # 200 weeks
    retention: 33600h0m0s
    shard_duration: 1w
  - rollup: 1d
    # forever
    retention: 0s
    shard_duration: 2w

continuous_query_template: |
  SELECT 
    mean(value) AS value, 
    max(value) AS max_value, 
    min(value) AS min_value
  INTO {database}.{policy}.{measurement}
  FROM {database}.input.{measurement}
  GROUP BY *, time({rollup})

create_continuous_query_template: |
  CREATE CONTINUOUS QUERY {measurement}_{policy} ON {database}
  BEGIN 
    {query}
  END

policy_template: |
  CREATE RETENTION POLICY {policy} ON {database} 
  DURATION {retention}
  REPLICATION {replication}
  SHARD DURATION {shard_duration}
  
policy_update_template: |
  ALTER RETENTION POLICY  {policy} ON {database} 
  DURATION {retention}
  REPLICATION {replication}
  SHARD DURATION {shard_duration}
  
policy_name_template: "rollup_{rollup}"
query_name_template: "{measurement}_{policy}"
  
configs:
  - database: prometheus
    host: 127.0.0.1
    port: 8086
    default_policy:
      # Default variables can be overridden on database level
      shard_duration: 1h
    # Defaults to global if not set.
    # desired_policies: 
"""


def update_config(configuration, server_base, config_data):
    """
    Updates the configuration with new values.
    It only does a update, so there is no recursive support, but it should
    be enough for most use cases.

    :param configuration: Destination configuration dictionary
    :param server_base: Server intermediate configuration dictionary
    :param config_data: Source data
    """
    for key in ['continuous_query_template', 'create_continuous_query_template',
                'policy_update_template', 'policy_name_template',
                'query_name_template', 'policy_template']:
        if key in config_data:
            configuration[key] = config_data[key]

    for key in ['default_policy', 'database', 'host', 'port',
                'desired_policies', 'configs']:
        if key in config_data:
            server_base[key] = config_data[key]


def make_db_configs(configuration, server_base):
    """
    Processes a server intermediate configuration dictionary into final
    configuration dictionary.
    This populates defaults so any subsequent usage of the config do not have
    to worry about them.

    :param configuration: Destination configuration dictionary
    :param server_base: Server intermediate configuration dictionary
    """
    standard_config = {
        'default_policy': server_base['default_policy'],
        'database': server_base['database'],
        'host': server_base['host'],
        'port': server_base['port'],
        'desired_policies': server_base['desired_policies']
    }

    databases = []
    configuration['configs'] = databases

    for conf in server_base['configs']:
        db_config = dict(standard_config)
        databases.append(db_config)
        for var in ['database', 'host', 'port', 'desired_policies']:
            if var in conf:
                db_config[var] = conf[var]
        if 'default_policy' in conf:
            db_config['default_policy'].update(conf['default_policy'])

        defaulted_policies = []
        for policy in db_config['desired_policies']:
            new_policy = dict(db_config['default_policy'])
            new_policy.update(policy)
            defaulted_policies.append(new_policy)
        db_config['desired_policies'] = defaulted_policies


config = {}
server_conf_base = {}

default_base = yaml.safe_load(default_policy)

# Possible to add a file load with update here.
update_config(config, server_conf_base, default_base)

make_db_configs(config, server_conf_base)
