# InfluxDB prometheus-backend aggregator.

This is a little script I wrote to be able to generate continuous queries
in InfluxDB for aggregating series when used as a prometheus backend.


## Configuration

The configuration is in YAML, and currently only editing the default policy is supported.
Loading from YAML files is planned into the design, but not implemented due to laziness.

### default_policy
Doubles as the policy for the default-series and as default values for other series.
Any values left out of any policy will ultimately revert to this policy.

Contents:
[<policy>](#<policy>)

Example:
~~~~
default_policy:
  retention: 48h0m0s
  replication: 1
  shard_duration: 1h
~~~~

### database
Name of the default database in InfluxDB to apply rules to if no value is provided.

Contents:
String

Example:
~~~~
database: prometheus
~~~~

### host
Default host to connect to if no value is provided

Contents:
String

Example:
~~~~
host: 127.0.0.1
~~~~

### port
Default port if no value is provided

Contents:
Integer

Example:
~~~~
port: 8086
~~~~

### Desired policies
A list of retention-policies to create and maintain.

Contents:
List of [<policy>](#<policy>)

Example:
~~~~
desired_policies:
  - rollup: 5m
    # 50 weeks in the format influx converts it to
    retention: 8400h0m0s
    shard_duration: 1d
  - rollup: 1h
    # 200 weeks in the format influx converts it to
    retention: 33600h0m0s
    shard_duration: 1w
  - rollup: 1d
    # forever
    retention: 0s
    shard_duration: 2w
~~~~

### configs
Database configurations, list of databases to maintain

Contents:
List of [<config>](#<config>)

~~~~
configs:
  - database: prometheus
    host: 127.0.0.1
    port: 8086
    default_policy:
      shard_duration: 1h
~~~~

### Templates
continuous_query_template, create_continuous_query_template, policy_template, policy_update_template,
policy_name_template, and query_name_template are templates for building queries.
Deviate form default at your own risk.


### <policy>
Describes a retention policy.

Contents:

rollup: String; Time to roll up, doubles as interval for query and data aggregation.

retention: String; Retention time for this retention policy, the queries will get re-created if it does
not match the format influxDB converts it to exactly.

replication: Integer; Replication factor.

shard_duration: String; Time duration of a shard.

Example:
~~~~
rollup: 5m
retention: 48h0m0s
replication: 1
shard_duration: 1h
~~~~

### <config>
Describes a database connection

Contents:

database: String; database name on influx server

host: String; Hostname of influx server

port: Integer; Port of influx server

desired_policies: List of [<policy>](#<policy>); override the global desired policies entirely.

Example:
~~~~
database: prometheus
host: 127.0.0.1
port: 8086
default_policy:
  # Default variables can be overridden on database level
  shard_duration: 1h
  # Overrides the default completely
  desired_policies: 
   - rollup: 5m
    # 50 weeks in the format influx converts it to
    retention: 8400h0m0s
    shard_duration: 1d
~~~~