from mock import Mock


def make_client(measurements=None, continuous_queries=None,
                retention_policies=None, show=None):
    client = Mock()

    if show is None:
        show = {}
    if measurements is not None:
        show["MEASUREMENTS"] = measurements
    if continuous_queries is not None:
        show["CONTINUOUS QUERIES"] = continuous_queries
    if retention_policies is not None:
        show["RETENTION POLICIES"] = retention_policies

    def mock_query_result(q):
        if q.startswith("SHOW "):
            key = q[5:]
            points = []
            if key in show:
                points = show[key]
        elif q.startswith("CREATE "):
            client.create_query(q[7:])
            points = Mock()
        elif q.startswith("ALTER "):
            client.alter_query(q[6:])
            points = Mock()
        elif q.startswith("DROP "):
            client.drop_query(q[5:])
            points = Mock()
        else:
            client.other_query(q)
            points = Mock()

        query_result = Mock()
        query_result.get_points.return_value = points
        return query_result

    client.query.side_effect = mock_query_result

    return client
