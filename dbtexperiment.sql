import yaml
import os
from trino.dbapi import connect

# Load dbt profile
def load_dbt_trino_profile(profile_name='my_project', target_name='dev'):
    profile_path = os.path.expanduser("~/.dbt/profiles.yml")
    with open(profile_path, 'r') as f:
        profiles = yaml.safe_load(f)
    profile = profiles[profile_name]['outputs'][target_name]

    return connect(
        host=profile['host'],
        port=profile['port'],
        user=profile['user'],
        catalog=profile['catalog'],
        schema=profile['schema'],
        http_scheme=profile.get('http_scheme', 'http'),
        auth=None,  # Optional: Add auth logic if needed
    )

# Use the connection
conn = load_dbt_trino_profile()
cursor = conn.cursor()
cursor.execute("EXPLAIN SELECT * FROM analytics.my_table")
plan = cursor.fetchall()[0][0]
print(plan)

