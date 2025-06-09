import os
import yaml
from trino.dbapi import connect
import glob

# Load Trino profile from dbt's profiles.yml
def load_dbt_trino_profile(profile_name='my_project', target_name='dev'):
    profile_path = os.path.expanduser("~/.dbt/profiles.yml")
    with open(profile_path, 'r') as f:
        profiles = yaml.safe_load(f)
    profile = profiles[profile_name]['outputs'][target_name]

    conn = connect(
        host=profile['host'],
        port=profile['port'],
        user=profile['user'],
        catalog=profile['catalog'],
        schema=profile['schema'],
        http_scheme=profile.get('http_scheme', 'http'),
        auth=None  # add BasicAuthentication(profile['user'], profile['password']) if needed
    )
    return conn

# Extract SQL files from target/compiled directory
def get_compiled_sql_files(base_path='target/compiled'):
    return glob.glob(os.path.join(base_path, '**', '*.sql'), recursive=True)

# Run EXPLAIN on the SQL and apply checks
def check_explain_plan(model_name, sql, cursor):
    explain_query = f"EXPLAIN {sql}"
    try:
        cursor.execute(explain_query)
        plan = cursor.fetchall()[0][0]
    except Exception as e:
        return [f"❌ Error analyzing model: {e}"]

    issues = []

    if "TableScan" in plan and "Filter" not in plan:
        issues.append("❌ Full table scan detected. Add WHERE clause on partition column.")
    if "BroadcastExchange" in plan:
        issues.append("❌ Broadcast join used. Reorder join or use JOIN HINT.")
    if "SELECT *" in sql.upper():
        issues.append("❌ SELECT * used. Select only necessary columns.")
    if "partitionPredicate" not in plan:
        issues.append("⚠️ Partition pruning may be missing.")
    if "missing statistics" in plan.lower() or "No table statistics" in plan:
        issues.append("❌ Table statistics missing. Run ANALYZE on the table.")

    return issues

# Main routine
def run_optimization_check(profile_name='my_project', target_name='dev'):
    conn = load_dbt_trino_profile(profile_name, target_name)
    cursor = conn.cursor()
    sql_files = get_compiled_sql_files()

    report = {}

    for sql_file in sql_files:
        with open(sql_file, 'r') as f:
            sql = f.read()
        issues = check_explain_plan(sql_file, sql, cursor)
        report[sql_file] = issues

    return report

# Run and display
optimization_report = run_optimization_check()
import pandas as pd
df_report = pd.DataFrame([
    {"Model": model, "Issue": issue}
    for model, issues in optimization_report.items()
    for issue in issues
])
import ace_tools as tools; tools.display_dataframe_to_user(name="DBT Model Optimization Report", dataframe=df_report)
