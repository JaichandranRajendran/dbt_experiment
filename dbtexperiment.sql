Here’s the full consolidated Python script that includes:
	•	Reuse of DBT’s profiles.yml Trino connection
	•	Analysis of compiled DBT SQL models
	•	Advanced optimization checks:
	•	❗Join count
	•	❗CTE depth
	•	❗Subquery detection
	•	❗Broadcast joins
	•	❗SELECT * usage
	•	❗Full table scans
	•	❗Missing statistics
	•	❗Partition pruning
	•	❗Dynamic filtering
	•	❗Small file count via Iceberg $files metadata


import os
import re
import yaml
import glob
from typing import List
from trino.dbapi import connect

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
        auth=None  # If needed: use BasicAuthentication(profile['user'], profile['password'])
    )
    return conn

def get_compiled_sql_files(base_path='target/compiled'):
    return glob.glob(os.path.join(base_path, '**', '*.sql'), recursive=True)

def advanced_iceberg_checks(sql: str, plan: str, cursor, model_name: str) -> List[str]:
    issues = []

    # Check Iceberg small file count
    match = re.search(r'FROM\s+([a-zA-Z0-9_."]+)', sql, re.IGNORECASE)
    if match:
        table_ref = match.group(1).replace('"', '')
        try:
            cursor.execute(f'SELECT COUNT(*) AS file_count, SUM(file_size_in_bytes) FROM "{table_ref}$files"')
            file_count, total_size = cursor.fetchone()
            if file_count and file_count > 500:
                issues.append(f"⚠️ {model_name}: High number of files ({file_count}) in {table_ref}. Consider running OPTIMIZE.")
        except Exception as e:
            issues.append(f"⚠️ {model_name}: Could not query metadata from {table_ref}$files – {str(e)}")

    # Iceberg-specific and Trino predicate/stat checks
    if "DynamicFilter" not in plan:
        issues.append(f"⚠️ {model_name}: Dynamic filtering not detected. Review join filters.")
    if "No table statistics" in plan or "missing statistics" in plan.lower():
        issues.append(f"❌ {model_name}: Missing statistics. Run ANALYZE for better performance.")
    if "COUNT" in sql.upper() and "metadata" not in plan and "aggregation" in sql.lower():
        issues.append(f"⚠️ {model_name}: COUNT(*) may scan full table. Enable metadata aggregation.")
    if "partitionPredicate" not in plan:
        issues.append(f"⚠️ {model_name}: Partition pruning may not be applied.")
    if "SELECT *" in sql.upper():
        issues.append(f"❌ {model_name}: Avoid SELECT *. Use only required columns.")
    
    return issues

def structural_checks(sql: str, plan: str, model_name: str) -> List[str]:
    issues = []

    join_count = len(re.findall(r'(InnerJoin|LeftJoin|Join)', plan))
    if join_count > 5:
        issues.append(f"⚠️ {model_name}: High join count ({join_count}). Consider refactoring.")

    cte_count = len(re.findall(r'\bWITH\b', sql, re.IGNORECASE))
    if cte_count > 2:
        issues.append(f"⚠️ {model_name}: Nested CTEs detected. Break into smaller models.")

    subquery_count = len(re.findall(r'\(\s*SELECT\s', sql, re.IGNORECASE))
    if subquery_count > 2:
        issues.append(f"⚠️ {model_name}: Multiple subqueries detected. Consider flattening.")

    if "CrossJoin" in plan or re.search(r'\bJOIN\b\s+\w+\s*(?!ON)', sql, re.IGNORECASE):
        issues.append(f"❌ {model_name}: Potential Cartesian join. Verify ON conditions.")
    
    return issues

def check_explain_plan_all(model_name: str, sql: str, cursor) -> List[str]:
    explain_query = f"EXPLAIN {sql}"
    try:
        cursor.execute(explain_query)
        plan = cursor.fetchall()[0][0]
    except Exception as e:
        return [f"❌ {model_name}: EXPLAIN failed – {str(e)}"]

    issues = []

    if "TableScan" in plan and "Filter" not in plan:
        issues.append(f"❌ {model_name}: Full table scan. Add WHERE filters.")
    if "BroadcastExchange" in plan:
        issues.append(f"❌ {model_name}: Broadcast join detected. Use partitioned joins.")

    issues += advanced_iceberg_checks(sql, plan, cursor, model_name)
    issues += structural_checks(sql, plan, model_name)
    return issues

def run_consolidated_check(profile_name='my_project', target_name='dev'):
    conn = load_dbt_trino_profile(profile_name, target_name)
    cursor = conn.cursor()
    sql_files = get_compiled_sql_files()

    report = {}
    for sql_file in sql_files:
        with open(sql_file, 'r') as f:
            sql = f.read()
        model_name = os.path.relpath(sql_file, start='target/compiled')
        issues = check_explain_plan_all(model_name, sql, cursor)
        report[model_name] = issues

    return report

if __name__ == "__main__":
    report = run_consolidated_check()
    for model, issues in report.items():
        print(f"\n🔍 Model: {model}")
        if not issues:
            print("✅ No major issues found.")
        else:
            for issue in issues:
                print(issue)