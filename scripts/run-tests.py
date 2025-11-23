import subprocess
import sys
import os

def run_tests(db_path, test_data_csv, test_queries_sql, duckdb_exe):
    """Run automated tests for NiFi/DuckDB workflow"""
    
    print("🧪 Running automated tests...")
    
    try:
        # Create test database with schema
        print("📦 Setting up test database...")
        schema_result = subprocess.run(
            [duckdb_exe, db_path, "-c", ".read sql/schema.sql"],
            capture_output=True,
            text=True,
            shell=True
        )
        
        if schema_result.returncode != 0:
            print(f"❌ Schema creation failed: {schema_result.stderr}")
            sys.exit(1)
        
        # Load test data
        # Load test data
       # Load test data
        print("📥 Loading test data...")
        load_cmd = f"COPY customers (customer_id, first_name, last_name,         email, signup_date, country, total_purchases) FROM '{test_data_csv}'         (HEADER, DELIMITER ',');"
        load_result = subprocess.run(
            [duckdb_exe, db_path, "-c", load_cmd],
            capture_output=True,
            text=True,
            shell=True
        )
        
        if load_result.returncode != 0:
            print(f"❌ Data loading failed: {load_result.stderr}")
            sys.exit(1)
        
        # Run validation queries
        print("🔍 Running data quality validations...")
        validation_result = subprocess.run(
            [duckdb_exe, db_path, "-c", ".read sql/validation-queries.sql"],
            capture_output=True,
            text=True,
            shell=True
        )
        
        print(validation_result.stdout)
        
        # Run test queries
        print("✅ Running test suite...")
        test_result = subprocess.run(
            [duckdb_exe, db_path, "-c", ".read " + test_queries_sql],
            capture_output=True,
            text=True,
            shell=True
        )
        
        print(test_result.stdout)
        
        # Check for failures
        if "FAIL" in test_result.stdout:
            print("❌ Some tests failed!")
            sys.exit(1)
        
        print("✅ All tests passed!")
        return True
        
    except Exception as e:
        print(f"❌ Test execution error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python run-tests.py <db-path> <test-data-csv> <test-queries-sql>")
        sys.exit(1)
    
    db_path = sys.argv[1]
    test_data_csv = sys.argv[2]
    test_queries_sql = sys.argv[3]
    
    # Path to DuckDB executable
    duckdb_exe = r"C:\Users\anup5\tools\duckdb.exe"
    
    if not os.path.exists(duckdb_exe):
        print(f"❌ DuckDB not found at: {duckdb_exe}")
        sys.exit(1)
    
    run_tests(db_path, test_data_csv, test_queries_sql, duckdb_exe)