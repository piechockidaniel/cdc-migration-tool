"""Setup verification script for MSSQL CDC Database Synchronizer.

This script verifies that all prerequisites and dependencies are properly installed.
Run this before starting the application for the first time.
"""

import sys
from pathlib import Path


def print_header(text):
    """Print a formatted header."""
    print("\n" + "=" * 70)
    print(f"  {text}")
    print("=" * 70)


def print_check(name, status, details=""):
    """Print a check result."""
    symbol = "✓" if status else "✗"
    status_text = "OK" if status else "FAILED"
    color = "\033[92m" if status else "\033[91m"
    reset = "\033[0m"
    
    print(f"{color}{symbol}{reset} {name}: {status_text}")
    if details:
        print(f"  {details}")


def check_python_version():
    """Check Python version."""
    print_header("Checking Python Version")
    
    version = sys.version_info
    required = (3, 10)  # README says 3.9+; we use `X | Y` typing and dataclass features -> 3.10 minimum
    
    is_ok = version >= required
    details = f"Found Python {version.major}.{version.minor}.{version.micro}"
    
    if not is_ok:
        details += f" (Required: {required[0]}.{required[1]}+)"
    
    print_check("Python Version", is_ok, details)
    return is_ok


def check_dependencies():
    """Check if required Python packages are installed."""
    print_header("Checking Python Dependencies")
    
    packages = {
        'fastapi': 'FastAPI',
        'uvicorn': 'Uvicorn',
        'pyodbc': 'pyodbc (SQL Server)',
        'duckdb': 'DuckDB',
        'pydantic': 'Pydantic',
        'jinja2': 'Jinja2',
    }
    optional = {
        'psycopg': 'psycopg (PostgreSQL connector)',
        'pymysql': 'PyMySQL (MariaDB/MySQL metadata)',
        'pymysqlreplication': 'mysql-replication (MariaDB/MySQL binlog stream)',
        'pymongo': 'pymongo (MongoDB connector)',
        'oracledb': 'python-oracledb (Oracle connector)',
        'confluent_kafka': 'confluent-kafka (Kafka mirror, optional)',
    }
    for package, name in optional.items():
        try:
            __import__(package)
            print_check(name, True, "installed (optional)")
        except ImportError:
            print_check(name, True, "not installed - only needed for that engine")
    
    all_ok = True
    
    for package, name in packages.items():
        try:
            __import__(package)
            print_check(name, True, f"Module '{package}' found")
        except ImportError:
            print_check(name, False, f"Module '{package}' not found")
            all_ok = False
    
    return all_ok


def check_odbc_driver():
    """Check if ODBC driver is available."""
    print_header("Checking ODBC Driver")
    
    try:
        import pyodbc
        drivers = pyodbc.drivers()
        
        # Look for SQL Server ODBC drivers
        sql_drivers = [d for d in drivers if 'SQL Server' in d]
        
        if sql_drivers:
            print_check("ODBC Driver", True, f"Found: {sql_drivers[0]}")
            return True
        else:
            print_check("ODBC Driver", False, "No SQL Server ODBC driver found")
            print("  Install from: https://docs.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server")
            return False
            
    except ImportError:
        print_check("ODBC Driver", False, "pyodbc not installed")
        return False


def check_directories():
    """Check if required directories exist."""
    print_header("Checking Project Structure")
    
    required_dirs = [
        'backend',
        'backend/api',
        'backend/core',
        'backend/db',
        'backend/models',
        'backend/utils',
        'frontend',
        'frontend/static',
        'frontend/templates',
        'config',
        'history',
        'examples'
    ]
    
    all_ok = True
    
    for dir_path in required_dirs:
        path = Path(dir_path)
        exists = path.exists() and path.is_dir()
        print_check(dir_path, exists)
        if not exists:
            all_ok = False
    
    return all_ok


def check_files():
    """Check if required files exist."""
    print_header("Checking Required Files")
    
    required_files = [
        'main.py',
        'requirements.txt',
        'README.md',
        'backend/api/app.py',
        'backend/core/pipeline.py',
        'backend/core/applier.py',
        'backend/db/mssql_manager.py',
        'frontend/templates/index.html'
    ]
    
    all_ok = True
    
    for file_path in required_files:
        path = Path(file_path)
        exists = path.exists() and path.is_file()
        print_check(file_path, exists)
        if not exists:
            all_ok = False
    
    return all_ok


def check_ports():
    """Check if default port is available."""
    print_header("Checking Network Ports")
    
    import socket
    
    port = 8000
    
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(1)
        result = sock.connect_ex(('localhost', port))
        sock.close()
        
        if result == 0:
            print_check(f"Port {port}", False, "Port is already in use")
            print(f"  Another application might be using port {port}")
            return False
        else:
            print_check(f"Port {port}", True, "Port is available")
            return True
            
    except Exception as ex:
        print_check(f"Port {port}", False, f"Error checking port: {ex}")
        return False


def print_summary(results):
    """Print verification summary."""
    print_header("Verification Summary")
    
    total = len(results)
    passed = sum(1 for r in results if r[1])
    failed = total - passed
    
    print(f"\nTotal Checks: {total}")
    print(f"Passed: {passed}")
    print(f"Failed: {failed}")
    
    if failed == 0:
        print("\n✓ All checks passed! You're ready to start the application.")
        print("\nRun the application with:")
        print("  python main.py")
        print("\nThen open your browser to:")
        print("  http://localhost:8000")
    else:
        print("\n✗ Some checks failed. Please resolve the issues above before starting.")
        print("\nFor help, see:")
        print("  - README.md for detailed setup instructions")
        print("  - QUICKSTART.md for a quick start guide")
    
    return failed == 0


def main():
    """Main verification function."""
    print_header("MSSQL CDC Database Synchronizer - Setup Verification")
    print("This script verifies that your system is ready to run the application.")
    
    results = [("Python Version", check_python_version()), ("Dependencies", check_dependencies()),
               ("ODBC Driver", check_odbc_driver()), ("Project Structure", check_directories()),
               ("Required Files", check_files()), ("Network Ports", check_ports())]
    
    # Run all checks

    # Print summary
    success = print_summary(results)
    
    # Return appropriate exit code
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nVerification cancelled by user.")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n✗ Unexpected error during verification: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)





