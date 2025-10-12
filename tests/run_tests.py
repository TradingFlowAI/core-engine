"""
Test Runner for Community Node System (Station)
社区节点系统测试运行器 - Station层
"""

import sys
import pytest
import os
from pathlib import Path

# Colors for console output
class Colors:
    RESET = '\033[0m'
    BRIGHT = '\033[1m'
    GREEN = '\033[32m'
    RED = '\033[31m'
    YELLOW = '\033[33m'
    BLUE = '\033[34m'
    CYAN = '\033[36m'


def print_header():
    """打印测试套件头部"""
    print(f"{Colors.BRIGHT}{Colors.CYAN}")
    print("╔═══════════════════════════════════════════════════════════════╗")
    print("║     TradingFlow Community Node Test Suite (Station)         ║")
    print("╚═══════════════════════════════════════════════════════════════╝")
    print(f"{Colors.RESET}\n")


def find_test_files():
    """查找所有测试文件"""
    test_dir = Path(__file__).parent
    test_files = []
    
    for file_path in test_dir.glob('test_*.py'):
        if file_path.name != 'run_tests.py':
            test_files.append(str(file_path))
    
    return sorted(test_files)


def main():
    """主测试函数"""
    print_header()
    
    # Find test files
    test_files = find_test_files()
    
    print(f"{Colors.BLUE}Found {len(test_files)} test files:{Colors.RESET}")
    for idx, file_path in enumerate(test_files, 1):
        file_name = Path(file_path).name
        print(f"  {idx}. {file_name}")
    print()
    
    # Prepare pytest arguments
    pytest_args = [
        '-v',  # Verbose
        '--tb=short',  # Short traceback
        '--color=yes',  # Colored output
        '-s',  # No capture (show print statements)
    ]
    
    # Add verbose flag if set
    if os.getenv('TEST_VERBOSE') == 'true':
        pytest_args.extend(['-vv', '--tb=long'])
    
    # Add coverage if requested
    if os.getenv('TEST_COVERAGE') == 'true':
        pytest_args.extend([
            '--cov=core',
            '--cov-report=term-missing',
            '--cov-report=html'
        ])
    
    # Add test files
    pytest_args.extend(test_files)
    
    # Run tests
    print(f"{Colors.YELLOW}Running tests...{Colors.RESET}\n")
    exit_code = pytest.main(pytest_args)
    
    # Print summary
    print(f"\n{Colors.BRIGHT}{'=' * 65}{Colors.RESET}")
    
    if exit_code == 0:
        print(f"{Colors.GREEN}{Colors.BRIGHT}")
        print("✓ All tests passed!")
        print(f"{Colors.RESET}")
    else:
        print(f"{Colors.RED}{Colors.BRIGHT}")
        print(f"✗ Some tests failed (exit code: {exit_code})")
        print(f"{Colors.RESET}")
    
    print(f"{Colors.BRIGHT}{'=' * 65}{Colors.RESET}\n")
    
    return exit_code


if __name__ == '__main__':
    sys.exit(main())
