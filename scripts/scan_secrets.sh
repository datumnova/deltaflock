#!/bin/bash

# Secret scanning utility script for Golduck project
# This script provides various secret scanning options

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_header() {
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE} Golduck Secret Scanner${NC}"
    echo -e "${BLUE}========================================${NC}"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

# Function to run git-secrets scan
run_git_secrets() {
    echo -e "\n${BLUE}Running git-secrets scan...${NC}"
    if command -v git-secrets &> /dev/null; then
        if git secrets --scan-history; then
            print_success "git-secrets scan completed successfully"
        else
            print_error "git-secrets found potential secrets!"
            return 1
        fi
    else
        print_warning "git-secrets not installed. Install with: brew install git-secrets"
        return 1
    fi
}

# Function to run gitleaks scan
run_gitleaks() {
    echo -e "\n${BLUE}Running gitleaks scan...${NC}"
    if command -v gitleaks &> /dev/null; then
        if gitleaks detect --config="${PROJECT_ROOT}/.gitleaks.toml" --verbose --redact --source="${PROJECT_ROOT}"; then
            print_success "gitleaks scan completed successfully"
        else
            print_error "gitleaks found potential secrets!"
            return 1
        fi
    else
        print_warning "gitleaks not installed. Install from: https://github.com/gitleaks/gitleaks"
        return 1
    fi
}

# Function to run trufflehog scan
run_trufflehog() {
    echo -e "\n${BLUE}Running trufflehog scan...${NC}"
    if command -v trufflehog &> /dev/null; then
        if trufflehog filesystem --directory="${PROJECT_ROOT}" --no-verification; then
            print_success "trufflehog scan completed successfully"
        else
            print_error "trufflehog found potential secrets!"
            return 1
        fi
    else
        print_warning "trufflehog not installed. Install from: https://github.com/trufflesecurity/trufflehog"
        return 1
    fi
}

# Function to check for common secret patterns
check_common_patterns() {
    echo -e "\n${BLUE}Checking for common secret patterns...${NC}"

    local found_issues=0

    # Check for common secret patterns in code files
    echo "Checking for potential API keys..."
    if grep -r -E "api[_-]?key.*=.*['\"][a-zA-Z0-9_-]{20,}['\"]" "${PROJECT_ROOT}/src" "${PROJECT_ROOT}/tests" 2>/dev/null; then
        print_error "Found potential API keys in code"
        found_issues=1
    fi

    echo "Checking for potential tokens..."
    if grep -r -E "token.*=.*['\"][a-zA-Z0-9_-]{20,}['\"]" "${PROJECT_ROOT}/src" "${PROJECT_ROOT}/tests" 2>/dev/null; then
        print_error "Found potential tokens in code"
        found_issues=1
    fi

    echo "Checking for potential passwords..."
    if grep -r -E "password.*=.*['\"][^'\"]{8,}['\"]" "${PROJECT_ROOT}/src" "${PROJECT_ROOT}/tests" 2>/dev/null | grep -v -E "(example|test|dummy|placeholder)"; then
        print_error "Found potential hardcoded passwords"
        found_issues=1
    fi

    # Check for .env files in version control
    if find "${PROJECT_ROOT}" -name ".env*" -not -path "*/.git/*" -not -name ".env.example" -not -name ".env.template" | grep -q .; then
        print_warning "Found .env files that might contain secrets"
        find "${PROJECT_ROOT}" -name ".env*" -not -path "*/.git/*" -not -name ".env.example" -not -name ".env.template"
    fi

    if [ $found_issues -eq 0 ]; then
        print_success "No common secret patterns found"
    fi

    return $found_issues
}

# Function to run pre-commit hooks
run_precommit() {
    echo -e "\n${BLUE}Running pre-commit hooks...${NC}"
    cd "${PROJECT_ROOT}"
    if command -v uv &> /dev/null; then
        if uv run pre-commit run --all-files; then
            print_success "Pre-commit hooks passed"
        else
            print_error "Pre-commit hooks found issues!"
            return 1
        fi
    else
        print_warning "uv not available, trying pre-commit directly"
        if command -v pre-commit &> /dev/null; then
            if pre-commit run --all-files; then
                print_success "Pre-commit hooks passed"
            else
                print_error "Pre-commit hooks found issues!"
                return 1
            fi
        else
            print_error "Neither uv nor pre-commit available"
            return 1
        fi
    fi
}

# Function to display usage
usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  -a, --all           Run all available secret scanners"
    echo "  -g, --git-secrets   Run git-secrets scan only"
    echo "  -l, --gitleaks      Run gitleaks scan only"
    echo "  -t, --trufflehog    Run trufflehog scan only"
    echo "  -p, --patterns      Check for common secret patterns"
    echo "  -c, --precommit     Run pre-commit hooks"
    echo "  -h, --help          Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 --all                    # Run all scanners"
    echo "  $0 --git-secrets            # Run only git-secrets"
    echo "  $0 --precommit              # Run pre-commit hooks"
}

# Main function
main() {
    print_header

    if [ $# -eq 0 ]; then
        usage
        exit 1
    fi

    local exit_code=0

    case "$1" in
        -a|--all)
            run_git_secrets || exit_code=1
            run_gitleaks || exit_code=1
            run_trufflehog || exit_code=1
            check_common_patterns || exit_code=1
            run_precommit || exit_code=1
            ;;
        -g|--git-secrets)
            run_git_secrets || exit_code=1
            ;;
        -l|--gitleaks)
            run_gitleaks || exit_code=1
            ;;
        -t|--trufflehog)
            run_trufflehog || exit_code=1
            ;;
        -p|--patterns)
            check_common_patterns || exit_code=1
            ;;
        -c|--precommit)
            run_precommit || exit_code=1
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            usage
            exit 1
            ;;
    esac

    if [ $exit_code -eq 0 ]; then
        echo -e "\n${GREEN}🎉 All scans completed successfully!${NC}"
    else
        echo -e "\n${RED}❌ Some scans found issues. Please review and fix them.${NC}"
    fi

    exit $exit_code
}

# Run main function with all arguments
main "$@"
