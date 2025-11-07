# Security and Secret Management

This document outlines the security practices and secret scanning setup for the deltaflock project.

## Pre-commit Hooks for Secret Scanning

We use pre-commit hooks to automatically scan for secrets before commits are made to the repository. This helps prevent accidental exposure of sensitive information.

### Setup

The pre-commit hooks are already configured in `.pre-commit-config.yaml`. To set them up:

1. **Install pre-commit** (if not already done):
   ```bash
   uv add --dev pre-commit
   ```

2. **Install the hooks**:
   ```bash
   uv run pre-commit install
   ```

3. **Run on all files** (optional, for initial setup):
   ```bash
   uv run pre-commit run --all-files
   ```

### Configured Hooks

Our pre-commit setup includes:

- **detect-private-key**: Detects private keys in files
- **check-added-large-files**: Prevents large files that might contain secrets
- **check-merge-conflict**: Ensures no merge conflicts are committed
- **git-secrets**: Runs git-secrets scan for additional secret patterns
- **Basic file maintenance**: Trailing whitespace and end-of-file fixes

### Manual Secret Scanning

We provide a comprehensive secret scanning script at `scripts/scan_secrets.sh`:

```bash
# Run all available scanners
./scripts/scan_secrets.sh --all

# Run specific scanner
./scripts/scan_secrets.sh --git-secrets
./scripts/scan_secrets.sh --patterns
./scripts/scan_secrets.sh --precommit
```

### Secret Scanning Tools

#### 1. Git-Secrets
Already configured in your repository. Run manually with:
```bash
git secrets --scan
git secrets --scan-history
```

#### 2. GitLeaks (Optional Enhancement)
Install and use GitLeaks for advanced secret detection:
```bash
# Install GitLeaks (macOS)
brew install gitleaks

# Install GitLeaks (Linux)
wget -O gitleaks.tar.gz https://github.com/gitleaks/gitleaks/releases/latest/download/gitleaks_8.18.0_linux_x64.tar.gz
tar -xzf gitleaks.tar.gz && sudo mv gitleaks /usr/local/bin/

# Run scan
gitleaks detect --config=.gitleaks.toml --verbose --redact --source=.
```

#### 3. TruffleHog (Optional Enhancement)
Install and use TruffleHog for comprehensive secret hunting:
```bash
# Install TruffleHog
curl -sSfL https://raw.githubusercontent.com/trufflesecurity/trufflehog/main/scripts/install.sh | sh -s -- -b /usr/local/bin

# Run scan
trufflehog filesystem --directory=. --no-verification
```

### Configuration Files

- `.pre-commit-config.yaml`: Pre-commit hooks configuration
- `.gitleaks.toml`: GitLeaks configuration with custom rules
- `.secrets.baseline`: Baseline file for tracking known false positives
- `scripts/scan_secrets.sh`: Comprehensive secret scanning utility

### Environment Variables and Secrets

#### Proper Secret Management

1. **Never commit secrets to git**:
   - Use `.env` files for local development (already in `.gitignore`)
   - Use environment variables in production
   - Use secret management services (AWS Secrets Manager, Azure Key Vault, etc.)

2. **Environment file template**:
   ```bash
   cp .env.example .env
   # Edit .env with your local values
   ```

3. **In production**, set environment variables through:
   - Docker environment variables
   - Kubernetes secrets
   - Cloud provider secret management services
   - CI/CD pipeline secret variables

#### Common Secret Patterns to Avoid

- API keys: `api_key = "sk-1234567890abcdef"`
- Database URLs: `DATABASE_URL = "postgresql://user:pass@host:port/db"`
- Private keys in code
- JWT tokens in source code
- Cloud provider credentials

### CI/CD Integration

The pre-commit hooks will run automatically on every commit. For CI/CD pipelines, add secret scanning as a pipeline step:

```yaml
# GitHub Actions example
- name: Secret Scan
  run: |
    uv run pre-commit run --all-files
    ./scripts/scan_secrets.sh --patterns
```

### Handling False Positives

If a scanner reports a false positive:

1. **Verify it's actually safe** - Double-check that it's not a real secret
2. **Add to allowlist** - Update `.gitleaks.toml` or `.secrets.baseline`
3. **Use comments** - Add `# nosec` or similar comments to mark intentional cases
4. **Refactor if possible** - Consider if the code can be written differently

### Security Best Practices

1. **Regular audits**: Run `./scripts/scan_secrets.sh --all` regularly
2. **Rotate secrets**: Periodically rotate API keys and tokens
3. **Principle of least privilege**: Use minimal required permissions
4. **Monitor access**: Log and monitor secret access in production
5. **Education**: Train team members on secret management best practices

### Emergency Response

If secrets are accidentally committed:

1. **Immediately rotate the exposed secrets**
2. **Remove from git history**: Use `git filter-branch` or BFG Repo-Cleaner
3. **Force push**: `git push --force-with-lease`
4. **Notify team**: Inform all team members about the incident
5. **Review logs**: Check if the secrets were accessed maliciously

### Tools Installation Guide

#### Installing Git-Secrets
```bash
# macOS
brew install git-secrets

# Linux
git clone https://github.com/awslabs/git-secrets.git
cd git-secrets
make install

# Configure for current repo
git secrets --register-aws
git secrets --install
```

#### Installing GitLeaks
```bash
# macOS
brew install gitleaks

# Linux/Windows
# Download from https://github.com/gitleaks/gitleaks/releases
```

#### Installing TruffleHog
```bash
# Using installer script
curl -sSfL https://raw.githubusercontent.com/trufflesecurity/trufflehog/main/scripts/install.sh | sh -s -- -b /usr/local/bin

# Or download binary from https://github.com/trufflesecurity/trufflehog/releases
```

### Troubleshooting

#### Pre-commit Hook Issues
```bash
# Reinstall hooks
uv run pre-commit uninstall
uv run pre-commit install

# Update hooks to latest versions
uv run pre-commit autoupdate

# Skip hooks temporarily (not recommended)
git commit --no-verify
```

#### Performance Issues
If scanning is slow:
- Use `.gitleaks.toml` allowlist to exclude large directories
- Run scans on changed files only: `pre-commit run`
- Consider using faster alternatives for large repositories

### Resources

- [OWASP Secrets Management Guide](https://owasp.org/www-community/vulnerabilities/Insufficient_Session-ID_Length)
- [GitLeaks Documentation](https://github.com/gitleaks/gitleaks)
- [TruffleHog Documentation](https://github.com/trufflesecurity/trufflehog)
- [Pre-commit Documentation](https://pre-commit.com/)
