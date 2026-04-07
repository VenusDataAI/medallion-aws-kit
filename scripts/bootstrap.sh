#!/usr/bin/env bash
# bootstrap.sh — Zero to lakehouse in one command.
#
# Usage:
#   export TF_VAR_redshift_admin_password="<password>"
#   ./scripts/bootstrap.sh [--env dev|prod] [--skip-terraform] [--skip-dbt]
#
# Idempotent: safe to run multiple times.

set -euo pipefail

# ── Defaults ──────────────────────────────────────────────────────────────────
ENVIRONMENT="dev"
SKIP_TERRAFORM=false
SKIP_DBT=false
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# ── Argument parsing ──────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        --env)        ENVIRONMENT="$2"; shift 2 ;;
        --skip-terraform) SKIP_TERRAFORM=true; shift ;;
        --skip-dbt)   SKIP_DBT=true; shift ;;
        *) echo "Unknown argument: $1"; exit 1 ;;
    esac
done

if [[ "$ENVIRONMENT" != "dev" && "$ENVIRONMENT" != "prod" ]]; then
    echo "ERROR: --env must be 'dev' or 'prod'" >&2
    exit 1
fi

echo "=========================================="
echo " Medallion AWS Kit Bootstrap"
echo " Environment : $ENVIRONMENT"
echo " Repo root   : $REPO_ROOT"
echo "=========================================="

# ── Prerequisite checks ───────────────────────────────────────────────────────
check_cmd() {
    if ! command -v "$1" &>/dev/null; then
        echo "ERROR: '$1' is not installed or not in PATH." >&2
        exit 1
    fi
}

check_cmd terraform
check_cmd dbt
check_cmd python3

if [[ -z "${TF_VAR_redshift_admin_password:-}" ]] && [[ "$SKIP_TERRAFORM" == "false" ]]; then
    echo "ERROR: Set TF_VAR_redshift_admin_password before running." >&2
    exit 1
fi

# ── Terraform ─────────────────────────────────────────────────────────────────
if [[ "$SKIP_TERRAFORM" == "false" ]]; then
    TF_DIR="$REPO_ROOT/terraform/environments/$ENVIRONMENT"
    echo ""
    echo "── Terraform: $ENVIRONMENT ──────────────────────────────"
    cd "$TF_DIR"

    echo "[1/3] terraform init"
    terraform init -input=false

    echo "[2/3] terraform plan"
    terraform plan -input=false -out=tfplan

    echo "[3/3] terraform apply"
    terraform apply -input=false tfplan

    cd "$REPO_ROOT"
fi

# ── dbt ───────────────────────────────────────────────────────────────────────
if [[ "$SKIP_DBT" == "false" ]]; then
    DBT_DIR="$REPO_ROOT/dbt"
    echo ""
    echo "── dbt ──────────────────────────────────────────────────"
    cd "$DBT_DIR"

    echo "[1/3] dbt deps"
    dbt deps

    echo "[2/3] dbt run"
    dbt run --target "$ENVIRONMENT"

    echo "[3/3] dbt test"
    dbt test --target "$ENVIRONMENT"

    cd "$REPO_ROOT"
fi

# ── Python tests ──────────────────────────────────────────────────────────────
echo ""
echo "── Python tests ─────────────────────────────────────────"
cd "$REPO_ROOT"
python3 -m pytest tests/ -v --tb=short

echo ""
echo "=========================================="
echo " Bootstrap complete!"
echo "=========================================="
