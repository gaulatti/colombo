#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if ! command -v psql >/dev/null 2>&1; then
  echo "psql is required but not installed."
  echo "Install PostgreSQL client tools (e.g. brew install libpq && brew link --force libpq)."
  exit 1
fi

load_env_file() {
  local file="$1"
  if [[ -f "$file" ]]; then
    # shellcheck disable=SC1090
    set -a && source "$file" && set +a
  fi
}

load_env_file "$ROOT_DIR/.env"
load_env_file "$ROOT_DIR/../.env"

DB_URL="${DATABASE_URL:-${DB_URL:-}}"
DB_USER="${DATABASE_USER:-${DB_USERNAME:-}}"
DB_PASSWORD="${DATABASE_PASSWORD:-${DB_PASSWORD:-}}"

if [[ -z "${DB_URL:-}" ]]; then
  echo "Missing DATABASE_URL (or DB_URL)."
  exit 1
fi

if [[ -z "${DB_USER:-}" ]]; then
  echo "Missing DATABASE_USER (or DB_USERNAME)."
  exit 1
fi

if [[ "$DB_URL" =~ ^jdbc:postgresql://([^:/]+):([0-9]+)/([^?]+)$ ]]; then
  DB_HOST="${BASH_REMATCH[1]}"
  DB_PORT="${BASH_REMATCH[2]}"
  DB_NAME="${BASH_REMATCH[3]}"
elif [[ "$DB_URL" =~ ^jdbc:postgresql://([^/]+)/([^?]+)$ ]]; then
  DB_HOST="${BASH_REMATCH[1]}"
  DB_PORT="5432"
  DB_NAME="${BASH_REMATCH[2]}"
else
  echo "Unsupported DATABASE_URL format: $DB_URL"
  echo "Expected: jdbc:postgresql://<host>:<port>/<database>"
  exit 1
fi

psql_cmd() {
  PGPASSWORD="${DB_PASSWORD:-}" psql \
    -X \
    -h "$DB_HOST" \
    -p "$DB_PORT" \
    -U "$DB_USER" \
    -d "$DB_NAME" \
    "$@"
}

sql_quote() {
  local value="${1:-}"
  value=${value//\'/\'\'}
  printf "'%s'" "$value"
}

sql_nullable() {
  local value="${1:-}"
  if [[ -z "$value" ]]; then
    printf "NULL"
  else
    sql_quote "$value"
  fi
}

generate_api_key() {
  # URL-safe random token, prefixed for easier identification in logs.
  local raw=""
  if command -v openssl >/dev/null 2>&1; then
    raw="$(openssl rand -base64 48 | tr -d '\n' | tr '+/' '-_' | tr -d '=')"
  else
    raw="$(head -c 48 /dev/urandom | base64 | tr -d '\n' | tr '+/' '-_' | tr -d '=')"
  fi
  printf 'ck_%s' "$raw"
}

validate_id() {
  local id="${1:-}"
  [[ "$id" =~ ^[0-9]+$ ]]
}

pause() {
  read -r -p "Press Enter to continue..."
}

prompt_required() {
  local label="$1"
  local value=""
  while true; do
    read -r -p "$label: " value
    if [[ -n "$value" ]]; then
      printf '%s' "$value"
      return 0
    fi
    echo "$label is required."
  done
}

prompt_with_default() {
  local label="$1"
  local default="$2"
  local value=""
  read -r -p "$label [$default]: " value
  if [[ -z "$value" ]]; then
    printf '%s' "$default"
  else
    printf '%s' "$value"
  fi
}

list_tenants() {
  psql_cmd -P pager=off -c "
    SELECT
      id,
      name,
      ftp_username
    FROM tenants
    ORDER BY id;
  "
}

view_tenant() {
  local id
  read -r -p "Enter tenant id: " id
  if ! validate_id "$id"; then
    echo "Tenant id must be a positive integer."
    return 1
  fi

  psql_cmd -P pager=off -v id="$id" -c "
    SELECT
      id,
      name,
      ftp_username,
      api_key,
      validation_endpoint,
      photo_endpoint
    FROM tenants
    WHERE id = $id;
  "
}

create_tenant() {
  local name ftp_username api_key validation_endpoint photo_endpoint

  echo "Create tenant"
  name="$(prompt_required "Name")"
  ftp_username="$(prompt_required "FTP username")"
  validation_endpoint="$(prompt_required "Validation endpoint")"
  photo_endpoint="$(prompt_required "Photo endpoint")"
  api_key="$(generate_api_key)"
  echo
  echo "Generated API key:"
  echo "$api_key"

  psql_cmd -v ON_ERROR_STOP=1 \
    -c "
      INSERT INTO tenants (
        name,
        ftp_username,
        api_key,
        validation_endpoint,
        photo_endpoint
      )
      VALUES (
        $(sql_quote "$name"),
        $(sql_quote "$ftp_username"),
        $(sql_quote "$api_key"),
        $(sql_quote "$validation_endpoint"),
        $(sql_quote "$photo_endpoint")
      );
    "
  echo "Tenant created."
}

update_tenant() {
  local id row
  read -r -p "Enter tenant id to update: " id
  if ! validate_id "$id"; then
    echo "Tenant id must be a positive integer."
    return 1
  fi

  row="$(psql_cmd -t -A -F $'\t' -c "
    SELECT
      name,
      ftp_username,
      api_key,
      validation_endpoint,
      photo_endpoint
    FROM tenants
    WHERE id = $id;
  ")"

  if [[ -z "$row" ]]; then
    echo "Tenant id '$id' not found."
    return 1
  fi

  IFS=$'\t' read -r cur_name cur_ftp_username cur_api_key cur_validation_endpoint cur_photo_endpoint <<< "$row"

  echo "Update tenant id=$id (press Enter to keep current value)"
  echo "Current API key (read-only): $cur_api_key"
  local name ftp_username validation_endpoint photo_endpoint
  name="$(prompt_with_default "Name" "$cur_name")"
  ftp_username="$(prompt_with_default "FTP username" "$cur_ftp_username")"
  validation_endpoint="$(prompt_with_default "Validation endpoint" "$cur_validation_endpoint")"
  photo_endpoint="$(prompt_with_default "Photo endpoint" "$cur_photo_endpoint")"

  psql_cmd -v ON_ERROR_STOP=1 \
    -c "
      UPDATE tenants
      SET
        name = $(sql_quote "$name"),
        ftp_username = $(sql_quote "$ftp_username"),
        api_key = $(sql_quote "$cur_api_key"),
        validation_endpoint = $(sql_quote "$validation_endpoint"),
        photo_endpoint = $(sql_quote "$photo_endpoint")
      WHERE id = $id;
    "
  echo "Tenant updated."
}

rotate_tenant_api_key() {
  local id row confirm new_api_key
  read -r -p "Enter tenant id to rotate API key: " id
  if ! validate_id "$id"; then
    echo "Tenant id must be a positive integer."
    return 1
  fi

  row="$(psql_cmd -t -A -F $'\t' -c "
    SELECT id, name, ftp_username
    FROM tenants
    WHERE id = $id;
  ")"

  if [[ -z "$row" ]]; then
    echo "Tenant id '$id' not found."
    return 1
  fi

  IFS=$'\t' read -r tenant_id tenant_name tenant_ftp_username <<< "$row"
  echo "Tenant: id=$tenant_id name=$tenant_name ftp_username=$tenant_ftp_username"
  read -r -p "Rotate API key now? Type ROTATE to confirm: " confirm
  if [[ "$confirm" != "ROTATE" ]]; then
    echo "Rotation canceled."
    return 0
  fi

  new_api_key="$(generate_api_key)"
  psql_cmd -v ON_ERROR_STOP=1 -c "
    UPDATE tenants
    SET api_key = $(sql_quote "$new_api_key")
    WHERE id = $id;
  "

  echo "API key rotated."
  echo "New API key:"
  echo "$new_api_key"
}

delete_tenant() {
  local id confirm
  read -r -p "Enter tenant id to delete: " id
  if ! validate_id "$id"; then
    echo "Tenant id must be a positive integer."
    return 1
  fi

  psql_cmd -P pager=off -c "
    SELECT id, name, ftp_username
    FROM tenants
    WHERE id = $id;
  "

  read -r -p "Type DELETE to confirm: " confirm
  if [[ "$confirm" != "DELETE" ]]; then
    echo "Delete canceled."
    return 0
  fi

  psql_cmd -v ON_ERROR_STOP=1 -c "DELETE FROM tenants WHERE id = $id;"
  echo "Tenant deleted."
}

while true; do
  if [[ -t 1 ]] && command -v clear >/dev/null 2>&1; then
    clear
  fi
  cat <<EOF
Colombo Tenant Manager
DB: ${DB_HOST}:${DB_PORT}/${DB_NAME}

1) List tenants
2) View tenant by id
3) Create tenant
4) Update tenant
5) Rotate tenant API key
6) Delete tenant
7) Exit
EOF

  read -r -p "Select an option [1-7]: " choice

  case "$choice" in
    1)
      list_tenants || true
      pause
      ;;
    2)
      view_tenant || true
      pause
      ;;
    3)
      create_tenant || true
      pause
      ;;
    4)
      update_tenant || true
      pause
      ;;
    5)
      rotate_tenant_api_key || true
      pause
      ;;
    6)
      delete_tenant || true
      pause
      ;;
    7)
      echo "Bye."
      exit 0
      ;;
    *)
      echo "Invalid option."
      pause
      ;;
  esac
done
