from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import time

# ── CONFIG ────────────────────────────────────────────────────────────────────
KEYCLOAK_URL       = Variable.get("keycloak_url")
ADMIN_USER         = Variable.get("keycloak_admin_username")
ADMIN_PASSWORD     = Variable.get("keycloak_admin_password")
TOKEN_REFRESH_BUFFER = 60  # seconds before expiry to proactively refresh

REALMS = [
    {
        "name": "auth",
        "users": [
            {"username": "test", "email": "test@suhac.eu", "password": "developer",
             "firstName": "Test", "lastName": "User"},
            {"username": "suhacb", "email": "blaz@suhac.eu", "password": "!2ndArmored",
             "firstName": "Blaz", "lastName": "Suhac"},
        ],
        "client": {
            "clientId":    "auth-client",
            "name":        "Auth Client",
            "description": "Client for the Auth service",
            "publicClient": False,
            "redirectUris": ["http://localhost:9020/*"],
        },
    },
    {
        "name": "nutrients",
        "users": [
            {"username": "test", "email": "test@suhac.eu", "password": "developer",
             "firstName": "Test", "lastName": "User"},
            {"username": "suhacb", "email": "blaz@suhac.eu", "password": "!2ndArmored",
             "firstName": "Blaz", "lastName": "Suhac"},
        ],
        "client": {
            "clientId":    "nutrients-client",
            "name":        "Nutrients Client",
            "description": "Client for the Nutrients service",
            "publicClient": False,
            "redirectUris": ["http://localhost:9010/*"],
        },
    },
    {
        "name": "princess",
        "users": [
            {"username": "suhacb",          "email": "blaz@suhac.eu",                   "password": "!2ndArmored", "firstName": "Blaž",      "lastName": "Suhač"},
            {"username": "pm_customer",     "email": "pm_customer@princess.local",     "password": "developer",   "firstName": "Alexandra", "lastName": "Reid"},
            {"username": "pm_supplier",     "email": "pm_supplier@princess.local",     "password": "developer",   "firstName": "James",     "lastName": "Thornton"},
            {"username": "executive",       "email": "executive@princess.local",       "password": "developer",   "firstName": "Victoria",  "lastName": "Harrington"},
            {"username": "senior_user",     "email": "senior_user@princess.local",     "password": "developer",   "firstName": "Robert",    "lastName": "Chen"},
            {"username": "senior_supplier", "email": "senior_supplier@princess.local", "password": "developer",   "firstName": "Elena",     "lastName": "Kowalski"},
            {"username": "proj_assurance",  "email": "proj_assurance@princess.local",  "password": "developer",   "firstName": "Diana",     "lastName": "Foster"},
            {"username": "proj_support",    "email": "proj_support@princess.local",    "password": "developer",   "firstName": "Marcus",    "lastName": "Webb"},
            {"username": "change_auth",     "email": "change_auth@princess.local",     "password": "developer",   "firstName": "Catherine", "lastName": "Blake"},
            {"username": "team_mgr",        "email": "team_mgr@princess.local",        "password": "developer",   "firstName": "Oliver",    "lastName": "Hayes"},
            {"username": "team_member",     "email": "team_member@princess.local",     "password": "developer",   "firstName": "Sophie",    "lastName": "Nguyen"},
            {"username": "observer",        "email": "observer@princess.local",        "password": "developer",   "firstName": "Nathan",    "lastName": "Okafor"},
        ],
        "client": {
            "clientId":    "princess-client",
            "name":        "Princess Client",
            "description": "Client for the Princess service",
            "publicClient": False,
            "redirectUris": ["http://localhost:10100/*"],
        },
    },
]

# ── TOKEN HELPERS ─────────────────────────────────────────────────────────────

def _fetch_new_token() -> dict:
    """Obtain a fresh token from Keycloak master realm."""
    resp = requests.post(
        f"{KEYCLOAK_URL}/realms/master/protocol/openid-connect/token",
        data={
            "grant_type":    "password",
            "client_id":     "admin-cli",
            "username":      ADMIN_USER,
            "password":      ADMIN_PASSWORD,
        },
    )
    resp.raise_for_status()
    payload = resp.json()
    return {
        "access_token":  payload["access_token"],
        "expires_at":    time.time() + payload["expires_in"],
        "refresh_token": payload.get("refresh_token"),
    }


def _refresh_token(refresh_token: str) -> dict:
    """Use the refresh token to get a new access token."""
    resp = requests.post(
        f"{KEYCLOAK_URL}/realms/master/protocol/openid-connect/token",
        data={
            "grant_type":    "refresh_token",
            "client_id":     "admin-cli",
            "refresh_token": refresh_token,
        },
    )
    resp.raise_for_status()
    payload = resp.json()
    return {
        "access_token":  payload["access_token"],
        "expires_at":    time.time() + payload["expires_in"],
        "refresh_token": payload.get("refresh_token"),
    }


def get_valid_token() -> str:
    """
    Return a valid access token, refreshing/re-fetching as needed.
    Persists token state in Airflow Variables.
    """
    try:
        stored = Variable.get("kc_token_state", deserialize_json=True)
        expires_at    = stored.get("expires_at", 0)
        refresh_token = stored.get("refresh_token")

        if time.time() < expires_at - TOKEN_REFRESH_BUFFER:
            # Token is still fresh — use it
            return stored["access_token"]

        if refresh_token:
            # Token near expiry — try refresh first
            try:
                token_state = _refresh_token(refresh_token)
            except Exception:
                token_state = _fetch_new_token()
        else:
            token_state = _fetch_new_token()

    except (KeyError, ValueError):
        # No stored token yet
        token_state = _fetch_new_token()

    try:
        Variable.set("kc_token_state", token_state, serialize_json=True)
    except Exception:
        # Parallel tasks may race to write this variable; ignore the loser — the token is still valid.
        pass
    return token_state["access_token"]


def _auth_headers() -> dict:
    return {"Authorization": f"Bearer {get_valid_token()}",
            "Content-Type":  "application/json"}

# ── TASK FUNCTIONS ────────────────────────────────────────────────────────────

def task_obtain_token(**ctx):
    """Force-obtain/refresh token at DAG start and push to XCom."""
    token = get_valid_token()
    ctx["ti"].xcom_push(key="access_token", value=token)


def task_create_realm(realm_name: str, **ctx):
    headers = _auth_headers()

    # Check if realm already exists
    existing = requests.get(f"{KEYCLOAK_URL}/admin/realms", headers=headers)
    existing.raise_for_status()
    if any(r["realm"] == realm_name for r in existing.json()):
        print(f"Realm '{realm_name}' already exists — skipping.")
        return

    payload = {
        "realm":   realm_name,
        "enabled": True,
        "displayName": realm_name.capitalize(),
    }
    resp = requests.post(
        f"{KEYCLOAK_URL}/admin/realms",
        headers=headers,
        json=payload,
    )
    if resp.status_code == 409:
        print(f"Realm '{realm_name}' already exists (409) — skipping.")
        return
    resp.raise_for_status()
    print(f"Realm '{realm_name}' created.")


def task_create_user(realm_name: str, user: dict, **ctx):
    headers = _auth_headers()
    base    = f"{KEYCLOAK_URL}/admin/realms/{realm_name}/users"

    # Check if user already exists — use exact=true to avoid prefix matches
    search = requests.get(base, headers=headers,
                          params={"username": user["username"], "exact": "true"})
    search.raise_for_status()
    if any(u["username"] == user["username"] for u in search.json()):
        print(f"User '{user['username']}' already exists in '{realm_name}' — skipping.")
        return

    payload = {
        "username":      user["username"],
        "email":         user["email"],
        "firstName":     user["firstName"],
        "lastName":      user["lastName"],
        "emailVerified": True,
        "enabled":       True,
        "credentials": [{
            "type":      "password",
            "value":     user["password"],
            "temporary": False,
        }],
    }

    resp = requests.post(base, headers=headers, json=payload)
    if resp.status_code == 409:
        print(f"User '{user['username']}' already exists in '{realm_name}' (409) — skipping.")
        return
    resp.raise_for_status()
    print(f"User '{user['username']}' created in realm '{realm_name}'.")


def task_create_client(realm_name: str, client_cfg: dict, **ctx):
    headers = _auth_headers()
    base    = f"{KEYCLOAK_URL}/admin/realms/{realm_name}/clients"

    # Check if client already exists — filter for exact clientId match
    existing = requests.get(base, headers=headers,
                            params={"clientId": client_cfg["clientId"]})
    existing.raise_for_status()
    if any(c["clientId"] == client_cfg["clientId"] for c in existing.json()):
        print(f"Client '{client_cfg['clientId']}' already exists in '{realm_name}' — skipping.")
        return

    resp = requests.post(base, headers=headers, json={
        **client_cfg,
        # Auth flows — enable only Direct Access Grants
        "standardFlowEnabled":         False,  # Authorization Code flow
        "implicitFlowEnabled":         False,  # Implicit flow
        "directAccessGrantsEnabled":   True,   # Direct Access Grants ✓
        "serviceAccountsEnabled":      False,  # Client Credentials flow
    })

    if resp.status_code == 409:
        print(f"Client '{client_cfg['clientId']}' already exists in '{realm_name}' (409) — skipping.")
        return
    resp.raise_for_status()
    print(f"Client '{client_cfg['clientId']}' created in realm '{realm_name}'.")

def task_create_client_mapper(realm_name: str, client_id: str, mapper_cfg: dict, **ctx):
    headers = _auth_headers()

    clients_resp = requests.get(
        f"{KEYCLOAK_URL}/admin/realms/{realm_name}/clients",
        headers=headers,
        params={"clientId": client_id},
    )
    clients_resp.raise_for_status()
    match = next((c for c in clients_resp.json() if c["clientId"] == client_id), None)
    if not match:
        raise ValueError(f"Client '{client_id}' not found in realm '{realm_name}'")
    client_uuid = match["id"]

    mappers_resp = requests.get(
        f"{KEYCLOAK_URL}/admin/realms/{realm_name}/clients/{client_uuid}/protocol-mappers/models",
        headers=headers,
    )
    mappers_resp.raise_for_status()
    if any(m["name"] == mapper_cfg["name"] for m in mappers_resp.json()):
        print(f"Mapper '{mapper_cfg['name']}' already exists on '{client_id}' — skipping.")
        return

    resp = requests.post(
        f"{KEYCLOAK_URL}/admin/realms/{realm_name}/clients/{client_uuid}/protocol-mappers/models",
        headers=headers,
        json=mapper_cfg,
    )
    if resp.status_code == 409:
        print(f"Mapper '{mapper_cfg['name']}' already exists on '{client_id}' (409) — skipping.")
        return
    resp.raise_for_status()
    print(f"Mapper '{mapper_cfg['name']}' created on client '{client_id}'.")


# ── DAG DEFINITION ────────────────────────────────────────────────────────────

with DAG(
    dag_id="keycloak_setup",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,          # manual trigger only
    catchup=False,
    tags=["keycloak", "setup"],
    default_args={
        "retries": 1,
        "retry_delay": timedelta(seconds=10),
    },
) as dag:

    obtain_token = PythonOperator(
        task_id="obtain_token",
        python_callable=task_obtain_token,
    )

    for realm_cfg in REALMS:
        realm_name           = realm_cfg["name"]
        realm_users          = realm_cfg["users"]
        realm_client         = realm_cfg["client"]
        realm_client_mappers = realm_cfg.get("client_mappers", [])

        create_realm = PythonOperator(
            task_id=f"create_realm__{realm_name}",
            python_callable=task_create_realm,
            op_kwargs={"realm_name": realm_name},
        )

        user_task_map = {}
        user_tasks = []
        for u in realm_users:
            t = PythonOperator(
                task_id=f"create_user__{realm_name}__{u['username']}",
                python_callable=task_create_user,
                op_kwargs={"realm_name": realm_name, "user": u},
            )
            user_tasks.append(t)
            user_task_map[u["username"]] = t

        create_client = PythonOperator(
            task_id=f"create_client__{realm_name}",
            python_callable=task_create_client,
            op_kwargs={"realm_name": realm_name, "client_cfg": realm_client},
        )

        mapper_tasks = [
            PythonOperator(
                task_id=f"create_mapper__{realm_name}__{m['name']}",
                python_callable=task_create_client_mapper,
                op_kwargs={
                    "realm_name": realm_name,
                    "client_id":  realm_client["clientId"],
                    "mapper_cfg": m,
                },
            )
            for m in realm_client_mappers
        ]

        # obtain_token → create_realm → [users, client] in parallel
        obtain_token >> create_realm >> user_tasks
        obtain_token >> create_realm >> create_client
        create_client >> mapper_tasks
