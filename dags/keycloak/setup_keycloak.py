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

    Variable.set("kc_token_state", token_state, serialize_json=True)
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
    resp.raise_for_status()
    print(f"Realm '{realm_name}' created.")


def task_create_user(realm_name: str, user: dict, **ctx):
    headers = _auth_headers()
    base    = f"{KEYCLOAK_URL}/admin/realms/{realm_name}/users"

    # Check if user already exists
    search = requests.get(base, headers=headers,
                          params={"username": user["username"]})
    search.raise_for_status()
    if search.json():
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
    resp.raise_for_status()
    print(f"User '{user['username']}' created in realm '{realm_name}'.")


def task_create_client(realm_name: str, client_cfg: dict, **ctx):
    headers = _auth_headers()
    base    = f"{KEYCLOAK_URL}/admin/realms/{realm_name}/clients"

    # Check if client already exists
    existing = requests.get(base, headers=headers,
                            params={"clientId": client_cfg["clientId"]})
    existing.raise_for_status()
    if existing.json():
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

    resp.raise_for_status()
    print(f"Client '{client_cfg['clientId']}' created in realm '{realm_name}'.")

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
        realm_name  = realm_cfg["name"]
        realm_users = realm_cfg["users"]
        realm_client = realm_cfg["client"]

        create_realm = PythonOperator(
            task_id=f"create_realm__{realm_name}",
            python_callable=task_create_realm,
            op_kwargs={"realm_name": realm_name},
        )

        user_tasks = [
            PythonOperator(
                task_id=f"create_user__{realm_name}__{u['username']}",
                python_callable=task_create_user,
                op_kwargs={"realm_name": realm_name, "user": u},
            )
            for u in realm_users
        ]

        create_client = PythonOperator(
            task_id=f"create_client__{realm_name}",
            python_callable=task_create_client,
            op_kwargs={"realm_name": realm_name, "client_cfg": realm_client},
        )

        # obtain_token → create_realm → [users + client] in parallel
        obtain_token >> create_realm >> user_tasks
        obtain_token >> create_realm >> create_client
