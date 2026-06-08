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
        "roles": [
            "executive", "senior_user", "senior_supplier", "project_manager",
            "project_assurance", "project_support", "change_authority",
            "team_manager", "team_member", "observer",
        ],
        "groups": ["customer", "supplier", "neutral"],
        "client_mappers": [
            {
                "name": "groups",
                "protocol": "openid-connect",
                "protocolMapper": "oidc-group-membership-mapper",
                "consentRequired": False,
                "config": {
                    "claim.name":         "groups",
                    "full.path":          "true",
                    "id.token.claim":     "false",
                    "access.token.claim": "true",
                    "userinfo.token.claim": "false",
                },
            },
        ],
        "users": [
            {"username": "suhacb",          "email": "blaz@suhac.eu",                   "password": "!2ndArmored", "firstName": "Blaz",    "lastName": "Suhac"},
            {"username": "pm_customer",     "email": "pm_customer@princess.local",     "password": "developer",   "firstName": "PM",      "lastName": "Customer",   "role": "project_manager",   "group": "customer"},
            {"username": "pm_supplier",     "email": "pm_supplier@princess.local",     "password": "developer",   "firstName": "PM",      "lastName": "Supplier",   "role": "project_manager",   "group": "supplier"},
            {"username": "executive",       "email": "executive@princess.local",       "password": "developer",   "firstName": "Executive","lastName": "User",       "role": "executive",         "group": "customer"},
            {"username": "senior_user",     "email": "senior_user@princess.local",     "password": "developer",   "firstName": "Senior",  "lastName": "User",       "role": "senior_user",       "group": "customer"},
            {"username": "senior_supplier", "email": "senior_supplier@princess.local", "password": "developer",   "firstName": "Senior",  "lastName": "Supplier",   "role": "senior_supplier",   "group": "supplier"},
            {"username": "proj_assurance",  "email": "proj_assurance@princess.local",  "password": "developer",   "firstName": "Project", "lastName": "Assurance",  "role": "project_assurance", "group": "customer"},
            {"username": "proj_support",    "email": "proj_support@princess.local",    "password": "developer",   "firstName": "Project", "lastName": "Support",    "role": "project_support",   "group": "customer"},
            {"username": "change_auth",     "email": "change_auth@princess.local",     "password": "developer",   "firstName": "Change",  "lastName": "Authority",  "role": "change_authority",  "group": "customer"},
            {"username": "team_mgr",        "email": "team_mgr@princess.local",        "password": "developer",   "firstName": "Team",    "lastName": "Manager",    "role": "team_manager",      "group": "supplier"},
            {"username": "team_member",     "email": "team_member@princess.local",     "password": "developer",   "firstName": "Team",    "lastName": "Member",     "role": "team_member",       "group": "supplier"},
            {"username": "observer",        "email": "observer@princess.local",        "password": "developer",   "firstName": "Observer","lastName": "User",       "role": "observer",          "group": "neutral"},
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

def task_create_realm_role(realm_name: str, role_name: str, **ctx):
    headers = _auth_headers()
    base = f"{KEYCLOAK_URL}/admin/realms/{realm_name}/roles"

    existing = requests.get(f"{base}/{role_name}", headers=headers)
    if existing.status_code == 200:
        print(f"Role '{role_name}' already exists in '{realm_name}' — skipping.")
        return

    resp = requests.post(base, headers=headers, json={"name": role_name})
    if resp.status_code == 409:
        print(f"Role '{role_name}' already exists in '{realm_name}' (409) — skipping.")
        return
    resp.raise_for_status()
    print(f"Role '{role_name}' created in realm '{realm_name}'.")


def task_create_group(realm_name: str, group_name: str, **ctx):
    headers = _auth_headers()
    base = f"{KEYCLOAK_URL}/admin/realms/{realm_name}/groups"

    existing = requests.get(base, headers=headers, params={"search": group_name, "exact": "true"})
    existing.raise_for_status()
    if any(g["name"] == group_name for g in existing.json()):
        print(f"Group '{group_name}' already exists in '{realm_name}' — skipping.")
        return

    resp = requests.post(base, headers=headers, json={"name": group_name})
    if resp.status_code == 409:
        print(f"Group '{group_name}' already exists in '{realm_name}' (409) — skipping.")
        return
    resp.raise_for_status()
    print(f"Group '{group_name}' created in realm '{realm_name}'.")


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


def task_assign_user_role(realm_name: str, username: str, role_name: str, **ctx):
    headers = _auth_headers()

    users_resp = requests.get(
        f"{KEYCLOAK_URL}/admin/realms/{realm_name}/users",
        headers=headers,
        params={"username": username, "exact": "true"},
    )
    users_resp.raise_for_status()
    user = next((u for u in users_resp.json() if u["username"] == username), None)
    if not user:
        raise ValueError(f"User '{username}' not found in realm '{realm_name}'")

    role_resp = requests.get(
        f"{KEYCLOAK_URL}/admin/realms/{realm_name}/roles/{role_name}",
        headers=headers,
    )
    role_resp.raise_for_status()

    assigned_resp = requests.get(
        f"{KEYCLOAK_URL}/admin/realms/{realm_name}/users/{user['id']}/role-mappings/realm",
        headers=headers,
    )
    assigned_resp.raise_for_status()
    if any(r["name"] == role_name for r in assigned_resp.json()):
        print(f"Role '{role_name}' already assigned to '{username}' — skipping.")
        return

    resp = requests.post(
        f"{KEYCLOAK_URL}/admin/realms/{realm_name}/users/{user['id']}/role-mappings/realm",
        headers=headers,
        json=[role_resp.json()],
    )
    resp.raise_for_status()
    print(f"Role '{role_name}' assigned to '{username}' in '{realm_name}'.")


def task_assign_user_group(realm_name: str, username: str, group_name: str, **ctx):
    headers = _auth_headers()

    users_resp = requests.get(
        f"{KEYCLOAK_URL}/admin/realms/{realm_name}/users",
        headers=headers,
        params={"username": username, "exact": "true"},
    )
    users_resp.raise_for_status()
    user = next((u for u in users_resp.json() if u["username"] == username), None)
    if not user:
        raise ValueError(f"User '{username}' not found in realm '{realm_name}'")

    groups_resp = requests.get(
        f"{KEYCLOAK_URL}/admin/realms/{realm_name}/groups",
        headers=headers,
        params={"search": group_name, "exact": "true"},
    )
    groups_resp.raise_for_status()
    group = next((g for g in groups_resp.json() if g["name"] == group_name), None)
    if not group:
        raise ValueError(f"Group '{group_name}' not found in realm '{realm_name}'")

    user_groups_resp = requests.get(
        f"{KEYCLOAK_URL}/admin/realms/{realm_name}/users/{user['id']}/groups",
        headers=headers,
    )
    user_groups_resp.raise_for_status()
    if any(g["id"] == group["id"] for g in user_groups_resp.json()):
        print(f"User '{username}' already in group '{group_name}' — skipping.")
        return

    resp = requests.put(
        f"{KEYCLOAK_URL}/admin/realms/{realm_name}/users/{user['id']}/groups/{group['id']}",
        headers=headers,
    )
    resp.raise_for_status()
    print(f"User '{username}' added to group '{group_name}' in '{realm_name}'.")


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
        realm_roles          = realm_cfg.get("roles", [])
        realm_groups         = realm_cfg.get("groups", [])
        realm_client_mappers = realm_cfg.get("client_mappers", [])

        create_realm = PythonOperator(
            task_id=f"create_realm__{realm_name}",
            python_callable=task_create_realm,
            op_kwargs={"realm_name": realm_name},
        )

        role_tasks = [
            PythonOperator(
                task_id=f"create_role__{realm_name}__{role}",
                python_callable=task_create_realm_role,
                op_kwargs={"realm_name": realm_name, "role_name": role},
            )
            for role in realm_roles
        ]

        group_tasks = [
            PythonOperator(
                task_id=f"create_group__{realm_name}__{group}",
                python_callable=task_create_group,
                op_kwargs={"realm_name": realm_name, "group_name": group},
            )
            for group in realm_groups
        ]

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

        # obtain_token → create_realm → [roles, groups, users, client] in parallel
        obtain_token >> create_realm >> role_tasks
        obtain_token >> create_realm >> group_tasks
        obtain_token >> create_realm >> user_tasks
        obtain_token >> create_realm >> create_client
        create_client >> mapper_tasks

        # Role/group assignment — requires both the user and the role/group to exist
        for u in realm_users:
            user_task = user_task_map[u["username"]]
            if "role" in u:
                assign_role = PythonOperator(
                    task_id=f"assign_role__{realm_name}__{u['username']}",
                    python_callable=task_assign_user_role,
                    op_kwargs={"realm_name": realm_name, "username": u["username"], "role_name": u["role"]},
                )
                [user_task, *role_tasks] >> assign_role
            if "group" in u:
                assign_group = PythonOperator(
                    task_id=f"assign_group__{realm_name}__{u['username']}",
                    python_callable=task_assign_user_group,
                    op_kwargs={"realm_name": realm_name, "username": u["username"], "group_name": u["group"]},
                )
                [user_task, *group_tasks] >> assign_group
