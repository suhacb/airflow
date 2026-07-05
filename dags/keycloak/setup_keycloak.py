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
            "clientId":                  "auth-client",
            "name":                      "Auth Client",
            "description":               "Client for the Auth service",
            "publicClient":              False,
            "standardFlowEnabled":       False,
            "directAccessGrantsEnabled": True,
            "redirectUris":              ["http://localhost:9020/*"],
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
            "clientId":                  "nutrients-client",
            "name":                      "Nutrients Client",
            "description":               "Client for the Nutrients service",
            "publicClient":              False,
            "standardFlowEnabled":       False,
            "directAccessGrantsEnabled": True,
            "redirectUris":              ["http://localhost:9010/*"],
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
            "clientId":                  "princess-client",
            "name":                      "Princess Client",
            "description":               "Client for the Princess service",
            "publicClient":              False,
            "standardFlowEnabled":       False,
            "directAccessGrantsEnabled": True,
            "redirectUris":              ["http://localhost:10100/*"],
        },
    },
    {
        "name": "ens3",
        "users": [
            {"username": "blaz.suhac",    "email": "blaz@suhac.eu",               "password": "developer", "firstName": "Blaž",   "lastName": "Suhač"},
            {"username": "ana.kovac",     "email": "ana.kovac@ens3.local",         "password": "developer", "firstName": "Ana",    "lastName": "Kovač"},
            {"username": "boris.novak",   "email": "boris.novak@ens3.local",       "password": "developer", "firstName": "Boris",  "lastName": "Novak"},
            {"username": "cvetka.vidmar", "email": "cvetka.vidmar@ens3.local",     "password": "developer", "firstName": "Cvetka", "lastName": "Vidmar"},
            {"username": "darko.zorman",  "email": "darko.zorman@ens3.local",      "password": "developer", "firstName": "Darko",  "lastName": "Zorman"},
        ],
        "groups": ["tajniki", "predsedniki", "svetniki", "opazovalci"],
        "user_groups": [
            {"username": "blaz.suhac",    "group": "tajniki"},
            {"username": "blaz.suhac",    "group": "predsedniki"},
            {"username": "ana.kovac",     "group": "tajniki"},
            {"username": "boris.novak",   "group": "predsedniki"},
            {"username": "cvetka.vidmar", "group": "svetniki"},
            {"username": "darko.zorman",  "group": "opazovalci"},
        ],
        "client": {
            "clientId":                  "ens3-app",
            "name":                      "ENS3 App",
            "description":               "ENS3 meeting management application",
            "publicClient":              False,
            "standardFlowEnabled":       True,
            "implicitFlowEnabled":       False,
            "directAccessGrantsEnabled": True,
            "serviceAccountsEnabled":    False,
            "redirectUris": [
                "http://localhost:40000/*",
                "http://localhost:40001/*",
            ],
            "webOrigins": [
                "http://localhost:40000",
                "http://localhost:40001",
            ],
            "attributes": {
                "pkce.code.challenge.method": "S256",
            },
        },
        "client_mappers": [
            {
                "name":           "groups",
                "protocol":       "openid-connect",
                "protocolMapper": "oidc-group-membership-mapper",
                "config": {
                    "claim.name":           "groups",
                    "full.path":            "false",
                    "id.token.claim":       "true",
                    "access.token.claim":   "true",
                    "userinfo.token.claim": "true",
                },
            },
        ],
        "service_account_clients": [
            {
                "clientId":                     "ens3-admin-sync",
                "name":                          "ENS3 Admin Sync",
                "description":                   "Service account client for ENS3 identity sync (read-only Keycloak Admin API access)",
                "protocol":                      "openid-connect",
                "publicClient":                  False,
                "standardFlowEnabled":           False,
                "implicitFlowEnabled":           False,
                "directAccessGrantsEnabled":     False,
                "serviceAccountsEnabled":        True,
                "authorizationServicesEnabled":  False,
                "redirectUris":                  [],
                "webOrigins":                    [],
                "service_account_roles": {
                    "client_id": "realm-management",
                    "roles":     ["view-users", "query-users", "query-groups"],
                },
            },
        ],
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

    resp = requests.post(base, headers=headers, json=client_cfg)

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


def _get_client_uuid(realm_name: str, client_id: str, headers: dict) -> str:
    resp = requests.get(
        f"{KEYCLOAK_URL}/admin/realms/{realm_name}/clients",
        headers=headers,
        params={"clientId": client_id},
    )
    resp.raise_for_status()
    match = next((c for c in resp.json() if c["clientId"] == client_id), None)
    if not match:
        raise ValueError(f"Client '{client_id}' not found in realm '{realm_name}'")
    return match["id"]


def task_assign_service_account_roles(realm_name: str, client_id: str, role_cfg: dict, **ctx):
    """Assign client roles (e.g. from realm-management) to a client's service account user."""
    headers = _auth_headers()
    base = f"{KEYCLOAK_URL}/admin/realms/{realm_name}"

    client_uuid = _get_client_uuid(realm_name, client_id, headers)

    sa_user_resp = requests.get(f"{base}/clients/{client_uuid}/service-account-user", headers=headers)
    sa_user_resp.raise_for_status()
    sa_user_id = sa_user_resp.json()["id"]

    role_client_uuid = _get_client_uuid(realm_name, role_cfg["client_id"], headers)

    existing_resp = requests.get(
        f"{base}/users/{sa_user_id}/role-mappings/clients/{role_client_uuid}",
        headers=headers,
    )
    existing_resp.raise_for_status()
    existing_role_names = {r["name"] for r in existing_resp.json()}

    roles_to_assign = []
    for role_name in role_cfg["roles"]:
        if role_name in existing_role_names:
            print(f"Service account of '{client_id}' already has role '{role_name}' — skipping.")
            continue
        role_resp = requests.get(
            f"{base}/clients/{role_client_uuid}/roles/{role_name}",
            headers=headers,
        )
        role_resp.raise_for_status()
        roles_to_assign.append(role_resp.json())

    if not roles_to_assign:
        return

    resp = requests.post(
        f"{base}/users/{sa_user_id}/role-mappings/clients/{role_client_uuid}",
        headers=headers,
        json=roles_to_assign,
    )
    resp.raise_for_status()
    assigned = [r["name"] for r in roles_to_assign]
    print(f"Assigned roles {assigned} from '{role_cfg['client_id']}' to service account of '{client_id}'.")


def task_create_group(realm_name: str, group_name: str, **ctx):
    headers = _auth_headers()
    base = f"{KEYCLOAK_URL}/admin/realms/{realm_name}/groups"

    existing = requests.get(base, headers=headers)
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


def task_assign_user_to_group(realm_name: str, username: str, group_name: str, **ctx):
    headers = _auth_headers()
    base = f"{KEYCLOAK_URL}/admin/realms/{realm_name}"

    users_resp = requests.get(f"{base}/users",
                              headers=headers,
                              params={"username": username, "exact": "true"})
    users_resp.raise_for_status()
    user = next((u for u in users_resp.json() if u["username"] == username), None)
    if not user:
        raise ValueError(f"User '{username}' not found in realm '{realm_name}'")
    user_id = user["id"]

    groups_resp = requests.get(f"{base}/groups", headers=headers)
    groups_resp.raise_for_status()
    group = next((g for g in groups_resp.json() if g["name"] == group_name), None)
    if not group:
        raise ValueError(f"Group '{group_name}' not found in realm '{realm_name}'")
    group_id = group["id"]

    member_resp = requests.get(f"{base}/users/{user_id}/groups", headers=headers)
    member_resp.raise_for_status()
    if any(g["id"] == group_id for g in member_resp.json()):
        print(f"User '{username}' already in group '{group_name}' — skipping.")
        return

    resp = requests.put(f"{base}/users/{user_id}/groups/{group_id}", headers=headers)
    resp.raise_for_status()
    print(f"User '{username}' assigned to group '{group_name}' in realm '{realm_name}'.")


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
        realm_groups         = realm_cfg.get("groups", [])
        realm_user_groups    = realm_cfg.get("user_groups", [])
        realm_client         = realm_cfg["client"]
        realm_client_mappers = realm_cfg.get("client_mappers", [])
        realm_sa_clients     = realm_cfg.get("service_account_clients", [])

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

        group_task_map = {}
        group_tasks = []
        for g in realm_groups:
            t = PythonOperator(
                task_id=f"create_group__{realm_name}__{g}",
                python_callable=task_create_group,
                op_kwargs={"realm_name": realm_name, "group_name": g},
            )
            group_tasks.append(t)
            group_task_map[g] = t

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

        sa_client_tasks = []
        for sa_cfg in realm_sa_clients:
            sa_role_cfg = sa_cfg["service_account_roles"]
            sa_client_cfg = {k: v for k, v in sa_cfg.items() if k != "service_account_roles"}

            create_sa_client = PythonOperator(
                task_id=f"create_client__{realm_name}__{sa_cfg['clientId']}",
                python_callable=task_create_client,
                op_kwargs={"realm_name": realm_name, "client_cfg": sa_client_cfg},
            )
            assign_sa_roles = PythonOperator(
                task_id=f"assign_sa_roles__{realm_name}__{sa_cfg['clientId']}",
                python_callable=task_assign_service_account_roles,
                op_kwargs={
                    "realm_name": realm_name,
                    "client_id":  sa_cfg["clientId"],
                    "role_cfg":   sa_role_cfg,
                },
            )
            create_sa_client >> assign_sa_roles
            sa_client_tasks.append(create_sa_client)

        assign_tasks = []
        for ug in realm_user_groups:
            t = PythonOperator(
                task_id=f"assign_group__{realm_name}__{ug['username']}__{ug['group']}",
                python_callable=task_assign_user_to_group,
                op_kwargs={
                    "realm_name": realm_name,
                    "username":   ug["username"],
                    "group_name": ug["group"],
                },
            )
            user_task_map[ug["username"]] >> t
            group_task_map[ug["group"]]   >> t
            assign_tasks.append(t)

        obtain_token >> create_realm >> user_tasks
        obtain_token >> create_realm >> group_tasks
        obtain_token >> create_realm >> create_client
        create_client >> mapper_tasks
        obtain_token >> create_realm >> sa_client_tasks
