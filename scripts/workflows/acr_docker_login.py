from __future__ import annotations

import argparse
import json
import subprocess
import sys
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlparse
from urllib.request import Request, urlopen


AZURE_RESOURCE_MANAGER_SCOPE = "https://management.azure.com/.default"
AZURE_RESOURCE_MANAGER_RESOURCE = "https://management.azure.com/"
ACR_REFRESH_TOKEN_USERNAME = "00000000-0000-0000-0000-000000000000"


def resolve_login_server(*, acr_name: str, login_server: str) -> str:
    server = login_server.strip()
    if not server:
        name = acr_name.strip()
        if not name:
            raise ValueError("ACR_NAME or ACR_LOGIN_SERVER must be provided")
        server = name if "." in name else f"{name}.azurecr.io"

    parsed = urlparse(server)
    if parsed.scheme:
        if parsed.scheme != "https":
            raise ValueError("ACR_LOGIN_SERVER must use https when a URL scheme is provided")
        if parsed.path not in ("", "/") or parsed.params or parsed.query or parsed.fragment:
            raise ValueError("ACR_LOGIN_SERVER must be a registry host, not a path URL")
        server = parsed.netloc

    server = server.strip().rstrip("/")
    if not server or "/" in server or "\\" in server:
        raise ValueError("ACR login server must be a single registry host")
    if "." not in server:
        raise ValueError("ACR login server must be a fully qualified host")
    return server.lower()


def get_aad_access_token() -> str:
    commands = (
        [
            "az",
            "account",
            "get-access-token",
            "--scope",
            AZURE_RESOURCE_MANAGER_SCOPE,
            "--query",
            "accessToken",
            "-o",
            "tsv",
        ],
        [
            "az",
            "account",
            "get-access-token",
            "--resource",
            AZURE_RESOURCE_MANAGER_RESOURCE,
            "--query",
            "accessToken",
            "-o",
            "tsv",
        ],
    )
    last_error: subprocess.CalledProcessError | None = None
    for command in commands:
        try:
            completed = subprocess.run(command, check=True, capture_output=True, text=True)
        except subprocess.CalledProcessError as exc:
            last_error = exc
            continue
        token = completed.stdout.strip()
        if token:
            return token

    if last_error is not None:
        stderr = (last_error.stderr or "").strip()
        raise RuntimeError(f"Azure CLI could not mint an ACR AAD token: {stderr}") from last_error
    raise RuntimeError("Azure CLI returned an empty ACR AAD token")


def exchange_acr_refresh_token(*, login_server: str, tenant_id: str, aad_access_token: str) -> str:
    body = urlencode(
        {
            "grant_type": "access_token",
            "service": login_server,
            "tenant": tenant_id,
            "access_token": aad_access_token,
        }
    ).encode("utf-8")
    request = Request(
        f"https://{login_server}/oauth2/exchange",
        data=body,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    try:
        with urlopen(request, timeout=30) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"ACR token exchange failed with HTTP {exc.code}: {detail}") from exc
    except URLError as exc:
        raise RuntimeError(f"ACR token exchange failed: {exc.reason}") from exc

    refresh_token = str(payload.get("refresh_token") or "").strip()
    if not refresh_token:
        raise RuntimeError("ACR token exchange did not return a refresh token")
    return refresh_token


def docker_login(*, login_server: str, acr_refresh_token: str) -> None:
    subprocess.run(
        [
            "docker",
            "login",
            login_server,
            "--username",
            ACR_REFRESH_TOKEN_USERNAME,
            "--password-stdin",
        ],
        input=acr_refresh_token,
        text=True,
        check=True,
    )


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Log Docker into Azure Container Registry using the active Azure CLI OIDC session."
    )
    parser.add_argument("--acr-name", default="", help="Azure Container Registry resource name.")
    parser.add_argument("--login-server", default="", help="ACR login server host. Defaults to <acr-name>.azurecr.io.")
    parser.add_argument("--tenant-id", required=True, help="Microsoft Entra tenant id for the active OIDC session.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    try:
        login_server = resolve_login_server(acr_name=args.acr_name, login_server=args.login_server)
        aad_token = get_aad_access_token()
        acr_refresh_token = exchange_acr_refresh_token(
            login_server=login_server,
            tenant_id=args.tenant_id,
            aad_access_token=aad_token,
        )
        docker_login(login_server=login_server, acr_refresh_token=acr_refresh_token)
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        return 1

    print(f"Docker login completed for {login_server}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
