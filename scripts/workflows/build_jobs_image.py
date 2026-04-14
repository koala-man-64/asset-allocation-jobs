from __future__ import annotations

import argparse
import os
from pathlib import Path
import subprocess


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build the jobs Docker image, optionally push it, and emit outputs.")
    parser.add_argument("--dockerfile", required=True, help="Dockerfile path to build from.")
    parser.add_argument("--image-ref", required=True, help="Full container image reference.")
    parser.add_argument("--contracts-version", required=True, help="Pinned contracts package version.")
    parser.add_argument("--runtime-common-version", required=True, help="Pinned runtime-common package version.")
    parser.add_argument("--context", default=".", help="Docker build context.")
    parser.add_argument("--pip-config-path", help="Optional pip config file mounted as a BuildKit secret.")
    parser.add_argument("--push", action="store_true", help="Push the image after building.")
    parser.add_argument("--github-output", help="Optional GitHub output file path.")
    return parser.parse_args()


def build_command(
    *,
    dockerfile: str,
    image_ref: str,
    contracts_version: str,
    runtime_common_version: str,
    context: str,
    pip_config_path: str | None,
) -> list[str]:
    command = [
        "docker",
        "build",
        "--file",
        dockerfile,
        "--tag",
        image_ref,
        "--build-arg",
        f"CONTRACTS_VERSION={contracts_version}",
        "--build-arg",
        f"RUNTIME_COMMON_VERSION={runtime_common_version}",
    ]
    if pip_config_path:
        command.extend(["--secret", f"id=pipconfig,src={pip_config_path}"])
    command.append(context)
    return command


def inspect_image_digest(image_ref: str) -> str:
    return subprocess.check_output(
        ["docker", "inspect", "--format={{index .RepoDigests 0}}", image_ref],
        text=True,
    ).strip()


def emit_outputs(outputs: dict[str, str], github_output: str | None) -> None:
    if github_output:
        output_path = Path(github_output)
        with output_path.open("a", encoding="utf-8") as handle:
            for key, value in outputs.items():
                handle.write(f"{key}={value}\n")
        return

    for key, value in outputs.items():
        print(f"{key}={value}")


def build_jobs_image(
    *,
    dockerfile: str,
    image_ref: str,
    contracts_version: str,
    runtime_common_version: str,
    context: str,
    pip_config_path: str | None,
    push: bool,
    github_output: str | None,
) -> dict[str, str]:
    command = build_command(
        dockerfile=dockerfile,
        image_ref=image_ref,
        contracts_version=contracts_version,
        runtime_common_version=runtime_common_version,
        context=context,
        pip_config_path=pip_config_path,
    )
    env = dict(os.environ)
    env["DOCKER_BUILDKIT"] = "1"
    subprocess.run(command, check=True, env=env)

    outputs = {"image_ref": image_ref}
    if push:
        subprocess.run(["docker", "push", image_ref], check=True)
        outputs["image_digest"] = inspect_image_digest(image_ref)

    emit_outputs(outputs, github_output)
    return outputs


def main() -> None:
    args = parse_args()
    build_jobs_image(
        dockerfile=args.dockerfile,
        image_ref=args.image_ref,
        contracts_version=args.contracts_version,
        runtime_common_version=args.runtime_common_version,
        context=args.context,
        pip_config_path=args.pip_config_path,
        push=args.push,
        github_output=args.github_output,
    )


if __name__ == "__main__":
    main()
