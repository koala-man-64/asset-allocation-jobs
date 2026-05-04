from __future__ import annotations

import argparse
from pathlib import Path
import re
import subprocess


PINNED_IMAGE_PATTERN = re.compile(
    r"(?P<image>(?:[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?/)?[a-z0-9._/-]+:[A-Za-z0-9._-]+)"
    r"@(?P<digest>sha256:[0-9a-f]{64})"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate pinned Docker image digests against current tag digests.")
    parser.add_argument(
        "paths",
        nargs="*",
        default=["Dockerfile", ".github/workflows"],
        help="Files or directories to scan.",
    )
    return parser.parse_args()


def iter_files(paths: list[str]) -> list[Path]:
    files: list[Path] = []
    for raw_path in paths:
        path = Path(raw_path)
        if path.is_dir():
            files.extend(sorted(p for p in path.rglob("*") if p.is_file() and p.suffix in {".yml", ".yaml"}))
        elif path.is_file():
            files.append(path)
    return files


def pinned_images(paths: list[str]) -> dict[str, set[str]]:
    found: dict[str, set[str]] = {}
    for path in iter_files(paths):
        text = path.read_text(encoding="utf-8")
        for match in PINNED_IMAGE_PATTERN.finditer(text):
            found.setdefault(match.group("image"), set()).add(match.group("digest"))
    return found


def current_digest(image: str) -> str:
    output = subprocess.check_output(["docker", "buildx", "imagetools", "inspect", image], text=True)
    for line in output.splitlines():
        stripped = line.strip()
        if stripped.startswith("Digest:"):
            return stripped.split(":", 1)[1].strip()
    raise SystemExit(f"Could not resolve current digest for {image}")


def validate_pins(paths: list[str]) -> None:
    failures: list[str] = []
    for image, digests in sorted(pinned_images(paths).items()):
        if len(digests) != 1:
            failures.append(f"{image} has inconsistent pinned digests: {', '.join(sorted(digests))}")
            continue
        pinned = next(iter(digests))
        current = current_digest(image)
        if pinned != current:
            failures.append(f"{image} pinned {pinned}, current tag digest is {current}")

    if failures:
        raise SystemExit("Pinned image digest validation failed:\n" + "\n".join(failures))


def main() -> None:
    args = parse_args()
    validate_pins(args.paths)


if __name__ == "__main__":
    main()
