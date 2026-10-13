"""Block commits that would publish infrastructure identifiers, secrets or real data.

    python tools/check_sensitive.py <files...>     (pre-commit passes the staged files)

gitleaks covers generic secrets. This adds: S3 bucket URLs carrying a UUID, AWS account ids in
ARNs, real-looking e-mail addresses, anything under data/ (private samples), and the private
patterns listed in the git-ignored file `.sensitive-patterns` (one regex per line, e.g. your real
bucket name), so that the repository does not even contain a hint of them.
"""

from __future__ import annotations

import gzip
import re
import sys
from pathlib import Path

FORBIDDEN = {
    "s3 bucket with a uuid in its name": re.compile(r"s3://[a-z0-9.-]*[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"),
    "bucket-like name with a uuid": re.compile(r"\b[a-z0-9-]{3,}-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b"),
    "aws access key id": re.compile(r"\bAKIA[0-9A-Z]{16}\b"),
    "aws account id in an ARN": re.compile(r"arn:aws:[a-z0-9-]+:[a-z0-9-]*:\d{12}:"),
    "private key": re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----"),
    "databricks token": re.compile(r"\bdapi[0-9a-f]{32}\b"),
}
EMAIL = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
ALLOWED_EMAIL_SUFFIXES = ("example.invalid", "example.com", "@anthropic.com")
SELF = {"tools/check_sensitive.py", "tests/test_privacy.py"}


def _private_patterns() -> dict[str, re.Pattern[str]]:
    local = Path(__file__).resolve().parents[1] / ".sensitive-patterns"
    if not local.exists():
        return {}
    lines = [ln.strip() for ln in local.read_text().splitlines() if ln.strip() and not ln.startswith("#")]
    return {f"private pattern #{i + 1}": re.compile(ln) for i, ln in enumerate(lines)}


FORBIDDEN.update(_private_patterns())


def read_text(path: Path) -> str:
    raw = path.read_bytes()
    if path.suffix == ".gz":
        raw = gzip.decompress(raw)
    return raw.decode("utf-8", errors="ignore")


def problems(path: Path, rel: str) -> list[str]:
    if rel.startswith("data/") or rel.endswith(".part"):
        return [f"{rel}: private data directory / sample file"]
    if rel in SELF or path.suffix in {".png", ".jpg", ".ico", ".whl"}:
        return []
    text = read_text(path)
    found = [f"{rel}: {label}" for label, rx in FORBIDDEN.items() if rx.search(text)]
    found += [f"{rel}: e-mail address {e}" for e in EMAIL.findall(text)
              if not e.lower().endswith(ALLOWED_EMAIL_SUFFIXES) and not e.startswith("noreply@")]
    return found


def main(argv: list[str]) -> int:
    root = Path.cwd()
    issues = [p for name in argv if (path := Path(name)).is_file()
              for p in problems(path, str(path.resolve().relative_to(root)) if path.is_absolute() else name)]
    for issue in issues:
        print(issue)
    return 1 if issues else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
