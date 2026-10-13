"""Alerts that don't fail the job (schema drift under the additive policy, DQ warnings, cost).

Job failures are already alerted by Databricks job notifications (see resources/). This covers
the "succeeded, but look at this" cases. The webhook URL (Slack, Teams, ntfy, ...) comes from an
environment variable or a Databricks secret, never from the code.
"""

from __future__ import annotations

import json
import logging
import os
import urllib.request

log = logging.getLogger(__name__)


def _webhook_url() -> str | None:
    url = os.environ.get("REGISTRY_ALERT_WEBHOOK")
    if url:
        return url
    scope = os.environ.get("REGISTRY_SECRET_SCOPE")
    if scope:
        try:
            from databricks.sdk.runtime import dbutils  # type: ignore[import-not-found]

            return dbutils.secrets.get(scope=scope, key="alert-webhook")
        except Exception:  # secret missing: alerts are best-effort, never fail the pipeline
            log.warning("alert webhook secret not readable in scope %s", scope)
    return None


def notify(level: str, title: str, details: str = "") -> bool:
    text = f"[registry-lakehouse][{level}] {title}\n{details}".strip()
    log.log(logging.WARNING if level != "INFO" else logging.INFO, text)
    url = _webhook_url()
    if not url:
        return False
    if not url.startswith("https://"):
        log.warning("refusing non-https webhook")
        return False
    req = urllib.request.Request(url, data=json.dumps({"text": text}).encode(),
                                 headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=10):  # https enforced above
            return True
    except Exception as exc:
        log.warning("alert delivery failed: %s", exc)
        return False
