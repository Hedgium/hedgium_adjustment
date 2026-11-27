"""Overnight broker-token expiry and the 07:30 market auto-login."""

from __future__ import annotations

import main
from stream.token_fetcher import (
    classify_kite_token_response,
    decide_credential_action,
    should_probe_broker_token,
)
from stream.ws_stream import is_kite_auth_failure


def test_classify_ok_rejected_and_unknown():
    assert classify_kite_token_response(200, {"status": "success"}) == "ok"
    assert (
        classify_kite_token_response(
            403,
            {
                "error_type": "TokenException",
                "message": "Incorrect `api_key` or `access_token`.",
            },
        )
        == "rejected"
    )
    assert classify_kite_token_response(500, {"message": "unavailable"}) == "unknown"


def test_decide_waits_for_auto_login_then_accepts_new_token():
    old = {"api_key": "k", "access_token": "old"}
    new = {"api_key": "k", "access_token": "new"}
    assert (
        decide_credential_action(old, rejected_access_token=None, token_status="rejected")
        == "wait"
    )
    # Same dead token, probe skipped: keep waiting. Do not open the socket.
    assert (
        decide_credential_action(old, rejected_access_token="old", token_status="unknown")
        == "wait"
    )
    # Transient 403 that later succeeds can proceed with the same token.
    assert (
        decide_credential_action(old, rejected_access_token="old", token_status="ok")
        == "ready"
    )
    assert (
        decide_credential_action(new, rejected_access_token="old", token_status="ok")
        == "ready"
    )
    # Kite blip on a token we have not rejected yet: keep streaming.
    assert (
        decide_credential_action(new, rejected_access_token="old", token_status="unknown")
        == "ready_unverified"
    )
    assert decide_credential_action({}, rejected_access_token=None, token_status="ok") == "wait"


def test_probe_backs_off_until_the_token_changes():
    assert (
        should_probe_broker_token(
            "old",
            rejected_access_token="old",
            last_probe_at=0.0,
            now=10.0,
            recheck_s=300,
        )
        is False
    )
    assert (
        should_probe_broker_token(
            "old",
            rejected_access_token="old",
            last_probe_at=0.0,
            now=300.0,
            recheck_s=300,
        )
        is True
    )
    # Auto-login stored a new access token: probe immediately.
    assert (
        should_probe_broker_token(
            "new",
            rejected_access_token="old",
            last_probe_at=0.0,
            now=10.0,
            recheck_s=300,
        )
        is True
    )


def test_ws_auth_failure_is_token_rejection_not_a_blip():
    class _Handshake:
        status_code = 403

    assert is_kite_auth_failure(_Handshake()) is True
    assert is_kite_auth_failure("Handshake status 403 Forbidden") is True
    assert is_kite_auth_failure("Invalid token") is True
    assert is_kite_auth_failure("timed out") is False
    assert is_kite_auth_failure("") is False


def test_adopt_swaps_in_the_token_from_auto_login(monkeypatch):
    creds = {"api_key": "key", "access_token": "overnight"}

    monkeypatch.setattr(
        main.backend_api,
        "get_credentials",
        lambda: {"api_key": "key", "api_secret": "s", "access_token": "morning"},
    )
    monkeypatch.setattr(main, "kite_token_status", lambda *_a, **_k: "ok")

    main._adopt_fresh_credentials(creds)

    assert creds["access_token"] == "morning"


def test_adopt_refuses_an_expired_token(monkeypatch):
    creds = {"api_key": "key", "access_token": "overnight"}

    monkeypatch.setattr(
        main.backend_api,
        "get_credentials",
        lambda: {"api_key": "key", "access_token": "overnight"},
    )
    monkeypatch.setattr(main, "kite_token_status", lambda *_a, **_k: "rejected")

    try:
        main._adopt_fresh_credentials(creds)
    except main.BrokerTokenUnavailable as exc:
        assert exc.access_token == "overnight"
    else:
        raise AssertionError("expected BrokerTokenUnavailable")
