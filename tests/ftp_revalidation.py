"""Exercise session revalidation through a persistent FTP control connection."""

import ftplib
import io
import json
import os
import pathlib
import subprocess
import sys
import time


COMPOSE = [
    "docker", "compose", "-f",
    str(pathlib.Path(__file__).resolve().parents[1] / "compose.yaml"),
    "exec", "-T", "mocks",
]


def state():
    return json.loads(subprocess.check_output(
        COMPOSE + ["wget", "-q", "-O", "-", "http://127.0.0.1:18080/state"]
    ))


def control(**changes):
    subprocess.check_call(COMPOSE + [
        "python", "-c",
        "import sys,urllib.request;urllib.request.urlopen(urllib.request.Request('http://127.0.0.1:18080/control',data=sys.argv[1].encode(),headers={'Content-Type':'application/json'})).read()",
        json.dumps(changes),
    ])


def connect():
    ftp = ftplib.FTP()
    ftp.connect("127.0.0.1", int(os.environ.get("COLOMBO_FTP_HOST_PORT", "12121")))
    ftp.login("photographer", "secret")
    return ftp


def stor(ftp, name):
    ftp.storbinary(f"STOR {name}", io.BytesIO(b"revalidation-test"))


def main():
    # The caller sets a one-second tenant threshold so this boundary test runs
    # quickly. Null-column behavior is verified before this script runs.
    first = connect()
    baseline = state()["validation_requests"]
    stor(first, "revalidation-first.txt")
    stor(first, "revalidation-second.txt")
    assert state()["validation_requests"] == baseline

    second = connect()
    assert state()["validation_requests"] == baseline + 1
    time.sleep(1.1)
    control(validation_assignment="assignment-new")
    stor(first, "revalidation-moved.txt")
    assert state()["validation_requests"] == baseline + 2
    # The other session has its own timestamp and still needs to revalidate.
    stor(second, "revalidation-other-session.txt")
    assert state()["validation_requests"] == baseline + 3

    time.sleep(1.1)
    control(validation_mode="unavailable")
    stor(first, "revalidation-outage.txt")
    assert state()["validation_requests"] == baseline + 4
    control(validation_mode="normal")
    stor(first, "revalidation-retry.txt")
    assert state()["validation_requests"] == baseline + 5

    time.sleep(1.1)
    control(validation_mode="denied")
    try:
        stor(first, "revalidation-denied.txt")
    except ftplib.error_perm:
        pass
    else:
        raise AssertionError("denied STOR succeeded")
    denied_count = state()["validation_requests"]
    control(validation_mode="normal")
    try:
        stor(first, "revalidation-evicted.txt")
    except ftplib.error_perm:
        pass
    else:
        raise AssertionError("evicted session accepted STOR")
    assert state()["validation_requests"] == denied_count
    first.close()
    second.close()

    for _ in range(80):
        objects = state()["objects"]
        if all(
            any(f"assignment-new/{name}" in obj for obj in objects)
            for name in (
                "revalidation-moved.txt",
                "revalidation-other-session.txt",
                "revalidation-outage.txt",
                "revalidation-retry.txt",
            )
        ):
            break
        time.sleep(0.25)
    else:
        raise AssertionError("reassigned uploads were not delivered")
    assert not any("revalidation-denied.txt" in obj for obj in objects)
    assert not any("revalidation-evicted.txt" in obj for obj in objects)


if __name__ == "__main__":
    if sys.argv[1:] == ["--null"]:
        ftp = connect()
        baseline = state()["validation_requests"]
        stor(ftp, "revalidation-disabled-a.txt")
        stor(ftp, "revalidation-disabled-b.txt")
        assert state()["validation_requests"] == baseline
        ftp.close()
    else:
        main()
