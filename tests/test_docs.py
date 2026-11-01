"""Every Python snippet in the README and every script in examples/ actually runs."""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import grpc
import pytest
from google.protobuf.wrappers_pb2 import StringValue

from grpc_sync_stream import EOF, BidiStream
from tests.conftest import Stubs

ROOT = Path(__file__).resolve().parent.parent
SNIPPETS = re.findall(r"```python\n(.*?)```", (ROOT / "README.md").read_text(), flags=re.DOTALL)
EXAMPLES = sorted(p for p in (ROOT / "examples").glob("*.py") if p.name != "echo_server.py")


def test_readme_has_snippets() -> None:
    assert len(SNIPPETS) >= 4


@pytest.mark.parametrize("snippet", SNIPPETS, ids=[f"snippet{i}" for i in range(len(SNIPPETS))])
def test_readme_snippet(snippet: str, stubs: Stubs) -> None:
    stub = SimpleNamespace(Chat=stubs.chat, Expand=stubs.expand, Join=stubs.join)
    namespace = {"grpc": grpc, "stub": stub, "Msg": StringValue, "EOF": EOF, "BidiStream": BidiStream}
    exec(compile(snippet, "README.md", "exec"), namespace)


@pytest.mark.parametrize("example", EXAMPLES, ids=[p.stem for p in EXAMPLES])
def test_example_runs(example: Path) -> None:
    result = subprocess.run(
        [sys.executable, str(example)], capture_output=True, text=True, timeout=60, check=False
    )
    assert result.returncode == 0, result.stderr
    assert "status: StatusCode.OK" in result.stdout
