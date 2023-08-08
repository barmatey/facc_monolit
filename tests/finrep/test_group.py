import pytest
import pytest_asyncio
from pathlib import Path

from ..conftest import override_get_async_session, client


def test_start():
    assert 1 == 1
