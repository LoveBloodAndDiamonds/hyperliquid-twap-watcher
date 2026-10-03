"""Проверяет, что версия в pyproject.toml больше последней опубликованной на PyPI.

Запускается в CI на pull request в main: публикация срабатывает на мерж, и если
версию забыли поднять, PyPI отклонит загрузку уже после мержа.
"""

import json
import sys
import tomllib
import urllib.error
import urllib.request
from pathlib import Path


def _parse(version: str) -> tuple[int, ...]:
    """Превращает версию вида `1.2.3` в кортеж для сравнения."""
    return tuple(int(part) for part in version.split("."))


def main() -> int:
    """Сравнивает локальную версию с PyPI и возвращает код выхода для CI."""
    project = tomllib.loads(Path("pyproject.toml").read_text())["project"]
    name, local = project["name"], project["version"]

    try:
        with urllib.request.urlopen(f"https://pypi.org/pypi/{name}/json", timeout=10) as response:
            published = json.load(response)["info"]["version"]
    except urllib.error.HTTPError as exc:
        # 404 — пакет еще ни разу не публиковался, любая версия подходит.
        if exc.code == 404:
            print(f"{name} is not on PyPI yet, version {local} is fine")
            return 0
        raise

    if _parse(local) <= _parse(published):
        print(f"Version {local} must be greater than published {published}. Bump pyproject.toml")
        return 1

    print(f"Version {local} > published {published}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
