.PHONY: install lint format typecheck test check build clean clean-macos-trash-stuff

# Установить зависимости (включая dev-группу) в .venv
install:
	uv sync

# Линтер без автоисправления — так же, как в CI
lint:
	uv run ruff check --no-fix .

# Автоисправление и форматирование
format:
	uv run ruff check --fix .
	uv run ruff format .

# Проверка типов
typecheck:
	uv run basedpyright hl_twap_watcher

# Тесты
test:
	uv run pytest

# Все проверки перед пушем: то же, что гоняет CI
check: lint typecheck test

# Собрать wheel и sdist в dist/
build: clean
	uv build

# Удалить артефакты сборки
clean:
	rm -rf dist build *.egg-info

clean-macos-trash-stuff:
	find . -name ".DS_Store" -type f -delete
