# async-pipeline

Biblioteca leve para compor **pipelines assíncronos**: cada `Stage` recebe a saída do estágio anterior, em ordem sequencial.

## Requisitos

- Python 3.14 ou superior

## Instalação com uv

No seu projeto:

```bash
uv add async-pipeline
```

Para desenvolver esta biblioteca:

```bash
git clone <repo-url>
cd async-pipeline
uv sync
```

## Uso básico

```python
from async_pipeline import Pipeline, Stage

async def add_one(value: int) -> int:
    return value + 1

async def multiply_by_two(value: int) -> int:
    return value * 2

pipeline = Pipeline([
    Stage("add_one", add_one),
    Stage("multiply_by_two", multiply_by_two),
])

result = await pipeline.run(10)
assert result == 22
```

Handlers **síncronos** também são aceitos (o método `run` do stage continua sendo `async`):

```python
def add_one(value: int) -> int:
    return value + 1

pipeline = Pipeline([
    Stage("add_one", add_one),
])

result = await pipeline.run(1)
assert result == 2
```

## Erros

Falhas dentro do handler são expostas como `StageExecutionError`, com o nome do stage e a exceção original:

```python
from async_pipeline import Pipeline, Stage, StageExecutionError

async def broken(value: int) -> int:
    raise RuntimeError("boom")

pipeline = Pipeline([
    Stage("broken", broken),
])

try:
    await pipeline.run(1)
except StageExecutionError as exc:
    assert exc.stage_name == "broken"
    assert isinstance(exc.original_error, RuntimeError)
```

Um `Pipeline` sem stages levanta `ValueError` na construção.

## Comandos (desenvolvimento)

```bash
uv sync
uv run pytest
uv run ruff check .
uv run mypy src
```

## Roadmap

- **Retry** — políticas de retentativa por stage ou pipeline
- **Timeout** — limitar tempo de execução por stage
- **Hooks** — antes/depois de cada stage ou do pipeline inteiro
- **Map com concorrência** — estágio que processa coleções com paralelismo controlado

## Licença

Veja o arquivo `LICENSE`.
