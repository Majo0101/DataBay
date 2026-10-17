"""Credential source selection stays explicit in an infrastructure environment."""

import pytest

from databay.etl import Octopus


def test_missing_explicit_env_file_fails_instead_of_using_environment(tmp_path, monkeypatch):
    monkeypatch.setenv("HOST", "unrelated-host")
    monkeypatch.setenv("DATABASE", "unrelated-database")
    with pytest.raises(FileNotFoundError):
        Octopus(env_file=tmp_path / "missing.env", engine="postgresql")


@pytest.mark.parametrize("content", ["", "  \n", "# no settings\n"])
def test_empty_explicit_env_file_fails(tmp_path, content):
    path = tmp_path / "database.env"
    path.write_text(content)
    with pytest.raises(ValueError, match="empty or contains no settings"):
        Octopus(env_file=path)


def test_explicit_env_file_does_not_fall_back_for_missing_keys(tmp_path, monkeypatch):
    monkeypatch.setenv("USER", "container-user")
    monkeypatch.setenv("PASSWORD", "unrelated-password")
    monkeypatch.setenv("HOST", "unrelated-host")
    path = tmp_path / "database.env"
    path.write_text("HOST=database-host\nDATABASE=analytics\n")
    octopus = Octopus(env_file=path, engine="postgresql")
    assert octopus._jdbc_url("postgresql") == "jdbc:postgresql://database-host/analytics"
    options = octopus._jdbc_base_options("postgresql")
    assert options["user"] is None
    assert options["password"] is None


def test_without_env_file_uses_infrastructure_environment(monkeypatch):
    for key, value in {"HOST": "database-host", "PORT": "5432", "DATABASE": "analytics",
                       "USER": "db-user", "PASSWORD": "db-password"}.items():
        monkeypatch.setenv(key, value)
    octopus = Octopus(engine="postgresql")
    assert octopus._jdbc_url("postgresql") == "jdbc:postgresql://database-host:5432/analytics"
    options = octopus._jdbc_base_options("postgresql")
    assert options["user"] == "db-user"
    assert options["password"] == "db-password"


@pytest.mark.parametrize("value", [
    "${DB_PASSWORD}", "prefix-${DB_PASSWORD}", "${MISSING:-fallback}",
    '"${DB_PASSWORD}"', "'${DB_PASSWORD}'", "${HOST}",
])
def test_explicit_env_file_rejects_variable_references(tmp_path, monkeypatch, value):
    monkeypatch.setenv("DB_PASSWORD", "do-not-use-process-value")
    path = tmp_path / "database.env"
    path.write_text(f"HOST=database-host\nPASSWORD={value}\n")
    with pytest.raises(ValueError, match="must not reference other variables") as error:
        Octopus(env_file=path)
    assert "do-not-use-process-value" not in str(error.value)
    assert value not in str(error.value)


@pytest.mark.parametrize("entry", ["PASSWORD", "PASSWORD=", 'PASSWORD="   "'])
def test_explicit_env_file_requires_values_for_declared_keys(tmp_path, entry):
    path = tmp_path / "database.env"
    path.write_text(f"HOST=database-host\n{entry}\n")
    with pytest.raises(ValueError, match="nonempty literal value"):
        Octopus(env_file=path)


def test_explicit_env_file_preserves_literal_values(tmp_path, monkeypatch):
    monkeypatch.setenv("PASSWORD", "unrelated-password")
    path = tmp_path / "database.env"
    path.write_text("HOST=database-host\nDATABASE=analytics\nUSER=db-user\nPASSWORD='pa$$word!#123'\n")
    octopus = Octopus(env_file=path, engine="postgresql")
    assert octopus._jdbc_base_options("postgresql")["password"] == "pa$$word!#123"
