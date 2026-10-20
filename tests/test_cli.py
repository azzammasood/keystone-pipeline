from typer.testing import CliRunner

from keystone import __version__
from keystone.cli import app

runner = CliRunner()


def test_version() -> None:
    result = runner.invoke(app, ["--version"])
    assert result.exit_code == 0
    assert __version__ in result.output


def test_info_shows_polite_user_agent() -> None:
    result = runner.invoke(app, ["info"])
    assert result.exit_code == 0
    ua = "keystone-pipeline/0.1 (+https://github.com/azzammasood/keystone-pipeline)"
    assert ua in result.output


def test_machine_json() -> None:
    result = runner.invoke(app, ["machine", "--json"])
    assert result.exit_code == 0
    assert '"disk_budget_gb"' in result.output
