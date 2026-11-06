"""Every workflow declares a top-level `permissions:` so a job never inherits the org default token."""

from pathlib import Path

import yaml

WORKFLOWS = Path(__file__).resolve().parents[3] / '.github' / 'workflows'


def test_every_workflow_declares_top_level_permissions():
    workflows = sorted(WORKFLOWS.glob('*.y*ml'))
    missing = [path.name for path in workflows if 'permissions' not in yaml.safe_load(path.read_text())]

    assert workflows
    assert missing == []
