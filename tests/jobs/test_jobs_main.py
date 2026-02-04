import runpy
from pathlib import Path
import types

import apps.jobs.main as jobs_main


def test_main_runs_router(monkeypatch):
    class StubRouter:
        created = 0
        last = None

        def __init__(self):
            StubRouter.created += 1
            StubRouter.last = self
            self.ran = False

        def run(self):
            self.ran = True

    monkeypatch.setattr(jobs_main, "Router", StubRouter)

    jobs_main.main()

    assert StubRouter.created == 1
    assert StubRouter.last.ran is True


def test_module_entrypoint_runs_main(monkeypatch):
    class StubRouter:
        created = 0
        last = None

        def __init__(self):
            StubRouter.created += 1
            StubRouter.last = self
            self.ran = False

        def run(self):
            self.ran = True

    module = types.SimpleNamespace(Router=StubRouter)
    monkeypatch.setitem(__import__("sys").modules, "apps.jobs.router", module)

    module_path = Path(jobs_main.__file__)
    runpy.run_path(str(module_path), run_name="__main__")

    assert StubRouter.created == 1
    assert StubRouter.last.ran is True
