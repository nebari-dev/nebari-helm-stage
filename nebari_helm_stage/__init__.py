import logging
import contextlib
from typing import Dict, Any
from pathlib import Path

from nebari.hookspecs import hookimpl, NebariStage
from nebari.schema import Base

from nebari_helm_stage import helm


logger = logging.getLogger(__name__)


# class HelmExtensions(Base):
#     helm_version: str = "3.12.1"


class InputSchema(Base):
    chart_name: str
    chart_repo: str
    chart_url: str
    chart_version: str
    chart_overrides: Dict[str, Any] = {}


class OutputSchema(Base):
    pass


class NebariHelmExtension(NebariStage):
    name = "helm_extension"
    priority = 100

    input_schema = InputSchema
    output_schema = OutputSchema

    @property
    def stage_prefix(self):
        return Path("stages")
    
    def render(self) -> Dict[str, str]:
        # TODO:
        # confirm kube context is set correctly
        
        contents = {}
        output_dir = self.output_directory / self.stage_prefix

        helm.helm_repo_add(self.input_schema.chart_repo, self.input_schema.chart_url)
        helm.helm_update()

        contents.update(
            helm.helm_pull(
                self.input_schema.chart_repo,
                self.input_schema.chart_name,
                self.input_schema.chart_version,
                self.input_schema.chart_overrides,
                output_dir,
            )
        )

        return contents

    @contextlib.contextmanager
    def deploy(self, stage_outputs: Dict[str, Dict[str, Any]]):
        pass

    @contextlib.contextmanager
    def destroy(self, stage_outputs: Dict[str, Dict[str, Any]]):
        pass

    def check(self, stage_outputs: Dict[str, Dict[str, Any]]):
        pass

    def input_vars(self, stage_outputs: Dict[str, Dict[str, Any]]):
        pass


@hookimpl
def nebari_stage():
    return [NebariHelmExtension]
