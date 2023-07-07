import contextlib
import logging
from pathlib import Path
from typing import Any, Dict

from nebari.hookspecs import NebariStage, hookimpl
from nebari.schema import Base, Main

from nebari_helm_stage import helm

logger = logging.getLogger(__name__)


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

    def __init__(
        self, output_directory: Path, config: Main, namespace: str = "default"
    ):
        super().__init__(output_directory, config)
        self.namespace = namespace

    @property
    def stage_prefix(self):
        return Path("stages")

    @property
    def chart_location(self):
        return (
            self.output_directory / self.stage_prefix / self.input_schema.chart_name
        ).absolute()

    def render(self) -> Dict[str, str]:
        # TODO:
        # confirm kube context is set correctly

        contents = {}

        helm.helm_repo_add(
            name=self.input_schema.chart_repo,
            url=self.input_schema.chart_url,
            namespace=self.namespace,
        )
        helm.helm_update(namespace=self.namespace)

        contents.update(
            helm.helm_pull(
                repo=self.input_schema.chart_repo,
                chart=self.input_schema.chart_name,
                version=self.input_schema.chart_version,
                overrides=self.input_schema.chart_overrides,
                output_dir=self.chart_location.parent,
                namespace=self.namespace,
            )
        )

        return contents

    @contextlib.contextmanager
    def deploy(self, stage_outputs: Dict[str, Dict[str, Any]]):
        # TODO: remove stage_outputs, update output_schema with appropriate values

        if helm.is_chart_deployed(
            release_name=self.input_schema.chart_name, namespace=self.namespace
        ):
            helm.helm_upgrade(
                chart_location=self.chart_location,
                release_name=self.input_schema.chart_name,
                namespace=self.namespace,
            )
        else:
            helm.helm_install(
                chart_location=self.chart_location,
                release_name=self.input_schema.chart_name,
                namespace=self.namespace,
            )

    @contextlib.contextmanager
    def destroy(self, stage_outputs: Dict[str, Dict[str, Any]]):
        # TODO: remove stage_outputs, update output_schema with appropriate values

        helm.helm_uninstall(
            release_name=self.input_schema.chart_name, namespace=self.namespace
        )

    def check(self, stage_outputs: Dict[str, Dict[str, Any]]):
        pass

    def input_vars(self, stage_outputs: Dict[str, Dict[str, Any]]):
        pass


@hookimpl
def nebari_stage():
    return [NebariHelmExtension]
