import contextlib
import inspect
import logging
import shutil
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from nebari.hookspecs import NebariStage
from nebari.schema import Base, Main

from nebari_helm_stage import helm
from nebari_helm_stage.utils import populate_contents, update_yaml

logger = logging.getLogger(__name__)


class InputSchema(Base):
    # for the top-level chart/values.yaml
    stage_values_overrides: Optional[Dict[str, Any]] = {}
    extra_chart_dependencies: Optional[List[helm.Chart]] = []


class OutputSchema(Base):
    pass


class NebariHelmStage(NebariStage):
    name = "helm_extension"
    priority = 100
    version: str = "0.1.0"  # TODO: use package version here

    input_schema = InputSchema
    output_schema = OutputSchema

    base_dependency_charts: List[helm.Chart] = []

    @property
    def stage_config(self) -> Union[None, Main]:
        return getattr(self.config, self.name)

    @property
    def stage_chart(self) -> helm.Chart:
        return helm.Chart(
            name=self.name,
            repo=str(self.stage_chart_directory),
            version=self.version,
            overrides={},
        )

    @property
    def dependency_charts(self) -> List[helm.Chart]:
        charts = []
        if self.stage_config is not None:
            for chart in self.stage_config.extra_chart_dependencies:
                charts.append(helm.Chart(**chart))
        return charts + self.base_dependency_charts

    @property
    def template_directory(self) -> Path:
        return Path(inspect.getfile(self.__class__)).parent / "stage_chart"

    @property
    def stage_chart_directory(self) -> Path:
        return self.output_directory / self.stage_prefix

    def render(self) -> Dict[str, str]:
        # TODO:
        # confirm kube context is set correctly

        contents = {}

        # create a persistent temporary directory to store all rendered files
        helm_tmp_dir = (
            helm.install_helm_binary().parent
            / f"{self.config.project_name}-{self.config.namespace}"
            / self.name
        )

        # copy chart template to temporary directory
        shutil.copytree(self.template_directory, helm_tmp_dir, dirs_exist_ok=True)
        if self.stage_config is not None:
            update_yaml(
                self.stage_config.stage_values_overrides, helm_tmp_dir / "values.yaml"
            )
        stage_chart = helm.ChartYAML(
            name=self.stage_chart.name,
            version=self.stage_chart.version,
            appVersion=self.stage_chart.version,
        ).dict()
        update_yaml(stage_chart, helm_tmp_dir / "Chart.yaml")

        # helm pull all dependencies to temporary directory
        charts_tmp_dir = helm_tmp_dir / "charts"
        deps = {"dependencies": []}
        for chart in self.dependency_charts:
            # TODO: find a better way to check if chart is already downloaded
            if not Path(charts_tmp_dir / chart.name / "values.yaml").is_file():
                helm.helm_pull(
                    repo=chart.repo,
                    chart=chart.name,
                    version=chart.version,
                    output_dir=charts_tmp_dir,
                    namespace=self.config.namespace,
                )
            update_yaml(chart.overrides, charts_tmp_dir / chart.name / "values.yaml")

            dep = helm.map_chart_to_dependecy(chart)
            # point to local chart directory
            dep.repository = "file://" + str(
                self.stage_chart_directory / "charts" / chart.name
            )
            deps["dependencies"].append(dep.dict())

        update_yaml(deps, helm_tmp_dir / "Chart.yaml")
        contents.update(
            populate_contents(helm_tmp_dir, self.output_directory / self.stage_prefix)
        )

        return contents

    @contextlib.contextmanager
    def deploy(self, stage_outputs: Dict[str, Dict[str, Any]]):
        # TODO: remove stage_outputs, update output_schema with appropriate values

        if helm.is_chart_deployed(
            release_name=self.name, namespace=self.config.namespace
        ):
            helm.helm_upgrade(
                chart_location=self.stage_chart_directory,
                release_name=self.name,
                namespace=self.config.namespace,
            )
        else:
            helm.helm_install(
                chart_location=self.stage_chart_directory,
                release_name=self.name,
                namespace=self.config.namespace,
            )
        yield

    @contextlib.contextmanager
    def destroy(self, stage_outputs: Dict[str, Dict[str, Any]]):
        # TODO: remove stage_outputs, update output_schema with appropriate values

        helm.helm_uninstall(release_name=self.name, namespace=self.config.namespace)
        yield

    def check(self, stage_outputs: Dict[str, Dict[str, Any]]):
        pass
