import contextlib
import inspect
import logging
import shutil
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

from nebari.hookspecs import NebariStage
from nebari.schema import Base, Main

from nebari_helm_stage import helm
from nebari_helm_stage.utils import populate_contents, update_yaml

logger = logging.getLogger(__name__)


# We use the helm install/upgrade --set-json flag to override the chart/values.yaml

# k: dot-separated path to specific key in values.yaml
# v: the `stage_outputs` {variable} as needed
SET_JSON_TEMPLATE = {
    "startup_greeting": "Hello World from {domain}!",
}

# Required values from `stage_outputs`
class RequiredValues(Base):
    domain: str


class InputSchema(Base):
    overrides: Optional[Dict[str, Any]] = {}
    extra_chart_dependencies: Optional[List[helm.Chart]] = []


class OutputSchema(Base):
    pass


class NebariHelmStage(NebariStage):
    priority = 100
    version: str = "0.1.0"  # TODO: use package version here

    input_schema = InputSchema
    output_schema = OutputSchema
    required_values = RequiredValues

    base_dependency_charts: List[helm.Chart] = []

    set_json_template = SET_JSON_TEMPLATE

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

    def get_stage_output(
        self, stage_outputs: Dict[str, Dict[str, Any]], output_name: str
    ):
        # utility function to get values from stage_outputs dict
        # for _, values, in stage_outputs.items():
        #     if output_name in values.keys():
        #         return values[output_name]
        return "https://jsawe.thusness.io"

    def generate_set_json(self, stage_outputs: Dict[str, Dict[str, Any]]):

        requirements = {}
        for req in self.required_values.__annotations__:
            value = self.get_stage_output(stage_outputs, req)
            if value is not None:
                requirements[req] = value
            else:
                raise ValueError(f"Required value `{req}` not found in stage_outputs.")

        updated_set_json = {
            k: v.format(**requirements) for k, v in self.set_json_template.items()
        }

        # apply overrides
        if self.stage_config is not None:
            updated_set_json.update(self.stage_config.overrides)

        # format set_json as string
        s = ""
        for k, v in updated_set_json.items():
            s += f'{k}="{v}" '

        return s.strip()

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
            update_yaml(self.stage_config.overrides, helm_tmp_dir / "values.yaml")
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
        # TODO: remove stage_outputs, update output_schema with appropriate value

        set_json = self.generate_set_json(stage_outputs)

        if helm.is_chart_deployed(
            release_name=self.name, namespace=self.config.namespace
        ):
            helm.helm_upgrade(
                chart_location=self.stage_chart_directory,
                release_name=self.name,
                namespace=self.config.namespace,
                set_json=set_json,
            )
        else:
            helm.helm_install(
                chart_location=self.stage_chart_directory,
                release_name=self.name,
                namespace=self.config.namespace,
                set_json=set_json,
            )
        yield

    @contextlib.contextmanager
    def destroy(self, stage_outputs: Dict[str, Dict[str, Any]]):
        # TODO: remove stage_outputs, update output_schema with appropriate values

        helm.helm_uninstall(release_name=self.name, namespace=self.config.namespace)
        yield

    def check(self, stage_outputs: Dict[str, Dict[str, Any]]):
        pass
