import json
import logging
import os
import platform
import tarfile
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
from urllib.parse import urljoin, urlparse

import requests
from nebari.schema import Base

from nebari_helm_stage.utils import run_subprocess_cmd

logger = logging.getLogger(__name__)

DEFAULT_HELM_VERSION = "v3.12.1"


class HelmException(Exception):
    pass


class Chart(Base):
    name: Optional[Union[str, None]] = None
    repo: Optional[Union[str, Path, None]] = None
    url: Optional[Union[str, None]] = None
    version: Optional[Union[str, None]] = None
    overrides: Dict[str, Any] = {}


class Dependency(Base):
    name: str
    version: str
    repository: str


class ChartYAML(Base):
    apiVersion: str = "v2"
    name: str
    version: str
    appVersion: str
    dependencies: Optional[Union[List[Dependency], None]] = None


def map_chart_to_dependecy(chart: Chart) -> Dependency:
    # TODO: determine better way of accurately setting `repository`
    repository = chart.repo
    if isinstance(chart.repo, Path):
        repository = "file://" + str(chart.repo.absolute())
    elif urlparse(chart.url).scheme:
        repository = chart.url

    return Dependency(
        name=chart.name,
        version=chart.version,
        repository=repository,
    )


def install_helm_binary(version: str = DEFAULT_HELM_VERSION) -> Path:
    base = "https://get.helm.sh"
    helm = "helm"
    helm_path = f"{platform.system().lower()}-{platform.machine()}"
    download_url = urljoin(base, f"helm-{version}-{helm_path}.tar.gz")

    helm_dir = Path(tempfile.gettempdir()) / helm / version
    helm_dir.mkdir(parents=True, exist_ok=True)
    final_path = helm_dir / helm

    if not final_path.is_file():
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_dir = Path(tmp_dir)
            file_path = tmp_dir / f"{helm}.tar.gz"

            with requests.get(download_url, stream=True) as r:
                r.raise_for_status()
                with open(file_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)

            with tarfile.open(file_path, "r:gz") as tar:
                tar.extractall(path=tmp_dir)

            # move the helm binary to final_path
            binary_path = tmp_dir / helm_path / helm
            binary_path.rename(final_path)

    os.chmod(final_path, 0o555)
    return final_path


def run_helm_subprocess(
    processargs, suppress_output=False, **kwargs
) -> tuple[int, str]:
    helm_path = install_helm_binary()
    logger.info(f" helm at {helm_path}")
    return run_subprocess_cmd(
        [helm_path] + processargs, suppress_output=suppress_output, **kwargs
    )


def helm_repo_add(name: str, url: str, namespace: str = "default"):
    run_helm_subprocess(["repo", "add", name, url, "--namespace", namespace])


def helm_update(namespace: str = "default"):
    run_helm_subprocess(["repo", "update", "--namespace", namespace])


def helm_pull(
    repo: str,
    chart: str,
    version: str,
    output_dir: str | Path,
    namespace: str = "default",
) -> Dict[str, str]:
    if isinstance(output_dir, str):
        output_dir = Path(output_dir)

    run_helm_subprocess(
        [
            "pull",
            f"{repo}/{chart}",
            "--version",
            version,
            "--untar",
            "--untardir",
            output_dir,
            "--namespace",
            namespace,
        ]
    )

    return output_dir


def helm_list(namespace: str = "default") -> dict[str, Any]:
    exit_code, helm_releases = run_helm_subprocess(
        ["list", "-o", "json", "--namespace", namespace]
    )
    if exit_code != 0:
        logger.warn(f"No Helm release were found.")
        return {}
    return json.loads(helm_releases)


def helm_status(release_name: str, namespace: str = "default") -> dict[str, Any]:
    exit_code, status = run_helm_subprocess(
        ["status", release_name, "-o", "json", "--namespace", namespace],
        suppress_output=True,
    )
    if exit_code != 0:
        logger.warn(f"Helm release `{release_name}` not found.")
        return {}
    return json.loads(status)


def helm_install(
    chart_location: str | Path, release_name: str, namespace: str = "default"
):
    if isinstance(chart_location, str):
        chart_location = Path(chart_location)
    run_helm_subprocess(
        ["install", release_name, chart_location, "--namespace", namespace],
        suppress_output=True,
    )


def helm_uninstall(release_name: str, namespace: str = "default"):
    run_helm_subprocess(
        ["uninstall", release_name, "--namespace", namespace], suppress_output=True
    )


def helm_upgrade(
    chart_location: str | Path, release_name: str, namespace: str = "default"
):
    if isinstance(chart_location, str):
        chart_location = Path(chart_location)
    run_helm_subprocess(
        ["upgrade", release_name, chart_location, "--namespace", namespace],
        suppress_output=True,
    )


def is_chart_deployed(release_name: str, namespace: str = "default") -> bool:
    status = helm_status(release_name, namespace=namespace)
    if status:
        return True
    return False
