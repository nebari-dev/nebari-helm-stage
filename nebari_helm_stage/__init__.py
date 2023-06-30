import io
import urllib.request
import logging
import os
import platform
import sys
import contextlib
import tarfile
import pathlib
import tempfile

from nebari.hookspecs import hookimpl, NebariStage
from nebari.schema import Base


logger = logging.getLogger(__name__)


def download_helm_binary(version: str = "3.12.1"):
    os_mapping = {
        "linux": "linux",
        "win32": "windows",
        "darwin": "darwin",
        "freebsd": "freebsd",
        "openbsd": "openbsd",
        "solaris": "solaris",
    }

    architecture_mapping = {
        "x86_64": "amd64",
        "i386": "386",
        "armv7l": "arm",
        "aarch64": "arm64",
        "arm64": "arm64",
    }

    download_url = f"https://get.helm.sh/helm-v{version}-{os_mapping[sys.platform]}-{architecture_mapping[platform.machine()]}.tar.gz"
    filename_directory = pathlib.Path(tempfile.gettempdir()) / "helm" / version
    filename_directory.mkdir(exist_ok=True, parents=True)
    filename_path = filename_directory / "helm"

    if not os.path.isfile(filename_path):
        logger.info(
            f"downloading and extracting terraform binary from url={download_url} to path={filename_path}"
        )
        with urllib.request.urlopen(download_url) as f:
            bytes_io = io.BytesIO(f.read())
        download_file = tarfile.open(mode='r:gz', fileobj=bytes_io)
        print(download_file.list())
        with filename_path.open("wb") as f:
            f.write(download_file.extractfile(f"{os_mapping[sys.platform]}-{architecture_mapping[platform.machine()]}/helm").read())

    os.chmod(filename_path, 0o555)
    return filename_path


class HelmExtensions(Base):
    helm_version: str = "3.12.1"


class InputSchema(Base):
    helm_extensions: HelmExtensions()


class OutputSchema(Base):
    pass


class NebariHelmExtension(NebariStage):
    name = "helm_extension"
    priority = 100

    input_schema = InputSchema
    output_schema = OutputSchema

    @contextlib.contextmanager
    def deploy(self, state_outputs):
        helm_binary = download_helm_binary(self.config.helm_extensions.helm_version)
        print('ran helm extension')

    @contextlib.contextmanager
    def destroy(self, state_outputs):
        helm_binary = download_helm_binary(self.config.helm_extensions.helm_version)
        print('boo')


@hookimpl
def nebari_stage():
    return [NebariHelmExtension]
