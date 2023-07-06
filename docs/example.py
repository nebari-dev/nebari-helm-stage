from pathlib import Path

from pydantic import BaseModel

from nebari_helm_stage import InputSchema, NebariHelmExtension

url = "https://charts.heartex.com/"
repo = "heartex"
chart = "label-studio"
version = "1.1.4"

schema = InputSchema(
    chart_name=chart,
    chart_repo=repo,
    chart_url=url,
    chart_version=version,
    chart_overrides={
        "ci": True,
    },
)


class LabelStudioHelmStage(NebariHelmExtension):
    input_schema = schema
    priority = 100
    name = "label-studio"


# blank `nebari-config.yaml`
class Config(BaseModel):
    pass


# current working directory
output_dir = "."

stage = LabelStudioHelmStage(output_dir, Config())


# Items below this line will be handled by Nebari but included to run the example
contents = stage.render()

# write the contents to the output_dir
for output_filename, data in contents.items():
    dirname = Path(output_filename).parent
    dirname.mkdir(parents=True, exist_ok=True)
    with open(output_filename, "w+") as f:
        f.write(data)
