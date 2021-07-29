from apache_beam.runners.runner import PipelineRunner
from apache_beam.runners.portability.fn_api_runner import FnApiRunner

from apache_beam.runners.ray.environment import RayWorkerEnvironment
from apache_beam.runners.ray.worker import RayWorkerHandler  # noqa: 401


class RayRunner(PipelineRunner):
    def run_pipeline(self, pipeline, options):
        return FnApiRunner(default_environment=RayWorkerEnvironment()
                           ).run_pipeline(pipeline, options)
