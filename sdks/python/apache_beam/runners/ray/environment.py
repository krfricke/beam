from apache_beam.options.pipeline_options import PortableOptions
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.runners.pipeline_context import PipelineContext
from apache_beam.transforms.environments import Environment, python_sdk_capabilities, python_sdk_dependencies
from apache_beam.transforms.resources import resource_hints_from_options
from apache_beam.typehints import Tuple, Iterable
from typing import Mapping

RAY_URN = "beam:env:ray:v1"


@Environment.register_urn(RAY_URN, None)
class RayWorkerEnvironment(Environment):
  def to_runner_api_parameter(self, context):
    # type: (PipelineContext) -> Tuple[str, None]
    return RAY_URN, None

  @staticmethod
  def from_runner_api_parameter(unused_payload,  # type: None
      capabilities,  # type: Iterable[str]
      artifacts,  # type: Iterable[beam_runner_api_pb2.ArtifactInformation]
      resource_hints,  # type: Mapping[str, bytes]
      context  # type: PipelineContext
                                ):
    # type: (...) -> RayWorkerEnvironment
    return RayWorkerEnvironment(capabilities, artifacts, resource_hints)

  @classmethod
  def from_options(cls, options):
    # type: (PortableOptions) -> RayWorkerEnvironment
    return cls(
        capabilities=python_sdk_capabilities(),
        artifacts=python_sdk_dependencies(options),
        resource_hints=resource_hints_from_options(options),
    )

  @classmethod
  def default(cls):
    # type: () -> RayWorkerEnvironment
    return cls(capabilities=python_sdk_capabilities(), artifacts=())
