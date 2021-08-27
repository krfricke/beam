import ray.data

from apache_beam import PTransform, DoFn
from apache_beam.pipeline import AppliedPTransform
from apache_beam.typehints import Dict


class RayDataTransform(PTransform):
  def __init__(self, original_transform: AppliedPTransform, results: Dict[str, ray.data.Dataset]):
    super(RayDataTransform, self).__init__()
    self.original_transform = original_transform
    self.results = results

  def expand(self, input_or_inputs):
    return self.original_transform.outputs


class RayTestTransform(RayDataTransform):
  pass
