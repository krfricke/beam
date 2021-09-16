from apache_beam import PTransform, Create, pvalue
from apache_beam.pipeline import PTransformOverride


class _Create(PTransform):
  def __init__(self, values):
    self.values = values

  def expand(self, input_or_inputs):
    return pvalue.PCollection.from_(input_or_inputs)

def _get_overrides():
  class CreateOverride(PTransformOverride):
    def matches(self, applied_ptransform):
      # Note: we match the exact class, since we replace it with a subclass.
      return applied_ptransform.transform.__class__ == Create

    def get_replacement_transform_for_applied_ptransform(
        self, applied_ptransform):
      # Use specialized streaming implementation.
      transform = _Create(applied_ptransform.transform.values)
      return transform

  return [
    CreateOverride()
  ]
