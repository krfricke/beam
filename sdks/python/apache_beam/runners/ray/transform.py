from apache_beam import PTransform
from apache_beam.pvalue import PBegin
from apache_beam.runners.ray.collection import CollectionMap
from apache_beam.runners.ray.translator import RayDataTranslation


class RayDataTransform(PTransform):
  def __init__(self, collection_map: CollectionMap, translation: RayDataTranslation):
    super(RayDataTransform, self).__init__()
    self._collection_map = collection_map
    self._translation = translation

  def apply_ray_dataset(self, input_or_inputs):
    applied_ptransform = self._translation.applied_ptransform

    named_inputs = {}
    for name, element in applied_ptransform.named_inputs().items():
      if isinstance(element, PBegin):
        ray_ds = None
      else:
        ray_ds = self._collection_map.get(element)

      named_inputs[name] = ray_ds

    if len(named_inputs) == 0:
      ray_ds = None
    elif len(named_inputs) == 1:
      ray_ds = list(named_inputs.values())[0]
    else:
      ray_ds = named_inputs

    import ray
    print("APPLYING", applied_ptransform.full_label, ray.get(ray_ds.to_pandas()) if ray_ds else None)
    result = self._translation.apply(ray_ds)
    print("RESULT", ray.get(result.to_pandas()) if result else None)

    for name, element in applied_ptransform.named_outputs().items():
      if isinstance(result, dict):
        out = result.get(name)
      else:
        out = result

      self._collection_map.set(element, out)

    return applied_ptransform.outputs

  def expand(self, input_or_inputs):
    return self.apply_ray_dataset(input_or_inputs)


class RayTestTransform(RayDataTransform):
  pass
