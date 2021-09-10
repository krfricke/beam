from apache_beam import PTransform
from apache_beam.portability import common_urns
from apache_beam.pvalue import PBegin, TaggedOutput
from apache_beam.runners.ray.collection import CollectionMap
from apache_beam.runners.ray.side_input import RayDictSideInput, RayListSideInput, RaySideInput, RayMultiMapSideInput
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
    else:
      ray_ds = {}
      for name in applied_ptransform.main_inputs.keys():
        ray_ds[name] = named_inputs.pop(name)

      if len(ray_ds) == 1:
        ray_ds = list(ray_ds.values())[0]

    side_inputs = []
    for side_input in applied_ptransform.side_inputs:
      side_ds = self._collection_map.get(side_input.pvalue)
      input_data = side_input._side_input_data()
      if input_data.access_pattern == common_urns.side_inputs.MULTIMAP.urn:
        wrapped_input = RayMultiMapSideInput(side_ds)
      elif input_data.access_pattern == list:
        wrapped_input = RayListSideInput(side_ds)
      elif input_data.view_fn == dict:
        wrapped_input = RayDictSideInput(side_ds)
      else:
        wrapped_input = RaySideInput(side_ds)

      side_inputs.append(wrapped_input)

    import ray
    print("APPLYING", applied_ptransform.full_label, ray.get(ray_ds.to_pandas()) if isinstance(ray_ds, ray.data.Dataset) else ray_ds)
    result = self._translation.apply(ray_ds, side_inputs=side_inputs)
    print("RESULT", ray.get(result.to_pandas()) if  isinstance(result, ray.data.Dataset) else result)

    for name, element in applied_ptransform.named_outputs().items():
      if isinstance(result, dict):
        out = result.get(name)
      else:
        out = result

      if out:
        if name != "None":
          # Side output
          out = out.filter(lambda x: isinstance(x, TaggedOutput) and x.tag == name)
          out = out.map(lambda x: x.value)
        else:
          # Main output
          out = out.filter(lambda x: not isinstance(x, TaggedOutput))

      self._collection_map.set(element, out)

    return applied_ptransform.outputs

  def expand(self, input_or_inputs):
    return self.apply_ray_dataset(input_or_inputs)


class RayTestTransform(RayDataTransform):
  pass
