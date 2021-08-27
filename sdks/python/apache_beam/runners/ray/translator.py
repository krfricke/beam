import ray.data

from apache_beam import Create, PTransform
from apache_beam.pipeline import PTransformOverride, AppliedPTransform
from apache_beam.pvalue import PBegin
from apache_beam.runners.ray.transform import RayDataTransform, RayTestTransform
from apache_beam.typehints import Optional


class RayDataTranslation:
  def __call__(self, applied_ptransform: AppliedPTransform, graph: Optional[ray.data.Dataset]):
    raise NotImplementedError


class RayCreate(RayDataTranslation):
  def __call__(self, applied_ptransform: AppliedPTransform, graph: Optional[ray.data.Dataset]):
    assert graph is None

    original_transform: Create = applied_ptransform.transform

    items = original_transform.values
    # Todo: parallelism should be configurable
    # Setting this to < 1 leads to errors for assert_that checks
    return ray.data.from_items(items, parallelism=1)


translations = {
  Create: RayCreate
}


# Test translations
class RayTestTranslation:
  def __call__(self, applied_ptransform: AppliedPTransform, graph: Optional[ray.data.Dataset]):
    raise NotImplementedError


class RayAssertThat(RayTestTranslation):
  def __call__(self, applied_ptransform: AppliedPTransform, graph: Optional[ray.data.Dataset]):
    map_fn = applied_ptransform.parts[-1].transform.fn
    return graph.map_batches(map_fn.process, batch_size=2)


test_translations = {
  "assert_that": RayAssertThat
}


class TranslateRayDataOverride(PTransformOverride):
  def __init__(self):
    self.graphs = {}

  def matches(self, applied_ptransform: AppliedPTransform):
    return (
        type(applied_ptransform.transform) in translations.keys()
        or applied_ptransform.full_label in test_translations.keys()
    )

  def get_replacement_transform_for_applied_ptransform(
      self, applied_ptransform: AppliedPTransform):

    is_test_transform = False

    # Sanity check
    type_ = type(applied_ptransform.transform)
    if type_ not in translations:
      label = applied_ptransform.full_label
      if label not in test_translations:
        raise RuntimeError(f"Could not translate transform of type {type(applied_ptransform.transform)}")
      translation_factory = test_translations[label]
      is_test_transform = True
    else:
      translation_factory = translations[type_]

    translation = translation_factory()

    results = {}
    for name, element in applied_ptransform.main_inputs.items():
      graph_name = str(name)
      if isinstance(element, PBegin):
        graph = None
      else:
        graph = self.graphs.get(graph_name, None)

      result = translation(applied_ptransform, graph)

      # Todo: Go via applied_ptransform.outputs.items() instead?
      self.graphs[graph_name] = result

    if not is_test_transform:
      output = RayDataTransform(applied_ptransform, results)
    else:
      output = RayTestTransform(applied_ptransform, results)

    return output
