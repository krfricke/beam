from typing import Mapping

import ray.data

from apache_beam import Create, Union, ParDo, Impulse, PTransform, Reshuffle, GroupByKey
from apache_beam.pipeline import PTransformOverride, AppliedPTransform
from apache_beam.runners.ray.collection import CollectionMap


class RayDataTranslation:
  def __init__(self, applied_ptransform: AppliedPTransform):
    self.applied_ptransform = applied_ptransform

  def apply(self, ray_ds: Union[None, ray.data.Dataset, Mapping[str, ray.data.Dataset]] = None):
    raise NotImplementedError


class RayNoop(RayDataTranslation):
  def apply(self, ray_ds: Union[None, ray.data.Dataset, Mapping[str, ray.data.Dataset]] = None):
    return ray_ds


class RayImpulse(RayDataTranslation):
  def apply(self, ray_ds: Union[None, ray.data.Dataset, Mapping[str, ray.data.Dataset]] = None):
    assert ray_ds is None
    return ray.data.from_items([0], parallelism=1)


class RayCreate(RayDataTranslation):
  def apply(self, ray_ds: Union[None, ray.data.Dataset, Mapping[str, ray.data.Dataset]] = None):
    assert ray_ds is None

    original_transform: Create = self.applied_ptransform.transform

    items = original_transform.values

    # Hack: Replace all sub parts
    self.applied_ptransform.parts = []

    # Todo: parallelism should be configurable
    # Setting this to < 1 leads to errors for assert_that checks
    return ray.data.from_items(items, parallelism=1)


class RayParDo(RayDataTranslation):
  def apply(self, ray_ds: Union[None, ray.data.Dataset, Mapping[str, ray.data.Dataset]] = None):
    assert ray_ds is not None
    assert isinstance(ray_ds, ray.data.Dataset)
    assert self.applied_ptransform.transform is not None
    assert isinstance(self.applied_ptransform.transform, ParDo)

    map_fn = self.applied_ptransform.transform.fn
    return ray_ds.flat_map(map_fn.process)


class RayGroupByKey(RayDataTranslation):
  def apply(self, ray_ds: Union[None, ray.data.Dataset, Mapping[str, ray.data.Dataset]] = None):
    assert ray_ds is not None
    assert isinstance(ray_ds, ray.data.Dataset)

    def _group_by_key(df):
      groups = {
        part[0]: list(part[1][1])
        for part in df.groupby(0)
      }

      # Emit [(key, [val1, val2]), ...]
      return ray.data.from_items(list(tuple(groups.items())))

    return _group_by_key(ray.get(ray_ds.to_pandas()[0]))


translations = {
  Create: RayCreate,  # Composite transform
  Impulse: RayImpulse,
  ParDo: RayParDo,
  Reshuffle: RayNoop,  # Todo: Add
  GroupByKey: RayGroupByKey,
  PTransform: RayNoop  # Todo: How to handle generic ptransforms? Map?
}


# Test translations
class RayTestTranslation(RayDataTranslation):
  pass


class RayAssertThat(RayTestTranslation):
  def apply(self, ray_ds: Union[None, ray.data.Dataset, Mapping[str, ray.data.Dataset]] = None):
    assert isinstance(ray_ds, ray.data.Dataset)

    map_fn = self.applied_ptransform.parts[-1].transform.fn

    # Hack: Replace all sub parts
    self.applied_ptransform.parts = []

    return ray_ds.map_batches(map_fn.process)


test_translations = {
  "assert_that": RayAssertThat
}


class TranslateRayDataOverride(PTransformOverride):
  def __init__(self, collection_map: CollectionMap):
    self.collection_map = collection_map
    self.graphs = {}

  def matches(self, applied_ptransform: AppliedPTransform):
    print(applied_ptransform.full_label, type(applied_ptransform.transform), type(applied_ptransform.transform) in translations.keys())
    return (
        type(applied_ptransform.transform) in translations.keys()
        or applied_ptransform.full_label in test_translations.keys()
    )

  def get_replacement_transform_for_applied_ptransform(
      self, applied_ptransform: AppliedPTransform):
    from apache_beam.runners.ray.transform import RayDataTransform, RayTestTransform

    is_test_transform = False

    # Sanity check
    type_ = type(applied_ptransform.transform)
    if type_ not in translations:
      label = applied_ptransform.full_label
      applied_ptransform.named_outputs()
      if label not in test_translations:
        raise RuntimeError(f"Could not translate transform of type {type(applied_ptransform.transform)}")
      translation_factory = test_translations[label]
      is_test_transform = True
    else:
      label = applied_ptransform.full_label
      if label in test_translations:
        translation_factory = test_translations[label]
      else:
        translation_factory = translations[type_]

    translation = translation_factory(applied_ptransform)

    if not is_test_transform:
      output = RayDataTransform(self.collection_map, translation)
    else:
      output = RayTestTransform(self.collection_map, translation)

    return output
