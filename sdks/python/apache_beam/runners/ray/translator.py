import inspect
from functools import partial
from typing import Mapping, Sequence

import ray.data

from apache_beam import Create, Union, ParDo, Impulse, PTransform, Reshuffle, GroupByKey, WindowInto, Flatten, \
  CoGroupByKey, DoFn
from apache_beam.internal.util import ArgumentPlaceholder
from apache_beam.pipeline import PTransformOverride, AppliedPTransform
from apache_beam.runners.common import MethodWrapper
from apache_beam.runners.ray.collection import CollectionMap
from apache_beam.runners.ray.side_input import RaySideInput
from apache_beam.runners.ray.util import group_by_key
from apache_beam.transforms.window import WindowFn, TimestampedValue
from apache_beam.typehints import Optional
from apache_beam.utils.timestamp import Timestamp
from apache_beam.utils.windowed_value import WindowedValue


class RayDataTranslation:
  def __init__(self, applied_ptransform: AppliedPTransform):
    self.applied_ptransform = applied_ptransform

  def apply(
      self, 
      ray_ds: Union[None, ray.data.Dataset, Mapping[str, ray.data.Dataset]] = None,
      side_inputs: Optional[Sequence[ray.data.Dataset]] = None):
    raise NotImplementedError


class RayNoop(RayDataTranslation):
  def apply(
      self, 
      ray_ds: Union[None, ray.data.Dataset, Mapping[str, ray.data.Dataset]] = None,
      side_inputs: Optional[Sequence[ray.data.Dataset]] = None):
    return ray_ds


class RayImpulse(RayDataTranslation):
  def apply(
      self, 
      ray_ds: Union[None, ray.data.Dataset, Mapping[str, ray.data.Dataset]] = None,
      side_inputs: Optional[Sequence[ray.data.Dataset]] = None):
    assert ray_ds is None
    return ray.data.from_items([0], parallelism=1)


class RayCreate(RayDataTranslation):
  def apply(
      self, 
      ray_ds: Union[None, ray.data.Dataset, Mapping[str, ray.data.Dataset]] = None,
      side_inputs: Optional[Sequence[ray.data.Dataset]] = None):
    assert ray_ds is None

    original_transform: Create = self.applied_ptransform.transform

    items = original_transform.values

    # Hack: Replace all sub parts
    self.applied_ptransform.parts = []

    # Todo: parallelism should be configurable
    # Setting this to < 1 leads to errors for assert_that checks
    return ray.data.from_items(items, parallelism=1)


class RayParDo(RayDataTranslation):
  def apply(
      self, 
      ray_ds: Union[None, ray.data.Dataset, Mapping[str, ray.data.Dataset]] = None,
      side_inputs: Optional[Sequence[ray.data.Dataset]] = None):
    assert ray_ds is not None
    assert isinstance(ray_ds, ray.data.Dataset)
    assert self.applied_ptransform.transform is not None
    assert isinstance(self.applied_ptransform.transform, ParDo)

    # Get original function and side inputs
    transform = self.applied_ptransform.transform
    map_fn = transform.fn
    args = transform.args or []
    kwargs = transform.kwargs or {}

    # Todo: datasets are not iterable, so we fetch pandas dfs here
    # This is a ray get anti-pattern! This will not scale to either many side inputs
    # or to large dataset sizes. Fix this!
    def convert_pandas(ray_side_input: RaySideInput):
      df = ray.get(ray_side_input.ray_ds.to_pandas())[0]
      return ray_side_input.convert_df(df)

    # side_inputs
    side_inputs = [convert_pandas(ray_side_input) for ray_side_input in side_inputs]

    # Replace args
    new_args = []
    for arg in args:
      if isinstance(arg, ArgumentPlaceholder):
        new_args.append(side_inputs.pop(0))
      else:
        new_args.append(arg)

    return ray_ds.flat_map(lambda x: map_fn.process(x, *new_args, **kwargs))

    # Todo: fix value parsing and side input parsing

    method = MethodWrapper(map_fn, "process")

    def get_item_attributes(item):
      if isinstance(item, tuple):
        key, value = item
      else:
        key = None
        value = item

      if isinstance(value, WindowedValue):
        timestamp = value.timestamp
        window = value.windows[0]
        elem = value.value
      else:
        timestamp = None
        window = None
        elem = value

      return elem, key, timestamp, window

    def execute(item):
      elem, key, timestamp, window = get_item_attributes(item)

      kwargs = {}
      # if method.has_userstate_arguments:
      #   for kw, state_spec in method.state_args_to_replace.items():
      #     kwargs[kw] = user_state_context.get_state(state_spec, key, window)
      #   for kw, timer_spec in method.timer_args_to_replace.items():
      #     kwargs[kw] = user_state_context.get_timer(
      #       timer_spec, key, window, timestamp, pane_info)

      if method.timestamp_arg_name:
        kwargs[method.timestamp_arg_name] = Timestamp.of(timestamp)
      if method.window_arg_name:
        kwargs[method.window_arg_name] = window
      if method.key_arg_name:
        kwargs[method.key_arg_name] = key
      # if method.dynamic_timer_tag_arg_name:
      #   kwargs[method.dynamic_timer_tag_arg_name] = dynamic_timer_tag

      return method.method_value(elem, *new_args, **kwargs)

    return ray_ds.flat_map(execute)


class RayGroupByKey(RayDataTranslation):
  def apply(
      self, 
      ray_ds: Union[None, ray.data.Dataset, Mapping[str, ray.data.Dataset]] = None,
      side_inputs: Optional[Sequence[ray.data.Dataset]] = None):
    assert ray_ds is not None
    assert isinstance(ray_ds, ray.data.Dataset)

    # Todo: Assert WindowedValue?

    df = ray.get(ray_ds.to_pandas())[0]
    grouped = group_by_key(df)

    return ray.data.from_items(list(tuple(grouped.items())))


class RayCoGroupByKey(RayDataTranslation):
  def apply(
      self,
      ray_ds: Union[None, ray.data.Dataset, Mapping[str, ray.data.Dataset]] = None,
      side_inputs: Optional[Sequence[ray.data.Dataset]] = None):
    assert ray_ds is not None
    assert isinstance(ray_ds, ray.data.Dataset)

    raise RuntimeError("CoGroupByKey")

    return group_by_key(ray.get(ray_ds.to_pandas()[0]))


class RayWindowInto(RayDataTranslation):
  def apply(
      self,
      ray_ds: Union[None, ray.data.Dataset, Mapping[str, ray.data.Dataset]] = None,
      side_inputs: Optional[Sequence[ray.data.Dataset]] = None):
    window_fn = self.applied_ptransform.transform.windowing.windowfn

    def to_windowed_value(item):
      if isinstance(item, WindowedValue):
        return item

      if isinstance(item, TimestampedValue):
        return WindowedValue(item.value, item.timestamp, window_fn.assign(
          WindowFn.AssignContext(item.timestamp, element=item.value)))

      return item

    return ray_ds.map(to_windowed_value)


class RayFlatten(RayDataTranslation):
  def apply(
      self, 
      ray_ds: Union[None, ray.data.Dataset, Mapping[str, ray.data.Dataset]] = None,
      side_inputs: Optional[Sequence[ray.data.Dataset]] = None):
    assert ray_ds is not None
    assert isinstance(ray_ds, Mapping)
    assert len(ray_ds) >= 1

    keys = sorted(ray_ds.keys())
    primary_key = keys.pop(0)

    primary_ds = ray_ds[primary_key]
    return primary_ds.union(*[ray_ds[key] for key in keys]).repartition(1)


translations = {
  Create: RayCreate,  # Composite transform
  Impulse: RayImpulse,
  ParDo: RayParDo,
  Flatten: RayFlatten,
  Reshuffle: RayNoop,  # Todo: Add
  WindowInto: RayNoop,  # RayWindowInto,
  GroupByKey: RayGroupByKey,
  CoGroupByKey: RayCoGroupByKey,
  PTransform: RayNoop  # Todo: How to handle generic ptransforms? Map?
}


# Test translations
class RayTestTranslation(RayDataTranslation):
  pass


class RayAssertThat(RayTestTranslation):
  def apply(
      self, 
      ray_ds: Union[None, ray.data.Dataset, Mapping[str, ray.data.Dataset]] = None,
      side_inputs: Optional[Sequence[ray.data.Dataset]] = None):
    assert isinstance(ray_ds, ray.data.Dataset)

    map_fn = self.applied_ptransform.parts[-1].transform.fn

    # Hack: Replace all sub parts
    self.applied_ptransform.parts = []

    return ray_ds.map_batches(map_fn.process)


test_translations = {
  # "assert_that": RayAssertThat
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
