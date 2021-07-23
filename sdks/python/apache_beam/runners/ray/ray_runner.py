#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""RayRunner, executing on a Ray cluster.

"""

# pytype: skip-file
import copy
import logging


from apache_beam.options.pipeline_options import DirectOptions
from apache_beam.options.value_provider import RuntimeValueProvider
from apache_beam.pipeline import PTransformOverride
from apache_beam.runners.direct.bundle_factory import BundleFactory
from apache_beam.runners.direct.clock import RealClock
from apache_beam.runners.direct.clock import TestClock
from apache_beam.runners.direct.direct_runner import DirectPipelineResult, \
  BundleBasedDirectRunner, _get_transform_overrides
from apache_beam.runners.runner import PipelineState

__all__ = ['RayRunner']

_LOGGER = logging.getLogger(__name__)


class RayRunner(BundleBasedDirectRunner):
  """Executes a single pipeline on the local machine."""
  @staticmethod
  def is_fnapi_compatible():
    return False

  def run_pipeline(self, pipeline, options):
    """Execute the entire pipeline and returns an DirectPipelineResult."""

    # TODO: Move imports to top. Pipeline <-> Runner dependency cause problems
    # with resolving imports when they are at top.
    # pylint: disable=wrong-import-position
    from apache_beam.pipeline import PipelineVisitor
    from apache_beam.runners.direct.consumer_tracking_pipeline_visitor import \
      ConsumerTrackingPipelineVisitor
    from apache_beam.runners.ray.evaluation_context import RayEvaluationContext
    from apache_beam.runners.ray.executor import Executor
    from apache_beam.runners.direct.transform_evaluator import \
      TransformEvaluatorRegistry
    from apache_beam.testing.test_stream import TestStream

    # If the TestStream I/O is used, use a mock test clock.
    class TestStreamUsageVisitor(PipelineVisitor):
      """Visitor determining whether a Pipeline uses a TestStream."""
      def __init__(self):
        self.uses_test_stream = False

      def visit_transform(self, applied_ptransform):
        if isinstance(applied_ptransform.transform, TestStream):
          self.uses_test_stream = True

    visitor = TestStreamUsageVisitor()
    pipeline.visit(visitor)
    clock = TestClock() if visitor.uses_test_stream else RealClock()

    overrides = _get_transform_overrides(options)

    # Annotations are protobufs and not pickleable
    class RemoveAnnotationsOverride(PTransformOverride):
      def matches(self, applied_ptransform):
        return applied_ptransform.annotations is not None

      def get_replacement_transform_for_applied_ptransform(
          self, applied_ptransform):
        # Todo: Create new instance here
        new_ptransform = applied_ptransform
        # Todo: Resolve instead of setting to None?
        new_ptransform.annotations = None
        return new_ptransform.transform

    overrides.append(RemoveAnnotationsOverride())

    # Performing configured PTransform overrides.
    pipeline.replace_all(overrides)

    _LOGGER.info('Running pipeline with RayRunner.')
    self.consumer_tracking_visitor = ConsumerTrackingPipelineVisitor()
    pipeline.visit(self.consumer_tracking_visitor)

    evaluation_context = RayEvaluationContext(
        options,
        BundleFactory(
            stacked=options.view_as(
                DirectOptions).direct_runner_use_stacked_bundle),
        self.consumer_tracking_visitor.root_transforms,
        self.consumer_tracking_visitor.value_to_consumers,
        self.consumer_tracking_visitor.step_names,
        self.consumer_tracking_visitor.views,
        clock)

    executor = Executor(
        self.consumer_tracking_visitor.value_to_consumers,
        TransformEvaluatorRegistry(evaluation_context),
        evaluation_context)
    # DirectRunner does not support injecting
    # PipelineOptions values at runtime
    RuntimeValueProvider.set_runtime_options({})
    # Start the executor. This is a non-blocking call, it will start the
    # execution in background threads and return.
    executor.start(self.consumer_tracking_visitor.root_transforms)
    result = RayPipelineResult(executor, evaluation_context)

    return result


class RayPipelineResult(DirectPipelineResult):
  """A RayPipelineResult provides access to info about a pipeline."""
  def __del__(self):
    if self._state == PipelineState.RUNNING:
      _LOGGER.warning(
          'The RayPipelineResult is being garbage-collected while the '
          'RayRunner is still running the corresponding pipeline. This may '
          'lead to incomplete execution of the pipeline if the main thread '
          'exits before pipeline completion. Consider using '
          'result.wait_until_finish() to wait for completion of pipeline '
          'execution.')

  def wait_until_finish(self, duration=None):
    if not PipelineState.is_terminal(self.state):
      if duration:
        raise NotImplementedError(
            'RayRunner does not support duration argument.')
      try:
        self._executor.await_completion()
        self._state = PipelineState.DONE
      except:  # pylint: disable=broad-except
        self._state = PipelineState.FAILED
        raise
    return self._state
