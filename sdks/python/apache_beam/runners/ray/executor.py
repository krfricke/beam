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

"""An executor that schedules and executes applied ptransforms."""

# pytype: skip-file
import asyncio
import itertools
import logging
import sys
from typing import TYPE_CHECKING
from typing import Dict
from typing import Optional

from apache_beam import pvalue
from apache_beam.runners.direct.executor import \
    _ExecutorServiceParallelExecutor, _TransformExecutorServices, \
    _CompletionCallback, _ExecutorService
from apache_beam.runners.worker import statesampler
from apache_beam.utils import counters

import ray
from ray.util.queue import Queue as QueueBase, Empty

if TYPE_CHECKING:
  from apache_beam.runners.direct.evaluation_context import EvaluationContext

_LOGGER = logging.getLogger(__name__)


class Queue(QueueBase):
  def task_done(self):
    # Todo: Implement task_done
    pass


class _EventActor:
    def __init__(self):
        self._event = asyncio.Event()

    def set(self):
        self._event.set()

    def clear(self):
        self._event.clear()

    def is_set(self):
        return self._event.is_set()


class Event:
    def __init__(self, actor_options: Optional[Dict] = None):
        actor_options = {} if not actor_options else actor_options
        self.actor = ray.remote(_EventActor).options(**actor_options).remote()

    def set(self):
        self.actor.set.remote()

    def clear(self):
        self.actor.clear.remote()

    def is_set(self):
        return ray.get(self.actor.is_set.remote())

    def shutdown(self):
        if self.actor:
            ray.kill(self.actor)
        self.actor = None


class _ExecutorServiceRayExecutor(_ExecutorServiceParallelExecutor):
  """An internal implementation for Executor."""

  NUM_WORKERS = 1

  def __init__(
      self,
      value_to_consumers,
      transform_evaluator_registry,
      evaluation_context  # type: EvaluationContext
  ):
    self.executor_service = _RayExecutorService(
        _ExecutorServiceParallelExecutor.NUM_WORKERS)
    self.transform_executor_services = _TransformExecutorServices(
        self.executor_service)
    self.value_to_consumers = value_to_consumers
    self.transform_evaluator_registry = transform_evaluator_registry
    self.evaluation_context = evaluation_context
    self.all_updates = _ExecutorServiceRayExecutor._TypedUpdateRayQueue(
        _ExecutorServiceParallelExecutor._ExecutorUpdate)
    self.visible_updates = _ExecutorServiceRayExecutor._TypedUpdateRayQueue(
        _ExecutorServiceParallelExecutor._VisibleExecutorUpdate)
    self.default_completion_callback = _CompletionCallback(
        evaluation_context, self.all_updates)

  class _TypedUpdateRayQueue(object):
    """Type checking update queue with blocking and non-blocking operations."""
    def __init__(self, item_type):
      self._item_type = item_type
      self._queue = Queue()

    def poll(self):
      try:
        item = self._queue.get_nowait()
        self._queue.task_done()
        return item
      except Empty:
        return None

    def take(self):
      # The implementation of Queue.Queue.get() does not propagate
      # KeyboardInterrupts when a timeout is not used.  We therefore use a
      # one-second timeout in the following loop to allow KeyboardInterrupts
      # to be correctly propagated.
      while True:
        try:
          item = self._queue.get(timeout=1)
          self._queue.task_done()
          return item
        except Empty:
          pass

    def offer(self, item):
      assert isinstance(item, self._item_type)
      self._queue.put_nowait(item)

  def start(self, roots):
    self.root_nodes = frozenset(roots)
    self.all_nodes = frozenset(
        itertools.chain(
            roots, *itertools.chain(self.value_to_consumers.values())))
    self.node_to_pending_bundles = {}
    for root_node in self.root_nodes:
      provider = (
          self.transform_evaluator_registry.get_root_bundle_provider(root_node))
      self.node_to_pending_bundles[root_node] = provider.get_root_bundles()
    self.executor_service.submit(
        _RayMonitorTask(self))

class Executor(object):
  """For internal use only; no backwards-compatibility guarantees."""
  def __init__(self, *args, **kwargs):
    self._executor = _ExecutorServiceRayExecutor(*args, **kwargs)

  def start(self, roots):
    self._executor.start(roots)

  def await_completion(self):
    self._executor.await_completion()

  def shutdown(self):
    self._executor.request_shutdown()



class _RayExecutorService(_ExecutorService):
  """Ray pool for executing tasks in parallel."""

  @ray.remote
  class _RayExecutorServiceWorker(object):
    """Worker thread for executing a single task at a time."""

    # Amount to block waiting for getting an item from the queue in seconds.
    TIMEOUT = 5

    def __init__(
        self,
        stop_event,  # type: Event
        queue,  # type: Queue[_RayExecutorService.CallableTask]
        index):
      self.stop_event = stop_event
      self.queue = queue
      self._index = index
      self._default_name = 'RayExecutorServiceWorker-' + str(index)
      self._update_name()

    @property
    def shutdown_requested(self):
      return self.stop_event.is_set()

    def _update_name(self, task=None):
      if task and task.name:
        name = task.name
      else:
        name = self._default_name
      self.name = 'Ray job: %d, %s (%s)' % (
          self._index, name, 'executing' if task else 'idle')

    def _get_task_or_none(self):
      # type: () -> Optional[_RayExecutorService.CallableTask]
      try:
        # Do not block indefinitely, otherwise we may not act for a requested
        # shutdown.
        return self.queue.get(
            timeout=_RayExecutorService._RayExecutorServiceWorker.TIMEOUT)
      except Empty:
        return None

    def run(self):
      state_sampler = statesampler.StateSampler('', counters.CounterFactory())
      statesampler.set_current_tracker(state_sampler)
      print("DEBUG: I AM NOW SAMPLING FOR A TASK")
      while not self.shutdown_requested:
        task = self._get_task_or_none()
        if task:
          try:
            if not self.shutdown_requested:
              self._update_name(task)
              print("DEBUG: I WOULD LIKE TO CALL TASK", task, task._executor.__dict__)
              task.call(state_sampler)
              print("DEBUG: I CALLED IT", task)
              self._update_name()
          finally:
            self.queue.task_done()

  def __init__(self, num_workers):
    self._stop_event = Event()  # Todo: place on head node?

    self.queue = Queue(
    )  # type: Queue[_RayExecutorService.CallableTask]

    self.workers = [
        _RayExecutorService._RayExecutorServiceWorker.remote(self._stop_event, self.queue, i)
        for i in range(num_workers)
    ]
    self.futures = [
      worker.run.remote()
      for worker in self.workers
    ]
    self.shutdown_requested = False

  def submit(self, task):
    # type: (_RayExecutorService.CallableTask) -> None
    assert isinstance(task, _RayExecutorService.CallableTask)

    if not self.shutdown_requested:
      self.queue.put(task)

  def await_completion(self):
    pending = self.futures
    while pending:
      ready, pending = ray.wait(pending, timeout=0.5, num_returns=1)

      for fut in ready:
        try:
          ray.get(fut)
        except Exception as e:
          index = self.futures.index(fut)
          print(f"Worker {index} job failed: {e}")

    return True

  def shutdown(self):
    self.shutdown_requested = True
    self._stop_event.set()

    # Consume all the remaining items in the queue
    while not self.queue.empty():
      try:
        self.queue.get_nowait()
        self.queue.task_done()
      except Empty:
        continue


class _RayMonitorTask(_ExecutorService.CallableTask):
  """MonitorTask continuously runs to ensure that pipeline makes progress."""
  def __init__(self, executor):
    # type: (_ExecutorServiceParallelExecutor) -> None
    self._executor = executor

  @property
  def name(self):
    return 'monitor'

  def call(self, state_sampler):
    try:
      update = self._executor.all_updates.poll()
      while update:
        if update.committed_bundle:
          self._executor.schedule_consumers(update.committed_bundle)
        elif update.unprocessed_bundle:
          self._executor.schedule_unprocessed_bundle(
              update.transform_executor._applied_ptransform,
              update.unprocessed_bundle)
        else:
          assert update.exception
          _LOGGER.warning(
              'A task failed with exception: %s', update.exception)
          self._executor.visible_updates.offer(
              _ExecutorServiceParallelExecutor._VisibleExecutorUpdate(
                  update.exc_info))
        update = self._executor.all_updates.poll()
      self._executor.evaluation_context.schedule_pending_unblocked_tasks(
          self._executor.executor_service)
      self._add_work_if_necessary(self._fire_timers())
    except Exception as e:  # pylint: disable=broad-except
      _LOGGER.error('Monitor task died due to exception.\n %s', e)
      self._executor.visible_updates.offer(
          _ExecutorServiceParallelExecutor._VisibleExecutorUpdate(
              sys.exc_info()))
    finally:
      if not self._should_shutdown():
        self._executor.executor_service.submit(self)

  def _should_shutdown(self):
    # type: () -> bool

    """Checks whether the pipeline is completed and should be shut down.

    If there is anything in the queue of tasks to do or
    if there are any realtime timers set, do not shut down.

    Otherwise, check if all the transforms' watermarks are complete.
    If they are not, the pipeline is not progressing (stall detected).
    Whether the pipeline has stalled or not, the executor should shut
    down the pipeline.

    Returns:
      True only if the pipeline has reached a terminal state and should
      be shut down.

    """
    if self._is_executing():
      # There are some bundles still in progress.
      return False

    watermark_manager = self._executor.evaluation_context._watermark_manager
    _, any_unfired_realtime_timers = watermark_manager.extract_all_timers()
    if any_unfired_realtime_timers:
      return False

    else:
      if self._executor.evaluation_context.is_done():
        self._executor.visible_updates.offer(
            _ExecutorServiceParallelExecutor._VisibleExecutorUpdate())
      else:
        # Nothing is scheduled for execution, but watermarks incomplete.
        self._executor.visible_updates.offer(
            _ExecutorServiceParallelExecutor._VisibleExecutorUpdate((
                Exception('Monitor task detected a pipeline stall.'),
                None,
                None)))
      self._executor.executor_service.shutdown()
      return True

  def _fire_timers(self):
    """Schedules triggered consumers if any timers fired.

    Returns:
      True if timers fired.
    """
    transform_fired_timers, _ = (
        self._executor.evaluation_context.extract_all_timers())
    for applied_ptransform, fired_timers in transform_fired_timers:
      # Use an empty committed bundle. just to trigger.
      empty_bundle = (
          self._executor.evaluation_context.create_empty_committed_bundle(
              applied_ptransform.inputs[0]))
      timer_completion_callback = _CompletionCallback(
          self._executor.evaluation_context,
          self._executor.all_updates,
          timer_firings=fired_timers)

      self._executor.schedule_consumption(
          applied_ptransform,
          empty_bundle,
          fired_timers,
          timer_completion_callback)
    return bool(transform_fired_timers)

  def _is_executing(self):
    # type: () -> bool

    """Checks whether the job is still executing.

    Returns:
      True if there is at least one non-blocked TransformExecutor active."""

    executors = self._executor.transform_executor_services.executors
    if not executors:
      # Nothing is executing.
      return False

    # Ensure that at least one of those executors is not blocked.
    for transform_executor in executors:
      if not transform_executor.blocked:
        return True
    return False

  def _add_work_if_necessary(self, timers_fired):
    """Adds more work from the roots if pipeline requires more input.

    If all active TransformExecutors are in a blocked state, add more work
    from root nodes that may have additional work. This ensures that if a
    pipeline has elements available from the root nodes it will add those
    elements when necessary.

    Args:
      timers_fired: True if any timers fired prior to this call.
    """
    # If any timers have fired, they will add more work; No need to add more.
    if timers_fired:
      return

    if self._is_executing():
      # We have at least one executor that can proceed without adding
      # additional work.
      return

    # All current TransformExecutors are blocked; add more work from any
    # pending bundles.
    for applied_ptransform in self._executor.all_nodes:
      if not self._executor.evaluation_context.is_done(applied_ptransform):
        pending_bundles = self._executor.node_to_pending_bundles.get(
            applied_ptransform, [])
        for bundle in pending_bundles:
          self._executor.schedule_consumption(
              applied_ptransform,
              bundle, [],
              self._executor.default_completion_callback)
        self._executor.node_to_pending_bundles[applied_ptransform] = []
