import threading

from apache_beam.runners.direct.evaluation_context import EvaluationContext


class RayEvaluationContext(EvaluationContext):
  def __getstate__(self):
    state = self.__dict__.copy()
    state.pop("_lock", None)
    return state

  def __setstate__(self, state):
    self.__dict__.update(state)
    self._lock = threading.Lock()
