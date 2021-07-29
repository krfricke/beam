import os
import threading

import grpc
import ray
from apache_beam.portability.api import beam_fn_api_pb2_grpc, endpoints_pb2
from apache_beam.runners.portability.fn_api_runner.fn_runner import ExtendedProvisionInfo
from apache_beam.runners.portability.fn_api_runner.worker_handlers import (WorkerHandler, GrpcWorkerHandler, _LOGGER,
  StateServicer, GrpcServer)
from apache_beam.runners.ray.environment import RAY_URN
from apache_beam.runners.ray.util import get_my_ip
from apache_beam.utils import thread_pool_executor
from google.protobuf import text_format


class BeamFnLoggingServicer(beam_fn_api_pb2_grpc.BeamFnLoggingServicer):
    def Logging(self, log_bundles, context=None):
        for log_bundle in log_bundles:
            for log_entry in log_bundle.log_entries:
                _LOGGER.info('Worker: %s', str(log_entry).replace('\n', ' '))
        return iter([])


@ray.remote
class BeamWorker(object):
    def __init__(self, env_dict):
        os.environ.update(env_dict)
        from apache_beam.runners.worker.sdk_worker_main import main
        main([])

    def await_termination(self):
        return "exited"


@ray.remote
def beam_worker_fn(env_dict):
    os.environ.update(env_dict)
    from apache_beam.runners.worker.sdk_worker_main import main
    main([])


class RaySDKWorker(object):
    """Manages a SDK worker implemented as a Ray actor."""

    def __init__(self, control_address, worker_id=None):
        self._control_address = control_address.replace(
            "localhost", get_my_ip())
        self._worker_id = worker_id

    def run(self):
        logging_server = grpc.server(
            thread_pool_executor.shared_unbounded_instance())
        logging_port = logging_server.add_insecure_port('[::]:0')
        logging_server.start()
        logging_servicer = BeamFnLoggingServicer()
        beam_fn_api_pb2_grpc.add_BeamFnLoggingServicer_to_server(
            logging_servicer, logging_server)
        logging_descriptor = text_format.MessageToString(
            endpoints_pb2.ApiServiceDescriptor(url='%s:%s' % (get_my_ip(),
                                                              logging_port)))

        control_descriptor = text_format.MessageToString(
            endpoints_pb2.ApiServiceDescriptor(url=self._control_address))

        env_dict = dict(
            os.environ,
            CONTROL_API_SERVICE_DESCRIPTOR=control_descriptor,
            LOGGING_API_SERVICE_DESCRIPTOR=logging_descriptor,
            RAY_DRIVER_CWD=os.getcwd())
        # only add worker_id when it is set.
        if self._worker_id:
            env_dict['WORKER_ID'] = self._worker_id

        actor = BeamWorker.remote(env_dict)
        try:
            # ray.get(beam_worker_fn.remote(env_dict))
            ray.get(actor.await_termination.remote())
        finally:
            logging_server.stop(0)



@WorkerHandler.register_environment(RAY_URN, bytes)
class RayWorkerHandler(GrpcWorkerHandler):
    def __init__(
            self,
            worker_command_line,  # type: bytes
            state,  # type: StateServicer
            provision_info,  # type: ExtendedProvisionInfo
            grpc_server  # type: GrpcServer
    ):
        # type: (...) -> None
        super(RayWorkerHandler, self).__init__(state, provision_info,
                                               grpc_server)
        self._worker_command_line = worker_command_line

    def start_worker(self):
        self.worker = RaySDKWorker(self.control_address, self.worker_id)
        self.worker_thread = threading.Thread(
            name='run_worker', target=self.worker.run)
        self.worker_thread.start()
        print("Ray BEAM worker started")

    def stop_worker(self):
        print("Stop worker")
        self.worker_thread.join()

    def host_from_worker(self):
        return get_my_ip()
