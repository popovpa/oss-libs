import logging

import time
import urllib3
from kubernetes import client, config
from kubernetes.client import V1DeleteOptions
from kubernetes.client.rest import ApiException
from kubernetes.stream import stream

urllib3.disable_warnings()

config.load_kube_config()

job_client = client.BatchV1Api()
pod_client = client.CoreV1Api()


def has_job(namespace, name):
    try:
        return job_client.read_namespaced_job(name=name, namespace=namespace) is not None
    except ApiException as ex:
        return False


def job_cleanup(namespace, name, clean_async=False):
    try:
        body = V1DeleteOptions(propagation_policy='Foreground')
        job_client.delete_namespaced_job(name=name, namespace=namespace, body=body)
        if not clean_async:
            while has_job(namespace=namespace, name=name):
                time.sleep(1)
        return True
    except ApiException as ex:
        logging.info(ex)
        return False


def orphan_pods_cleanup(namespace, job_name):
    try:
        pods = pod_client.list_namespaced_pod(
            namespace=namespace,
            label_selector='job-name={}'.format(job_name))
        for pod in pods.items:
            pod_client.delete_namespaced_pod(namespace=namespace, name=pod.metadata.name)
        return True
    except ApiException as ex:
        logging.error(ex)
        return False


def create_job(namespace, spec):
    try:
        job_client.create_namespaced_job(namespace=namespace, body=spec)
    except ApiException as ex:
        logging.error(ex)


def job_status(namespace, name):
    try:
        return job_client.read_namespaced_job_status(namespace=namespace, name=name).status
    except ApiException as ex:
        logging.error(ex)


def wait_job_starting(namespace, name):
    try:
        status = job_status(namespace=namespace, name=name)
        while status.active is None and status.succeeded is None and status.failed is None:
            logging.info(">>> Waiting for start job...")
            time.sleep(1)
            status = job_status(namespace=namespace, name=name)
    except ApiException as ex:
        logging.error(ex)


def job_is_running(namespace, name):
    actives = job_status(namespace=namespace, name=name).active
    return actives is not None and actives > 0


def get_pod_status(namespace, name):
    try:
        return pod_client.read_namespaced_pod_status(namespace=namespace, name=name).status
    except ApiException as ex:
        return None


def get_pods(namespace, job_name):
    return pod_client.list_namespaced_pod(namespace=namespace, label_selector='job-name={}'.format(job_name))


def get_succeeded_pods(namespace, job_name):
    return pod_client.list_namespaced_pod(namespace=namespace,
                                          label_selector='job-name={}'.format(job_name),
                                          field_selector='status.phase==Succeeded')


def get_active_pods(namespace, job_name):
    return pod_client.list_namespaced_pod(namespace=namespace,
                                          label_selector='job-name={}'.format(job_name),
                                          field_selector='status.phase==Running')


def get_pending_pods(namespace, job_name):
    return pod_client.list_namespaced_pod(namespace=namespace,
                                          label_selector='job-name={}'.format(job_name),
                                          field_selector='status.phase==Pending')


def get_log(namespace, pod_name, container_name, timeout):
    return pod_client.read_namespaced_pod_log(namespace=namespace,
                                              name=pod_name,
                                              container=container_name,
                                              _request_timeout=(timeout, timeout),
                                              follow=True,
                                              pretty=True,
                                              tail_lines=100,
                                              _preload_content=False)


def container_is_running(namespace, pod_name, container_name):
    event = read_pod(namespace=namespace, pod_name=pod_name)
    status = next(iter(filter(lambda s: s.name == container_name,
                              event.status.container_statuses)), None)
    return status.state.running is not None


def read_pod(namespace, pod_name):
    return pod_client.read_namespaced_pod(name=pod_name, namespace=namespace)


def get_events(namespace, job):
    return list(w for w in pod_client.list_namespaced_event(namespace).items if w.involved_object.name.startswith(job))


def exec_pod_command(namespace, pod_name, container_name, command):
    resp = stream(func=pod_client.connect_get_namespaced_pod_exec,
                  name=pod_name, namespace=namespace,
                  container=container_name,
                  command=['/bin/sh'], stdin=True, stdout=True,
                  stderr=True, tty=False,
                  _preload_content=False)
    return _exec_pod_command(resp=resp, command=command)


def _exec_pod_command(resp, command):
    if resp.is_open():
        logging.info('Running command... %s\n' % command)
        resp.write_stdin(command + '\n')
        while resp.is_open():
            resp.update(timeout=1)
            if resp.peek_stdout():
                return resp.read_stdout()
            if resp.peek_stderr():
                logging.info(resp.read_stderr())
                break
