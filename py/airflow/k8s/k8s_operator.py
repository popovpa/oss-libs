import hashlib
import logging
import os
from datetime import datetime as dt

import sys
import time
from airflow import AirflowException
from airflow.operators.bash_operator import BaseOperator
from airflow.utils.decorators import apply_defaults

import etl.tm.k8s_utils as utils
from etl.tm.k8s_spec import Spec

"""
 Класс оператора для запуска докер-образов на кубе
 На воркерах требуется наличие установленной переменной KUBECONFIG,
 которая ссылается на конфиг куба
"""


class K8SOperator(BaseOperator):
    template_fields = ('command', 'env', 'volumes')
    ui_color = '#3371e3'

    VOLUME_NAME = "airflow-k8s-volume"
    DATA_MOUNT_PATH = '/data'
    DOCKER_REGISTRY_HOST = os.getenv('DOCKER_REGISTRY_HOST', '')
    MAX_LEN = 63
    HASH_LEN = 20
    DELIMITER = ':'

    @apply_defaults
    def __init__(self,
                 image=None,
                 command=None,
                 env=None,
                 namespace=None,
                 memory='128Mi',
                 cpu='100m',
                 startup_timeout=60,
                 delete_success=True,
                 delete_fail=True,
                 backoff_limit=0,
                 active_deadline_seconds=86400,
                 image_pull_policy='Always',
                 image_pull_secret=None,
                 registry=None,
                 volume_mounts=None,
                 volumes=None,
                 no_mounts=False,
                 affinity=None,
                 *args,
                 **kwargs):
        super(K8SOperator, self).__init__(*args, **kwargs)

        self.image = image
        self.command = command
        self.env = env or {}
        self.namespace = os.getenv("ENVIRONMENT_NAME", namespace)
        self.memory = memory
        self.cpu = cpu
        self.backoff_limit = backoff_limit
        self.active_deadline_seconds = active_deadline_seconds
        self.image_pull_policy = os.getenv('K8S_PULL_POLICY', image_pull_policy)
        self.delete_success = delete_success
        self.delete_fail = delete_fail
        self.startup_timeout = startup_timeout
        self.volume_mounts = volume_mounts
        self.volumes = volumes
        self.no_mounts = no_mounts
        self.image_pull_secret = os.getenv('K8S_IMAGE_PULL_SECRET', image_pull_secret)
        self.registry = registry
        self.affinity = affinity

        logging.basicConfig(stream=sys.stdout, level=logging.INFO)
        logging.captureWarnings(True)

    def execute(self, context):
        try:

            task_id = context.get('task_instance').task_id
            dag_id = context.get('task_instance').dag_id
            exec_date = context['execution_date']
            date_str = str(exec_date.format('%Y%m%d%H%M%S'))

            short_dag_id = dag_id[0:self.MAX_LEN]
            short_task_id = task_id[0:self.MAX_LEN]

            container_name = "{dag}-{task}".format(
                dag=short_dag_id,
                task=short_task_id
            ).replace("_", "-").replace(".", "-")

            job_name = self.simple_build_job_name(dag_id, task_id, date_str)
            context['job_name'] = job_name

            labels = {
                "DAG": short_dag_id,
                "Task": short_task_id
            }

            if self.registry is None:
                registry = self.DOCKER_REGISTRY_HOST
            else:
                registry = self.registry

            image = os.path.join(registry, self.image)

            if not self.no_mounts:

                if self.volume_mounts is None or not isinstance(self.volume_mounts, list):
                    self.volume_mounts = []

                self.volume_mounts.append({
                    "name": self.VOLUME_NAME,
                    "mountPath": self.DATA_MOUNT_PATH
                })

                self.volume_mounts.append({
                    'name': 'yandex-ca',
                    'readOnly': True,
                    'mountPath': '/opt/certs/clickhouse'
                })

                if self.volumes is None or not isinstance(self.volumes, list):
                    self.volumes = []

                vol = None
                data_vol_mode = os.getenv('K8S_DATA_MODE', 'Local')
                if data_vol_mode == 'Distributed':
                    vol = {
                        'name': self.VOLUME_NAME,
                        'persistentVolumeClaim': {
                            'claimName': os.getenv('K8S_DATA_PVC')
                        }
                    }
                elif data_vol_mode == 'Local':
                    vol = {
                        'name': self.VOLUME_NAME,
                        'hostPath': {
                            'path': os.getenv('K8S_LOCAL_DATA_DIR'),
                            'type': 'DirectoryOrCreate'
                        }
                    }

                self.volumes.append({
                    'name': 'yandex-ca',
                    'secret': {
                        'secretName': 'yandex-ca'
                    }
                })
                self.volumes.append(vol)

            self.env.update({'EXECUTION_DATE': str(exec_date)})

            spec = Spec(
                name=job_name,
                namespace=self.namespace,
                image=image,
                container_name=container_name,
                command=self.command,
                env=self.env,
                backoff_limit=self.backoff_limit,
                image_pull_policy=self.image_pull_policy,
                image_pull_secret=self.image_pull_secret,
                active_deadline_seconds=self.active_deadline_seconds,
                memory=self.memory,
                cpu=self.cpu,
                volume_mounts=self.volume_mounts,
                labels=labels,
                volumes=self.volumes,
                affinity=self.affinity
            ).get()

            logging.info(">>> Checking for existing job '{}'".format(job_name))
            if utils.has_job(namespace=self.namespace, name=job_name):
                logging.info(">>> Remove old job '{}'".format(job_name))
                utils.job_cleanup(namespace=self.namespace, name=job_name)

                logging.info(">>> Remove old/orphan pods of job '{}'".format(job_name))
                utils.orphan_pods_cleanup(namespace=self.namespace, job_name=job_name)

            if not utils.has_job(namespace=self.namespace, name=job_name):
                logging.info(">>> Creating job '{}'".format(job_name))
                utils.create_job(namespace=self.namespace, spec=spec)

            # Подчищаем все старые джобы для текущей материализации, запускаем ее и ждем успешного запуска
            utils.wait_job_starting(namespace=self.namespace, name=job_name)
            logging.info(">>> Job is started: {}".format(job_name))

            is_job_failed = False

            job_status = utils.job_status(namespace=self.namespace, name=job_name)
            was_awaiting = False
            # Ждем пока вся джоба не закончится
            while utils.job_is_running(namespace=self.namespace, name=job_name):
                was_awaiting = True
                if is_job_failed:
                    logging.info(">>> Restarting pod for job '{}'".format(job_name))
                    is_job_failed = False

                logging.info(">>> Waiting while pod starting...")

                curr_time = dt.now()
                # В джобе всегда только 1 активный POD, ждем его создания и запуска
                succeeded_pods = None
                running_pods = None
                while (running_pods is None or len(running_pods) == 0) and (succeeded_pods is None or len(succeeded_pods) == 0):
                    all_pods = utils.get_pods(namespace=self.namespace, job_name=job_name)

                    succeeded_pods = list(pod for pod in all_pods.items if pod.status.phase == 'Succeeded')
                    running_pods = list(pod for pod in all_pods.items if pod.status.phase == 'Running')
                    err_pods = list(pod for pod in all_pods.items if pod.status.phase == 'Error')
                    failed_pods = list(pod for pod in all_pods.items if pod.status.phase == 'Failed')

                    if (failed_pods is not None and len(failed_pods) > 0) or (err_pods is not None and len(err_pods) > 0):
                        pod_name = all_pods.items[-1].metadata.name
                        self.print_logs(namespace=self.namespace, pod=pod_name, container=container_name)

                        message = "Pod failed!"
                        message += "\nStatus pod: "
                        message += "\n".join(list(str(item.status) for item in all_pods.items))
                        raise Exception(message)

                    delta = dt.now() - curr_time
                    if delta.seconds > 0 and delta.seconds % 5 == 0:
                        logging.info(">>> passed {} seconds...".format(delta.seconds))

                    # Ждем время startup_timeout и грохаем его
                    if delta.seconds >= self.startup_timeout:
                        message = "Pod so long to starting..."
                        if all_pods.items is not None and len(all_pods.items) > 0:
                            message += "\nStatus pod: "
                            message += "\n".join(list(str(item.status) for item in all_pods.items))
                        raise Exception(message)
                    time.sleep(1)

                active_pod = None
                if succeeded_pods is not None and len(succeeded_pods) > 0:
                    active_pod = succeeded_pods[-1].metadata.name
                    print(">>>Job was faster than API interactions! That's OK")
                else:
                    active_pod = running_pods[-1].metadata.name

                logging.info(">>> New pod '{} created'".format(active_pod))
                logging.info(">>> Listening logs of pod '{}':".format(active_pod))
                # Читаем логи основного контейнера
                self.print_logs(namespace=self.namespace, pod=active_pod, container=container_name)

                # После чтения логов, надо подождать пока основной контейнер не закончит работу
                while utils.container_is_running(namespace=self.namespace,
                                                 pod_name=active_pod,
                                                 container_name=container_name):
                    logging.info(">>> Container '{}' still running...".format(container_name))
                    time.sleep(1)

                # Подождем пока POD не завершится
                pod_status = utils.get_pod_status(namespace=self.namespace, name=active_pod)
                while pod_status is None or pod_status.phase == 'Running':
                    logging.info(">>> Wait while POD {} is running...".format(active_pod))
                    pod_status = utils.get_pod_status(namespace=self.namespace, name=active_pod)
                    time.sleep(1)

                # Pod завершился, теперь проверим что завершилась евонная Job
                js = utils.job_status(namespace=self.namespace, name=job_name)
                while js is not None and js.active is not None and js.active > 0:
                    js = utils.job_status(namespace=self.namespace, name=job_name)
                    logging.info(">>> Wait while JOB {} is running...".format(job_name))
                    time.sleep(1)

                # Проверим статус POD'а, если он не Succeeded, то рестартанем его снова
                pod_status = utils.get_pod_status(namespace=self.namespace, name=active_pod)
                if pod_status is not None and pod_status.phase != 'Succeeded':
                    is_job_failed = True
                    status = str(pod_status.container_statuses)
                    logging.error(">>> Pod failed: '{}'".format(active_pod))
                    logging.error(">>> Status: '{}'".format(status))

            if job_status is not None and job_status.succeeded is not None and not was_awaiting:
                logging.info(">>> Job was pretty fast: {}".format(job_name))
                succeeds = utils.get_succeeded_pods(namespace=self.namespace, job_name=job_name)
                succeed_pod = succeeds.items[-1].metadata.name

                self.print_logs(namespace=self.namespace, pod=succeed_pod, container=container_name)
                logging.info(">>> END")

            if not is_job_failed:
                logging.info(">>> SUCCESSFUL: '{}' ☺".format(job_name))
            else:
                logging.info(">>> FAILED: '{}' ☹".format(job_name))
                raise AirflowException("Job '{}' error".format(job_name))

            context['job_failed'] = is_job_failed
        except Exception as ex:
            logging.info(">>> Job failed! Let's remove it! Job name: {}".format(job_name))
            self.remove_job(self.namespace, job_name)
            raise Exception('Job Launching failed: {error}'.format(error=ex))

    def post_execute(self, context, result=None):
        job_failed = context['job_failed']
        job_name = context['job_name']
        if (self.delete_success and not job_failed) or (self.delete_fail and job_failed):
            logging.info(">>> Cleanup job '{}'".format(job_name))
            self.remove_job(self.namespace, job_name)

    def remove_job(self, namespace, name):
        utils.job_cleanup(namespace=namespace, name=name)
        utils.orphan_pods_cleanup(namespace=namespace, job_name=name)

    def on_kill(self):
        logging.info(">>>KILL")

    def restrict_string(self, string, length=75, suffix='...'):
        return (str(string)[:length] + (str(string)[length:] and suffix)).replace('\n', '').replace('\r', '')

    def print_logs(self, namespace, pod, container):
        try:
            logs_resp = utils.get_log(namespace=namespace, pod_name=pod, container_name=container, timeout=self.active_deadline_seconds)
            for log_bytes in logs_resp:
                logging.info(str(log_bytes, 'utf-8').rstrip('\n\r'))
        except Exception as ex:
            logging.error(str(ex, 'utf-8'))
            logging.info(">>> Log print retry...")
            time.sleep(1)
            self.print_logs(namespace, pod, container)

    def name_strict(self, name, max_len):
        tokens = list(s for s in name.split('-') if s is not '')
        res = []
        while len(res) != len(tokens) and len(''.join(res)) + len(res) + len(tokens[len(res)]) < max_len:
            res.append(tokens[len(res)])

        if len(res) == 0:
            res.append(tokens[0][0:max_len])
        return '-'.join(res)

    def simple_build_job_name(self, dag_id, task_id, date):
        prefix = 'air'

        raw = self.DELIMITER.join([dag_id, task_id, date])
        hashed = hashlib.md5(raw.encode('utf-8')).hexdigest()
        return '{}-{}'.format(prefix, hashed[0:self.HASH_LEN])

    def build_job_name(self, dag_id, task_id, date):
        prefix = 'air'

        raw = self.DELIMITER.join([dag_id, task_id, date])
        hashed = hashlib.md5(raw.encode('utf-8')).hexdigest()
        hash_cut = hashed[0:self.HASH_LEN]

        remain_len = self.MAX_LEN - self.HASH_LEN - len(prefix) - 4
        partial_len = remain_len // 2

        dag_id = self.name_strict(dag_id.replace('_', '-'), partial_len)
        task_id = ''  # self.name_strict(task_id, partial_len)

        return '-'.join((s for s in [prefix, dag_id, task_id, hash_cut] if s is not ''))
