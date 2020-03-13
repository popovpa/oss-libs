import yaml


class Spec:

    def __init__(self,
                 name=None,
                 namespace=None,
                 image=None,
                 container_name=None,
                 command=None,
                 env=None,
                 backoff_limit=0,
                 image_pull_policy='IfNotPresent',
                 image_pull_secret=None,
                 active_deadline_seconds=3600,
                 memory='128Mi',
                 cpu='100m',
                 volume_mounts=None,
                 volumes=None,
                 labels=None,
                 affinity=None
                 ):
        self.name = name
        self.namespace = namespace
        self.image = image
        self.container_name = container_name
        self.command = command
        self.env = env or {}
        self.backoff_limit = backoff_limit
        self.active_deadline_seconds = active_deadline_seconds
        self.image_pull_policy = image_pull_policy
        self.memory = memory
        self.cpu = cpu
        self.volume_mounts = volume_mounts or {}
        self.volumes = volumes or {}
        self.labels = labels or {}
        self.image_pull_secret = image_pull_secret
        self.affinity = affinity

    def get(self):
        spec = {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {
                "name": self.name,
                "namespace": self.namespace,
                "labels": self.labels
            },
            "spec": {
                "parallelism": 1,
                "replicas": 1,
                "backoffLimit": self.backoff_limit,
                "activeDeadlineSeconds": self.active_deadline_seconds,
                "template": {
                    "spec": {
                        "restartPolicy": "Never",
                        "containers": [
                            {
                                "name": self.container_name,
                                "resources": {
                                    "requests": {
                                        "memory": self.memory,
                                        "cpu": self.cpu
                                    },
                                    "limits": {
                                        "memory": self.memory,
                                        "cpu": self.cpu
                                    }
                                },
                                "imagePullPolicy": self.image_pull_policy,
                                "image": self.image,
                                "command": self.command
                            }]
                    }
                }
            }
        }

        if (len(self.env)) > 0:
            env = []
            for k in self.env.keys():
                env.append({'name': k, 'value': self.env[k]})
            spec['spec']['template']['spec']['containers'][0]['env'] = env

        if (len(self.volume_mounts)) > 0:
            spec['spec']['template']['spec']['containers'][0]['volumeMounts'] = self.volume_mounts

        if (len(self.volumes)) > 0:
            spec['spec']['template']['spec']['volumes'] = self.volumes

        if self.image_pull_secret is not None:
            spec['spec']['template']['spec']['imagePullSecrets'] = [{'name': self.image_pull_secret}]

        if self.affinity is not None:
            spec['spec']['template']['spec']['affinity'] = self.affinity

        return spec

    def to_yaml(self):
        return yaml.dump(self.get())
