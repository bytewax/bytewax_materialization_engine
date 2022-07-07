import yaml

import kubernetes
import kubernetes.client

from kubernetes import utils, config, client

from datetime import datetime, timedelta

from typing import List

from feast import FeatureStore, RepoConfig
from feast.registry import BaseRegistry

from tqdm import tqdm


class BytewaxMaterializationTask:
    def __init__(self, project, feature_view, start_date, end_date, tqdm):
        self.project = project
        self.feature_view = feature_view
        self.start_date = start_date
        self.end_date = end_date
        self.tqdm = tqdm


class BytewaxMaterializationJob:
    def __init__(self, task: BytewaxMaterializationTask):
        self.task = task
        print(f"new job, task is {task}")
        self._materialize()

    def status(self):
        pass

    def should_be_retried(self):
        pass

    def job_id(self):
        pass

    def url(self):
        pass

    def _materialize(self):
        config.load_kube_config()
        v1 = client.CoreV1Api()
        # TODO: Maybe this should be a method on the RepoConfig object?
        # Taken from https://github.com/feast-dev/feast/blob/master/sdk/python/feast/repo_config.py#L342
        feature_store_configuration = yaml.dump(
            yaml.safe_load(
                store.config.json(
                    exclude={"repo_path"},
                    exclude_unset=True,
                )
            )
        )

        configmap_manifest = {
            "kind": "ConfigMap",
            "apiVersion": "v1",
            "metadata": {
                "name": "feast",
            },
            "data": {"feature_store.yaml": feature_store_configuration},
        }
        # TODO: Create or update
        v1.patch_namespaced_config_map(
            name="feast",
            namespace="default",
            body=configmap_manifest,
        )

        k8s_client = client.api_client.ApiClient()
        yaml_file = "bytewax_dataflow.yaml"
        utils.create_from_yaml(k8s_client, yaml_file, verbose=True)


class BytewaxMaterializationEngine:
    def materialize_single_feature_view(
        self,
        config: RepoConfig,
        registry: BaseRegistry,
        tasks: List[BytewaxMaterializationTask],
    ):
        self.config = config
        self.registry = registry

        return [BytewaxMaterializationJob(task) for task in tasks]


if __name__ == "__main__":
    store = FeatureStore(".")
    task = BytewaxMaterializationTask(
        "drivers",
        store.get_feature_view("driver_stats"),
        timedelta(days=-1),
        datetime.now(),
        tqdm,
    )

    engine = BytewaxMaterializationEngine()
    tasks = engine.materialize_single_feature_view(store.config, store, [task])
