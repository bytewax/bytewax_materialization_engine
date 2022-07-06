from datetime import datetime, timedelta

from typing import List

from feast import FeatureStore, RepoConfig, FeatureView, BaseRegistry
from feast.infra.provider import _convert_arrow_to_proto
from feast.infra.offline_stores.offline_utils import get_offline_store_from_config

from kubernetes import client, config, utils

from tqdm import tqdm


class BytewaxMaterializationTask:
    def __init__(self, project, feature_view, start_date, end_date, tqdm):
        self.project = project
        self.feature_view = feature_view
        self.start_date = start_date
        self.end_date = end_date
        self.tqdm = tqdm


class BytewaxMaterializationEngine:
    def materialize_single_feature_view(
        self,
        config: RepoConfig,
        registry: BaseRegistry,
        tasks: List[BytewaxMaterializationTask],
    ):
        self.config = config
        self.registry = registry

        return self._materialize_task.map(tasks)

    def _materialize_task(task):
        config.load_kube_config()
        k8s_client = client.CoreV1Api()
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
                "name": "feature-store",
            },
            "data": {"feature_store.yaml": feature_store_configuration},
        }
        # TODO: Create or update
        k8s_client.patch_namespaced_config_map(
            name="feast",
            namespace="default",
            body=configmap_manifest,
        )
        yaml_file = "bytewax_dataflow.yaml"
        utils.create_from_yaml(k8s_client, yaml_file, verbose=True)
