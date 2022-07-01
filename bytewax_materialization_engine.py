from datetime import datetime, timedelta

from typing import List

from feast import FeatureStore, RepoConfig, FeatureView, BaseRegistry
from feast.infra.provider import _convert_arrow_to_proto
from feast.infra.offline_stores.offline_utils import get_offline_store_from_config

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
        self.feature_store = FeatureStore(config=config)
        self.repo_config = config
        self.offline_store = config.offline_store
        self.registry = registry
        self.offline_store = get_offline_store_from_config(
            self.repo_config.offline_store
        )

        return self._materialize_task.map(tasks)

    def _materialize_task(task):
        """Task -> Job"""
        pass
