from datetime import datetime, timedelta

import s3fs
import pyarrow.parquet as pq

from typing import Callable, Any, Iterable

from bytewax import cluster_main, Dataflow, inputs, AdvanceTo, Emit

from feast import FeatureStore, RepoConfig, FeatureView
from feast.infra.provider import _convert_arrow_to_proto
from feast.infra.offline_stores.offline_utils import get_offline_store_from_config

from tqdm import tqdm


def distribute(elements: Iterable[Any], index: int, count: int) -> Iterable[Any]:
    assert index < count, f"Highest index should only be {count - 1}; got {index}"
    for i, x in enumerate(elements):
        if i % count == index:
            yield x


class BytewaxMaterializationEngine:
    def materialize_single_feature_view(
        self,
        config: RepoConfig,
        feature_view: FeatureView,
        start_date: datetime,
        end_date: datetime,
        registry: Any,
        project: str,
        tqdm_builder: Callable[[int], tqdm],
    ):
        self.feature_store = FeatureStore(config=config)
        self.repo_config = config
        self.offline_store = config.offline_store
        self.feature_view = feature_view
        self.registry = registry
        self.offline_store = get_offline_store_from_config(
            self.repo_config.offline_store
        )
        self.tqdm_builder = tqdm_builder
        # TODO: set bytewax config when available
        # self.bytewax_config = config.batch_engine_config

        # TODO: Run the offline store materialization job
        # job = self.offline_store.pull_latest_from_table_or_query(
        # TODO: Params here?
        # )
        # self.paths = job.to_remote_storage()
        self.paths = [
            "s3://bytewax-demo-feast/driver_stats.parquet",
            "s3://bytewax-demo-feast/driver_stats2.parquet",
        ]

    def process_path(self, path):
        fs = s3fs.S3FileSystem()
        dataset = pq.ParquetDataset(path, filesystem=fs, use_legacy_dataset=False)
        batches = []
        for fragment in dataset.fragments:
            for batch in fragment.to_table().to_batches():
                batches.append(batch)

        return batches

    def input_builder(self, worker_index, worker_count):
        worker_paths = list(distribute(self.paths, worker_index, worker_count))
        epoch = 0
        for path in worker_paths:
            yield AdvanceTo(epoch)
            yield Emit(path)
            epoch += 1

        return

    def output_builder(self, worker_index, worker_count):
        def output_fn(epoch_batch):
            _, batch = epoch_batch
            join_keys = {}
            for entity_name in self.feature_view.entities:
                entity = self.feature_store.get_entity(entity_name)
                join_keys[entity.join_key] = entity.value_type

            rows_to_write = _convert_arrow_to_proto(batch, self.feature_view, join_keys)
            provider = self.feature_store._get_provider()
            with self.tqdm_builder(total=len(rows_to_write)) as progress:
                provider.online_write_batch(
                    config=store.config,
                    table=self.feature_view,
                    data=rows_to_write,
                    progress=progress.update,
                )

        return output_fn

    def run_dataflow(self):
        flow = Dataflow()
        flow.flat_map(self.process_path)
        flow.capture()
        cluster_main(
            flow,
            self.input_builder,
            self.output_builder,
            [],  # TODO: self.batch_engine_config.addresses
            0,  # TODO: set process id
            2,
        )


if __name__ == "__main__":
    store = FeatureStore(repo_path=".")
    bytewax_engine = BytewaxMaterializationEngine()
    bytewax_engine.materialize_single_feature_view(
        store.config,
        store.get_feature_view("driver_stats"),
        timedelta(days=-1),
        datetime.now(),
        [],
        "driver_project",
        tqdm,
    )
    bytewax_engine.run_dataflow()
