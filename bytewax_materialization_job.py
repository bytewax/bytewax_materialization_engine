import s3fs
import pyarrow.parquet as pq
import yaml

from datetime import datetime, timedelta

from typing import List, Tuple, Optional, Iterable, Any

from bytewax import cluster_main, Dataflow, inputs, AdvanceTo, Emit
from bytewax.parse import proc_env

from feast import Entity, FeatureView, RepoConfig
from feast.feature_view import DUMMY_ENTITY_ID
from feast.infra.provider import _convert_arrow_to_proto

from tqdm import tqdm


class BytewaxMaterializationJob:
    def __init__(
        self,
        config: RepoConfig,
        feature_view: FeatureView,
        start_date: datetime,
        end_date: datetime,
        project: str,
    ):
        self.start_date = start_date
        self.feature_store = FeatureStore(config=config)
        self.config = config
        self.feature_view = feature_view
        self.start_date = start_date
        self.end_date = end_date
        self.project = project
        self.offline_store = feature_view.source

        # Code from https://github.com/feast-dev/feast/pull/2901
        entities = []
        for entity_name in feature_view.entities:
            entities.append(self.feature_store.get_entity(entity_name, project))

        (
            join_key_columns,
            feature_name_columns,
            timestamp_field,
            created_timestamp_column,
        ) = self._get_column_names(feature_view, entities)

        # job = self.offline_store.pull_latest_from_table_or_query(
        #     config=self.config,
        #     data_source=feature_view.batch_source,
        #     join_key_columns=join_key_columns,
        #     feature_name_columns=feature_name_columns,
        #     timestamp_field=timestamp_field,
        #     created_timestamp_column=created_timestamp_column,
        #     start_date=start_date,
        #     end_date=end_date,
        # )

        # self.paths = job.to_remote_storage()
        # TODO: Using static paths until I have an online store
        # that supports `pull_latest_from_table_or_query`
        self.paths = [feature_view.batch_source.path]
        self.run_dataflow()

    def _get_column_names(
        self, feature_view: FeatureView, entities: List[Entity]
    ) -> Tuple[List[str], List[str], str, Optional[str]]:
        """
        From https://github.com/feast-dev/feast/pull/2901
        If a field mapping exists, run it in reverse on the join keys,
        feature names, event timestamp column, and created timestamp column
        to get the names of the relevant columns in the offline feature store table.
        Returns:
            Tuple containing the list of reverse-mapped join_keys,
            reverse-mapped feature names, reverse-mapped event timestamp column,
            and reverse-mapped created timestamp column that will be passed into
            the query to the offline store.
        """
        # if we have mapped fields, use the original field names in the call to the offline store
        timestamp_field = feature_view.batch_source.timestamp_field
        feature_names = [feature.name for feature in feature_view.features]
        created_timestamp_column = feature_view.batch_source.created_timestamp_column
        join_keys = [
            entity.join_key for entity in entities if entity.join_key != DUMMY_ENTITY_ID
        ]
        if feature_view.batch_source.field_mapping is not None:
            reverse_field_mapping = {
                v: k for k, v in feature_view.batch_source.field_mapping.items()
            }
            timestamp_field = (
                reverse_field_mapping[timestamp_field]
                if timestamp_field in reverse_field_mapping.keys()
                else timestamp_field
            )
            created_timestamp_column = (
                reverse_field_mapping[created_timestamp_column]
                if created_timestamp_column
                and created_timestamp_column in reverse_field_mapping.keys()
                else created_timestamp_column
            )
            join_keys = [
                reverse_field_mapping[col]
                if col in reverse_field_mapping.keys()
                else col
                for col in join_keys
            ]
            feature_names = [
                reverse_field_mapping[col]
                if col in reverse_field_mapping.keys()
                else col
                for col in feature_names
            ]

        # We need to exclude join keys and timestamp columns from the list of features,
        # after they are mapped to their final column names via the `field_mapping`
        # field of the source.
        feature_names = [
            name
            for name in feature_names
            if name not in join_keys
            and name != timestamp_field
            and name != created_timestamp_column
        ]
        return (
            join_keys,
            feature_names,
            timestamp_field,
            created_timestamp_column,
        )

    def process_path(self, path):
        print(path)
        fs = s3fs.S3FileSystem()
        dataset = pq.ParquetDataset(path, filesystem=fs, use_legacy_dataset=False)
        batches = []
        for fragment in dataset.fragments:
            for batch in fragment.to_table().to_batches():
                batches.append(batch)

        return batches

    def input_builder(self, worker_index, worker_count):
        epoch = 0
        for path in self.paths:
            yield AdvanceTo(epoch)
            yield Emit(path)
            epoch += 1

        return

    def output_builder(self, worker_index, worker_count):
        def output_fn(epoch_batch):
            _, batch = epoch_batch
            join_keys = {
                entity.name: entity.dtype.to_value_type()
                for entity in self.feature_view.entity_columns
            }

            rows_to_write = _convert_arrow_to_proto(batch, self.feature_view, join_keys)
            provider = self.feature_store._get_provider()
            with tqdm(total=len(rows_to_write)) as progress:
                provider.online_write_batch(
                    config=self.config,
                    table=self.feature_view,
                    data=rows_to_write,
                    progress=progress.update,
                )

        return output_fn

    def run_dataflow(self):
        flow = Dataflow()
        flow.flat_map(self.process_path)
        flow.capture()
        cluster_main(flow, self.input_builder, self.output_builder, **proc_env())
        # cluster_main(flow, self.input_builder, self.output_builder, [], 0, 1)
