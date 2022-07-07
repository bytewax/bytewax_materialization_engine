import yaml

from feast import FeatureStore, RepoConfig
from datetime import datetime, timedelta

from bytewax_materialization_dataflow import BytewaxMaterializationDataflow

if __name__ == "__main__":
    with open("/var/feast/feature_store.yaml") as f:
        yaml_config = yaml.safe_load(f)

        config = RepoConfig(**yaml_config)
        store = FeatureStore(config=config)

        job = BytewaxMaterializationDataflow(
            store.config,
            store.get_feature_view("driver_stats"),
            timedelta(days=-1),
            datetime.now(),
            "drivers",
        )
