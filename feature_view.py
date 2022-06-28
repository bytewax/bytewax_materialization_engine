# Feature definition file. Establishes the shape of the feature view for
# storage.
from datetime import datetime, timedelta

from feast import Entity, Feature, FeatureView, FileSource, ValueType
from feast.field import Field
from feast.types import Float32, Int32

# Read data from parquet files. Parquet is convenient for local development mode. For
# production, you can use your favorite DWH, such as BigQuery. See Feast documentation
# for more info.
driver_hourly_stats = FileSource(
    path="data/driver_stats.parquet",
    timestamp_field="event_timestamp",
    created_timestamp_column="created",
)

# Our parquet files contain sample data that includes a driver_id column, timestamps and
# three feature columns. Here we define a Feature View that will allow us to serve this
# data to our model online.
driver = Entity(name="driver", join_keys=["driver_id"])
driver_stats_feature_view = FeatureView(
    name="driver_stats",
    entities=[driver],
    ttl=timedelta(hours=2),
    schema=[
        Field(name="conv_rate", dtype=Float32),
        Field(name="acc_rate", dtype=Float32),
        Field(name="avg_daily_trips", dtype=Int32),
    ],
    source=driver_hourly_stats,
)
