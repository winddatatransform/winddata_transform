from abc import ABC, abstractmethod
from collections import namedtuple
from typing import List
import pandas as pd
from pandas import DataFrame as PandasDataFrame
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame as PysparkDataFrame


class InOutBase(ABC):
    """Abstract base class for read and write functionality."""

    @abstractmethod
    def read(self, path: str):
        pass

    @abstractmethod
    def write(self, data, path: str):
        pass


class PandasIO(InOutBase):
    """Reads and write csv files from/to pandas Dataframes."""

    def read(self, path) -> PandasDataFrame:
        return pd.read_csv(path)

    def write(self, data: PandasDataFrame, path: str):
        data.to_csv(path, index=False)


class SparkIO(InOutBase):
    """Reads and write csv files from/to pyspark Dataframes."""

    def __init__(self):
        self.spark = SparkSession.builder.getOrCreate()
        super().__init__()

    def read(self, path) -> PysparkDataFrame:
        return self.spark.read.format("csv").option("header", "true").load(path)

    def write(self, data: PysparkDataFrame, path: str):
        data.coalesce(1).write.mode("overwrite").format("csv").save(path, header="true")


class NamedTupleIO(InOutBase):
    """Reads and write csv files from/to named tuples, 
       where the header in the csv files corresponds to the field names."""

    def read(self, path) -> List[namedtuple]:
        with open(path, "r") as f:
            data = []
            for idx, l in enumerate(f):
                if idx == 0:
                    named_tuple = namedtuple("row", l)
                else:
                    l = l.rstrip()
                    if len(named_tuple._fields) == len(l.split(",")):
                        data.append(named_tuple(*l.split(",")))
        return data

    def write(self, path, data: List[namedtuple]):
        with open(path, "w") as f:
            f.write(",".join(data[0]._fields) + "\n")
            for row in data:
                f.write(",".join(row) + "\n")

