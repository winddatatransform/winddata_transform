# Transforming wind data

The windfarm ANH01 has turbines (wtgid) ANH01A01, ANH01A02, ...,  ANH01C03.

Every ten minutes a csv file is generated with columns: *id, timestamp, wtgid, siteid, tditag, value, insert_time*
See f.x. "ANH01_2020-02-20-13-10-00.csv"

Different sensors (*tditag*: Tag1, ..., Tag6) produce values of different types (string, integer, datetime, float).

The data scientists in the operations department require easy access to the time series produced by the sensors Tag3, Tag4 and Tag5.

You thus have to 
(1) filter 
and 
(2) transform/transpose the data to have columns: timestamp, siteid, wtgid, Tag3, Tag4, Tag5
where the tag columns contain the corresponding value with an appropriate datatype.

Use pyspark, pandas or pure python.

There is a file: file_handler.py containing classes, that help you read and write files.

Your task is to write the transform method in TransformJob in transform.py

-----
