"""Pure PySpark transformations for the LakeFlow medallion pipeline.

Everything here takes DataFrames and returns DataFrames -- no I/O, no
SparkSession globals -- so it runs unchanged in Databricks notebooks and in
local pytest (see tests/).
"""
