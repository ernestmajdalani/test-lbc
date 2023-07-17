import os
import re
from datetime import datetime
from pyspark.sql import DataFrame, SparkSession, Column
import pyspark.sql.functions as F
from pyspark.sql.types import StringType


class UnrecognizedDateFormatError(Exception):
    pass


def _standardize_date(date_str: str) -> str:
    """
    Function that standardizes dates to yyyy-MM-dd from identified formats in the data files. Formats that are not
    present in identified_date_formats are considered unidentified and will raise an UnrecognizedDateFormatError.

    Parameters
    ----------
        date_str: str
            The date string to parse
    Returns
    -------
        str:
            The formatted date string yyyy-MM-dd
    """
    standard_format = "%Y-%m-%d"
    identified_date_formats = [standard_format, "%d %B %Y", "%d/%m/%Y"]

    for date_fmt in identified_date_formats:
        try:
            date_obj = datetime.strptime(date_str, date_fmt).strftime(standard_format)
            return date_obj

        except ValueError:
            pass
    raise UnrecognizedDateFormatError


# UDF used to parse date columns
get_std_date_udf = F.udf(_standardize_date, StringType())


def read_data(spark: SparkSession, config: dict, type_medical_file: str) -> DataFrame:
    """
    Reads a specified medical file type (pubmed, clinical trial) in JSON or CSV format and returns a single concatenated
    DataFrame. Calls the respective `spark.read` function based on the identified input data file formats: JSON or CSV.

    Parameters
    ----------
        spark: SparkSession
                Entry point to spark API
        config: dict
                Contains project level configurations
        type_medical_file: str
                Specifies the respective medical file : pubmed or clinical trials

    Returns
    --------
        DataFrame: pyspark.sql.DataFrame
                Returns the concatenated DataFrame of a given medical file type from a JSON or CSV format
    """
    pattern = rf"^{type_medical_file}\.(csv|json)$"
    files = os.listdir(f"{config.get('data_input_path')}")
    files = list(filter(lambda x: re.match(pattern, x), files))

    combined_df = None

    for file in files:
        file_path = os.path.join(config.get('data_input_path'), file)
        file_format = os.path.splitext(file_path)[-1][1:]

        try:
            temp_df = None
            if file_format == "csv":
                temp_df = spark.read \
                    .option("header", "true") \
                    .option("inferSchema", "true") \
                    .csv(file_path)
            elif file_format == "json":
                temp_df = spark.read \
                    .option("multiline", "true") \
                    .option("inferSchema", "true") \
                    .json(file_path)
            else:
                pass

            if combined_df is None:
                combined_df = temp_df
            else:
                combined_df = combined_df.unionByName(temp_df)

        except Exception as e:
            raise e

    return combined_df


def clean_hex(col_name: str) -> Column:
    """
    Used to clean hexadecimal characters in the specified column.

    Parameters
    ----------
        col_name: str
            The column name on which to apply

    Returns
    -------
        Column : pyspark.sql.Column
            String without hex characters as specified by `_re_clean_hex`

    """
    _re_clean_hex = r"\\x[0-9a-fA-F]+"
    return F.regexp_replace(F.col(col_name), _re_clean_hex, "")


def clean_whitespace(col_name: str) -> Column:
    """
    Used to remove extraneous whitespace characters in the specified column.

    Parameters
    ----------
        col_name: str
            The column name on which to apply `_re_clean_whitespace`

    Returns
    -------
        Column : pyspark.sql.Column
            String without extraneous whitespace characters as specified by `_re_clean_whitespace`

    """
    _re_clean_whitespace = r"\s{2,}"
    return F.regexp_replace(F.col(col_name), _re_clean_whitespace, " ")
