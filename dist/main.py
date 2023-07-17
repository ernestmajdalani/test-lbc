import json
import argparse
import importlib
import glob
import os.path
from pyspark.sql import SparkSession

PIPELINE_COMPACT_GRAPH = "pipeline_compact_graph"
PIPELINE_GRANULAR_GRAPH = "pipeline_granular_graph"


def _parse_args():
    """
    Used to specify and parse arguments when calling the main.py script
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--job", required=True, choices=[PIPELINE_COMPACT_GRAPH, PIPELINE_GRANULAR_GRAPH], nargs="+",
                        help="Specifies which job pipeline to run")
    parser.add_argument("--adhoc", required=False, action="store_true", help="Set argument to call adhoc solutions")
    return parser.parse_args()


def _max_journal_unique_drug_count():
    """
    This function uses the `granular graph` output data to compute the journals with the most mentions of unique drugs

    Returns
    -------
        journal_max_unique_drugs: dict
            Journals with the most mentions of unique drugs
            Where key is a dated journal and value is the count of distinct drugs
            e.g. {(2020-01-01, psychopharmacology) : 2}
    """
    data_path = os.path.join("data", "output", "granular_graph", "part-*.json")
    data_files = glob.glob(data_path)
    journals_drugs = {}

    for file in data_files:
        with open(file, "r") as data_file:
            lines = data_file.readlines()

        for line in lines:
            parsed_line = json.loads(line)
            journal_name, journal_date = parsed_line["journal"], parsed_line["date"]
            dated_journal = (journal_name, journal_date)
            drug = parsed_line["source_id"]
            journals_drugs[dated_journal] = journals_drugs.get(dated_journal, [])

            if drug not in journals_drugs[dated_journal]:
                journals_drugs[dated_journal].append(drug)

    max_unique_drugs = max(len(drugs) for drugs in journals_drugs.values())
    journal_max_unique_drugs = [{journal: len(drugs)} for journal, drugs in journals_drugs.items() if len(drugs) == max_unique_drugs]

    return journal_max_unique_drugs


def _adhoc_granular():
    """
    Used to format the output for `_get_max_journal_unique_drug_count()`
    """
    journal_max_unique_drug = _max_journal_unique_drug_count()

    print('-' * 32)
    print("ADHOC EXERCISE ON GRANULAR GRAPH")
    print('-' * 32)
    print("The journals with the most mentions of distinct drugs are :")
    for journal in journal_max_unique_drug:
        print(journal)
    print()


def _adhoc_compact(spark: SparkSession):
    """
    This function uses the `compact graph` output data to compute the journals with the most mentions of unique drugs
    """
    import pyspark.sql.functions as F

    data_path = os.path.join("data", "output", "compact_graph", "part-*.json")
    spark.read.json(data_path)\
        .select("journal", "drug") \
        .withColumn("dated_journal", F.explode(F.col("journal"))) \
        .groupBy("dated_journal") \
        .agg(
            F.count_distinct(F.col("drug")).alias("drug_count"),
        ) \
        .orderBy(F.col("drug_count").desc())\
        .show(10, truncate=False)
    print('-' * 31)
    print("ADHOC EXERCISE ON COMPACT GRAPH")
    print('-' * 31)
    print("The above DataFrame displays the journals with the most mentions of distinct drugs.")
    print()


def main():
    args = _parse_args()

    with open("config.json", "r") as config_file:
        config = json.load(config_file)

    from pyspark.sql import SparkSession
    spark = SparkSession.builder \
        .master("local") \
        .appName(config.get("app_name")) \
        .getOrCreate()

    for job in args.job:
        job_module = importlib.import_module(f"jobs.{job}")
        job_module.run_job(spark, config)

    if args.adhoc and PIPELINE_COMPACT_GRAPH in args.job:
        _adhoc_compact(spark)

    if args.adhoc and PIPELINE_GRANULAR_GRAPH in args.job:
        _adhoc_granular()


if __name__ == "__main__":
    main()

