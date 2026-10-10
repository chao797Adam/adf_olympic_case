import dlt
from pyspark.sql.functions import col

@dlt.table(
    name="gold_athletes_enriched",
    comment="Wide table: athletes joined with nocs for country information"
)
def gold_athletes_enriched():
    athletes = spark.read.table("olympic.silver.athletes")
    nocs = spark.read.table("olympic.silver.nocs")

    return (
        athletes.alias("a")
            .join(
                nocs.alias("n"),
                col("a.country_code") == col("n.code"),
                "left"
            )
            .select(
                col("a.athlete_code"),
                col("a.name"),
                col("a.name_short"),
                col("a.gender"),
                col("a.function"),
                col("a.country_code"),
                col("a.country_long").alias("athlete_country_long"),
                col("n.country_long").alias("nocs_country_long"),   # from nocs name
                col("a.nationality"),
                col("a.nationality_long"),
                col("a.nationality_code"),
                col("a.height"),
                col("a.weight"),
                col("a.current")
            )
    )