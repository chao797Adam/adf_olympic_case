@dlt.table(
    name="gold_coaches_enriched",
    comment="Wide table: coaches joined with nocs for country information"
)
def gold_coaches_enriched():
    coaches = spark.read.table("olympic.silver.coaches")
    nocs = spark.read.table("olympic.silver.nocs")

    return (
        coaches.alias("c")
            .join(
                nocs.alias("n"),
                col("c.country_code") == col("n.code"),
                "left"
            )
            .select(
                col("c.code").alias("coach_code"),
                col("c.name"),
                col("c.gender"),
                col("c.function"),
                col("c.category"),
                col("c.country_code"),
                col("c.country_long").alias("coach_country_long"),
                col("n.country_long").alias("nocs_country_long"),
                col("c.disciplines"),
                col("c.events"),
                col("c.birth_date"),
                col("c.current")
            )
    )