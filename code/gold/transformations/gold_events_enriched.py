@dlt.table(
    name="gold_events_enriched",
    comment="Events with sport and tag information"
)
def gold_events_enriched():
    return (
        spark.read.table("olympic.silver.events")
             .select(
                 "event",
                 "tag",
                 "sport",
                 "sport_code",
                 "sport_url"
             )
    )