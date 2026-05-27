from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit

WAREHOUSE_PATH = "/home/ubuntu/lab8_lakehouse/warehouse"

REFERENCE_SOURCES = {
    "users": "/home/ubuntu/lab8_lakehouse/data/reference/users.jsonl",
    "test_users": "/home/ubuntu/lab8_lakehouse/data/reference/test_users.jsonl",
    "promo_codes": "/home/ubuntu/lab8_lakehouse/data/reference/promo_codes.jsonl",
}


def create_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("load-reference-to-iceberg")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", WAREHOUSE_PATH)
        .getOrCreate()
    )


def main() -> None:
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    spark.sql("CREATE DATABASE IF NOT EXISTS local.reference")

    for table_name, path in REFERENCE_SOURCES.items():
        print(f"\n=== Loading {table_name} from {path} ===")

        df = (
            spark.read
            .json(path)
            .withColumn("source_file", lit(path))
            .withColumn("loaded_at", current_timestamp())
        )

        print(f"\nSchema for {table_name}:")
        df.printSchema()

        row_count = df.count()
        print(f"Rows in {table_name}: {row_count}")

        target_table = f"local.reference.{table_name}"

        (
            df.writeTo(target_table)
            .using("iceberg")
            .createOrReplace()
        )

        print(f"Saved to Iceberg table: {target_table}")

    print("\n=== Tables in local.reference ===")
    spark.sql("SHOW TABLES IN local.reference").show(truncate=False)

    print("\n=== Counts ===")
    for table_name in REFERENCE_SOURCES:
        spark.sql(
            f"SELECT '{table_name}' AS table_name, COUNT(*) AS row_count "
            f"FROM local.reference.{table_name}"
        ).show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
