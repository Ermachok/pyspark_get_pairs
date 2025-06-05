from pyspark.sql import SparkSession, DataFrame
from utils.logger import get_logger

logger = get_logger(__name__)

def create_sample_data(spark: SparkSession) -> tuple[DataFrame, DataFrame, DataFrame]:
    logger.info("Создание тестовых данных...")

    products = spark.createDataFrame([
        (1, "Apple"),
        (2, "Banana"),
        (3, "Carrot"),
        (4, "Donut"),
    ], ["product_id", "product_name"])

    categories = spark.createDataFrame([
        (10, "Fruit"),
        (20, "Vegetable"),
        (30, "Dessert"),
    ], ["category_id", "category_name"])

    product_category = spark.createDataFrame([
        (1, 10),
        (2, 10),
        (3, 20),
    ], ["product_id", "category_id"])

    logger.info("Тестовые данные успешно созданы.")
    return products, categories, product_category
