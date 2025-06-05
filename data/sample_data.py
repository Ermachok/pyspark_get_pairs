from pyspark.sql import DataFrame, SparkSession

from utils.logger import get_logger

logger = get_logger(__name__)


def create_sample_data(spark: SparkSession) -> tuple[DataFrame, DataFrame, DataFrame]:
    logger.info("Создание расширенного тестового датасета...")

    products = spark.createDataFrame(
        [
            (1, "Apple"),
            (2, "Banana"),
            (3, "Carrot"),
            (4, "Donut"),
            (5, "Eggplant"),
            (6, "Fig"),
            (7, "Grapes"),
            (8, "Honeydew"),
            (9, "Ice Cream"),
            (10, "Jelly"),
            (11, "Kiwi"),
            (12, "Lemon"),
            (13, "Mango"),
            (14, "Nectarine"),
            (15, "Orange"),
            (16, "Peach"),
            (17, "Quinoa"),
            (18, "Raspberry"),
            (19, "Strawberry"),
            (20, "Tomato"),
        ],
        ["product_id", "product_name"],
    )

    categories = spark.createDataFrame(
        [
            (10, "Fruit"),
            (20, "Vegetable"),
            (30, "Dessert"),
            (40, "Berry"),
            (50, "Frozen"),
            (60, "Grain"),
        ],
        ["category_id", "category_name"],
    )

    product_category = spark.createDataFrame(
        [
            (1, 10),  # Apple — Fruit
            (2, 10),  # Banana — Fruit
            (3, 20),  # Carrot — Vegetable
            (5, 20),  # Eggplant — Vegetable
            (6, 10),  # Fig — Fruit
            (7, 10),  # Grapes — Fruit
            (8, 10),  # Honeydew — Fruit
            (9, 30),  # Ice Cream — Dessert
            (10, 30),  # Jelly — Dessert
            (11, 10),
            (12, 10),
            (13, 10),
            (14, 10),
            (15, 10),
            (16, 10),
            (18, 40),  # Raspberry — Berry
            (19, 40),  # Strawberry — Berry
            (20, 20),  # Tomato — Vegetable
            (9, 50),  # Ice Cream — Frozen (multi-category)
            (17, 60),  # Quinoa — Grain
        ],
        ["product_id", "category_id"],
    )

    logger.info("Тестовый датасет успешно создан.")
    return products, categories, product_category
