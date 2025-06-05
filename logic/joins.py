from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from utils.logger import get_logger

logger = get_logger(__name__)

def get_product_category_pairs(
    products: DataFrame,
    categories: DataFrame,
    product_category: DataFrame
) -> DataFrame:
    logger.info("Формируем пары 'product_name – category_name'...")

    result = (
        products
        .join(product_category, on="product_id", how="inner")
        .join(categories, on="category_id", how="inner")
        .select("product_name", "category_name")
    )

    logger.info(f"Получено {result.count()} пар продукт-категория.")
    return result


def get_products_without_categories(
    products: DataFrame,
    product_category: DataFrame
) -> DataFrame:
    logger.info("Ищем продукты без категории...")

    result = (
        products
        .join(product_category, on="product_id", how="left")
        .filter(col("category_id").isNull())
        .select("product_name")
    )

    logger.info(f"Найдено {result.count()} продуктов без категории.")
    return result
