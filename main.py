from config.spark_config import init_spark
from data.sample_data import create_sample_data
from logic.joins import (get_product_category_pairs,
                         get_products_without_categories)
from utils.logger import get_logger

logger = get_logger(__name__)


def main() -> None:
    spark = init_spark()

    products, categories, product_category = create_sample_data(spark)

    pairs_df = get_product_category_pairs(products, categories, product_category)
    no_category_df = get_products_without_categories(products, product_category)

    logger.info("=== Пары продукт – категория ===")
    pairs_df.show()

    logger.info("=== Продукты без категории ===")
    no_category_df.show()

    spark.stop()
    logger.info("Spark остановлен. Программа завершена.")


if __name__ == "__main__":
    main()
