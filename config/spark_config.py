from pyspark.sql import SparkSession
from utils.logger import get_logger

logger = get_logger(__name__)

def init_spark(app_name: str = "ProductCategoryApp") -> SparkSession:
    logger.info("Инициализация SparkSession...")
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    logger.info("SparkSession успешно создан.")
    return spark
