import pytest
from pyspark.sql import SparkSession
from pysparkcategory.product_category import get_product_category_pairs

@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .appName("test_product_category")
        .master("local[2]")
        .getOrCreate()
    )
    #yield session
    #session.stop()

def test_get_product_category_pairs(spark):
    # Arrange
    products = spark.createDataFrame(
        [(1, "Laptop"), (2, "Banana"), (3, "Winnie the Pooh")],
        ["product_id", "product_name"]
    )
    categories = spark.createDataFrame(
        [(10, "Electronics"), (11, "Computers"), (12, "Fruits")],
        ["category_id", "category_name"]
    )
    product_category = spark.createDataFrame(
        [(1, 10), (1, 11), (2, 12)],
        ["product_id", "category_id"]
    )

    # Act
    result_df = get_product_category_pairs(products, categories, product_category)
    result = result_df.collect()
    #Преобразуем результат к множеству для сравнения с ответом
    result_set = {(row.product_name, row.category_name) for row in result} 

    # Assert
    expected = {
        ("Laptop", "Electronics"),
        ("Laptop", "Computers"),
        ("Banana", "Fruits"),
        ("Winnie the Pooh", None)
    }
    assert result_set == expected

