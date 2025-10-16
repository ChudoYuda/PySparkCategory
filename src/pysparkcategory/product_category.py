from pyspark.sql import DataFrame

def get_product_category_pairs(
    products: DataFrame,
    categories: DataFrame,
    product_category: DataFrame
) -> DataFrame:
    """
    Выдаёт пары (product_name, category_name),
    если у продукта нет категории, то - (product_name, null).
    
    Args:
        products: DataFrame с колонками ['product_id', 'product_name']
        categories: DataFrame с колонками ['category_id', 'category_name']
        product_category: DataFrame с колонками ['product_id', 'category_id']
    """
    return (
        products
        .join(product_category, on="product_id", how="left")
        .join(categories, on="category_id", how="left")
        .select("product_name", "category_name")
    )