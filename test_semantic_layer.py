import json
from semantic_layer import generate_sql
from run_sql import query_bigquery 

def test_query(query_json: str, semantic_layer_json: str, description: str, run_query: bool = False):
    """
    Test a semantic layer query and optionally execute it
    
    Args:
        query_json: Query definition in JSON format
        semantic_layer_json: Semantic layer configuration in JSON format
        description: Human-readable test description
        run_query: Whether to execute query against BigQuery
    """
    print(f"\n{'='*50}")
    print(f" {description} ")
    print(f"{'='*50}")
    
    query = json.loads(query_json)
    semantic_layer = json.loads(semantic_layer_json)
    
    sql = generate_sql(query, semantic_layer)
    print("\nGenerated SQL:")
    print(sql)
    
    if run_query:
        print("\nResults:")
        query_bigquery(sql)

# Example usage
if __name__ == "__main__":
    # Example Query: Shows basic COUNT aggregation
    query_example = '''
    {
      "metrics": ["order_count"]
    }
    '''
    semantic_example = '''
    {
      "metrics": [
        {
          "name": "order_count",
          "sql": "COUNT(*)",
          "table": "orders"
        }
      ]
    }
    '''
    test_query(query_example, semantic_example, "Example: Basic order count", run_query=True)

    # Query 1: Basic revenue metric
    query1 = '''
    {
        "metrics": ["total_revenue"]
    }
    '''
    
    semantic1 = '''
    {
      "metrics": [
        {
          "name": "total_revenue",
          "sql": "SUM(sale_price)",
          "table": "order_items"
        }
      ]
    }
    '''
    
    # Query 2: Revenue grouped by status
    query2 = '''
    {
        "metrics": ["total_revenue"],
        "dimensions": ["status"]
    }
    '''
    semantic2 = '''
    {
      "metrics": [
        {
          "name": "total_revenue",
          "sql": "SUM(sale_price)",
          "table": "order_items"
        }
      ],
      "dimensions": [
        {
          "name": "status",
          "sql": "status",
          "table": "order_items"
        }
      ]
    }
    '''

    # Query 3: Filtered revenue for complete orders
    query3 = '''
    {
        "metrics": ["total_revenue"],
        "filters": [
            {
                "field": "status",
                "operator": "=",
                "value": "Complete"
            }
        ]
    }
    '''
    semantic3 = '''
    {
      "metrics": [
        {
          "name": "total_revenue",
          "sql": "SUM(sale_price)",
          "table": "order_items"
        }
      ],
      "dimensions": [
        {
          "name": "status",
          "sql": "status",
          "table": "order_items"
        }
      ]
    }
    '''

    # Query 4: Count with numeric filter
    query4 = '''
    {
        "metrics": ["count_of_orders"],
        "filters": [
            {
                "field": "num_of_item",
                "operator": ">",
                "value": 1
            }
        ]
    }
    '''
    semantic4 = '''
    {
      "metrics": [
        {
          "name": "count_of_orders",
          "sql": "COUNT(order_id)",
          "table": "orders"
        }
      ],
      "dimensions": [
        {
          "name": "num_of_item",
          "sql": "num_of_item",
          "table": "orders"
        }
      ]
    }
    '''

    # Query 5: Multiple filters combined
    query5 = '''
    {
        "metrics": ["count_of_orders"],
        "filters": [
            {
                "field": "status",
                "operator": "=",
                "value": "Complete"
            },
            {
                "field": "gender",
                "operator": "=",
                "value": "F"
            }
        ]
    }
    '''
    semantic5 = '''
    {
      "metrics": [
        {
          "name": "count_of_orders",
          "sql": "COUNT(order_id)",
          "table": "orders"
        }
      ],
      "dimensions": [
        {
          "name": "status",
          "sql": "status",
          "table": "orders"
        },
        {
          "name": "gender",
          "sql": "gender",
          "table": "orders"
        }
      ]
    }
    '''

    # Query 6: Metric filtering with HAVING
    query6 = '''
    {
        "metrics": ["total_revenue"],
        "dimensions": ["order_id"],
        "filters": [
            {
                "field": "total_revenue",
                "operator": ">",
                "value": 1000
            }
        ]
    }
    '''
    semantic6 = '''
    {
      "metrics": [
        {
          "name": "total_revenue",
          "sql": "SUM(sale_price)",
          "table": "order_items"
        }
      ],
      "dimensions": [
        {
          "name": "order_id",
          "sql": "order_id",
          "table": "order_items"
        }
      ]
    }
    '''

    # Query 7: Complex join with multiple dimensions
    query7 = '''
    {
        "metrics": ["total_revenue"],
        "dimensions": ["order_id", "gender", "status"],
        "filters": [
            {
                "field": "total_revenue",
                "operator": ">",
                "value": 1000
            }
        ]
    }
    '''
    semantic7 = '''
    {
      "metrics": [
        {
          "name": "total_revenue",
          "sql": "SUM(sale_price)",
          "table": "order_items"
        }
      ],
      "dimensions": [
        {
          "name": "order_id",
          "sql": "order_id",
          "table": "order_items"
        },
        {
          "name": "gender",
          "sql": "gender",
          "table": "orders"
        },
        {
          "name": "status",
          "sql": "status",
          "table": "orders"
        }
      ],
      "joins": [
        {
          "one": "orders",
          "many": "order_items",
          "join": "order_items.order_id = orders.order_id"
        }
      ]
    }
    '''

    # Query 8: Date handling with weekly aggregation
    query8 = '''
    {
        "metrics": ["total_revenue"],
        "dimensions": ["ordered_date__week"],
        "filters": [
            {
                "field": "ordered_date",
                "operator": ">=",
                "value": "2024-01-01"
            }
        ]
    }
    '''
    semantic8 = '''
    {
      "metrics": [
        {
          "name": "total_revenue",
          "sql": "SUM(sale_price)",
          "table": "order_items"
        }
      ],
      "dimensions": [
        {
          "name": "ordered_date",
          "sql": "created_at",
          "table": "orders"
        }
      ],
      "joins": [
        {
          "one": "orders",
          "many": "order_items",
          "join": "order_items.order_id = orders.order_id"
        }
      ]
    }
    '''

    # Run all queries with results
    test_query(query1, semantic1, "Query 1: Basic total revenue", run_query=True)
    test_query(query2, semantic2, "Query 2: Revenue by status", run_query=True)
    test_query(query3, semantic3, "Query 3: Revenue for complete orders", run_query=True)
    test_query(query4, semantic4, "Query 4: Orders with more than 1 item", run_query=True)
    test_query(query5, semantic5, "Query 5: Complete orders by women", run_query=True)
    test_query(query6, semantic6, "Query 6: Orders with value over $1000", run_query=True)
    test_query(query7, semantic7, "Query 7: Order details for orders over $1000", run_query=True)
    test_query(query8, semantic8, "Query 8: Weekly sales since 2024", run_query=True)