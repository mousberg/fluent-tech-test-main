from typing import Dict, List, Any, Optional
from dataclasses import dataclass
import json
from datetime import datetime

@dataclass
class Metric:
    """Represents an aggregation metric (e.g., SUM, COUNT) with its table source"""
    name: str    # Unique identifier for the metric
    sql: str     # SQL expression (e.g., "SUM(sale_price)")
    table: str   # Source table name

@dataclass
class Dimension:
    """Represents a grouping dimension with its table source"""
    name: str    # Unique identifier for the dimension
    sql: str     # SQL column or expression
    table: str   # Source table name

@dataclass
class Join:
    """Defines a table join relationship"""
    one: str     # Parent table name
    many: str    # Child table name
    join: str    # Join condition (e.g., "table1.id = table2.id")

@dataclass
class Filter:
    """Represents a WHERE or HAVING condition"""
    field: str   # Field to filter on
    operator: str  # Comparison operator (e.g., "=", ">")
    value: Any   # Filter value

@dataclass
class SemanticLayer:
    """Main configuration for the semantic layer"""
    metrics: List[Metric]
    dimensions: Optional[List[Dimension]] = None
    joins: Optional[List[Join]] = None

def parse_semantic_layer(semantic_json: Dict) -> SemanticLayer:
    """Parse semantic layer JSON into a SemanticLayer object"""
    metrics = [Metric(**m) for m in semantic_json.get("metrics", [])]
    dimensions = [Dimension(**d) for d in semantic_json.get("dimensions", [])] if "dimensions" in semantic_json else None
    joins = [Join(**j) for j in semantic_json.get("joins", [])] if "joins" in semantic_json else None
    return SemanticLayer(metrics=metrics, dimensions=dimensions, joins=joins)

def get_qualified_field(field: str, semantic_layer: SemanticLayer) -> str:
    """
    Get the table-qualified field name (e.g., "orders.status")
    
    Args:
        field: Field name to qualify
        semantic_layer: Semantic layer configuration
        
    Returns:
        Table-qualified field name (e.g., "table.field")
    """
    # First check dimensions
    if semantic_layer.dimensions:
        dimension = next((d for d in semantic_layer.dimensions if d.name == field), None)
        if dimension:
            return f"{dimension.table}.{dimension.sql}"
    
    # Then check metrics
    metric = next((m for m in semantic_layer.metrics if m.name == field), None)
    if metric:
        return f"{metric.table}.{metric.sql}"
    
    return field

def generate_sql(query: Dict, semantic_layer_json: Dict) -> str:
    """Generate SQL from query and semantic layer definitions"""
    
    semantic_layer = parse_semantic_layer(semantic_layer_json)
    
    # Get requested metrics
    requested_metrics = query.get("metrics", [])
    metric_sql_parts = []
    tables = set()
    
    # Build metric SQL parts
    for metric_name in requested_metrics:
        metric = next((m for m in semantic_layer.metrics if m.name == metric_name), None)
        if not metric:
            raise ValueError(f"Metric {metric_name} not found in semantic layer")
        metric_sql_parts.append(f"{metric.sql} AS {metric.name}")
        tables.add(metric.table)

    # Handle dimensions if present
    dimension_sql_parts = []
    if "dimensions" in query:
        for dim_name in query["dimensions"]:
            if dim_name.endswith("__week"):
                # Special handling for week grouping
                base_dim = dim_name.replace("__week", "")
                dimension = next((d for d in semantic_layer.dimensions if d.name == base_dim), None)
                if dimension:
                    dimension_sql_parts.append(f"DATE_TRUNC({dimension.table}.{dimension.sql}, WEEK) AS {dim_name}")
                    tables.add(dimension.table)
            else:
                dimension = next((d for d in semantic_layer.dimensions if d.name == dim_name), None)
                if dimension:
                    dimension_sql_parts.append(f"{dimension.table}.{dimension.sql} AS {dim_name}")
                    tables.add(dimension.table)

    # Build SELECT clause
    select_parts = dimension_sql_parts + metric_sql_parts
    sql = f"SELECT {', '.join(select_parts)}\n"

    # Build FROM and JOIN clauses
    if semantic_layer.joins and len(tables) > 1:
        # Find the table that's referenced in joins
        join_tables = {j.one for j in semantic_layer.joins} | {j.many for j in semantic_layer.joins}
        main_table = next(iter(tables & join_tables))
        sql += f"FROM {main_table}\n"
        
        # Add necessary joins
        for join in semantic_layer.joins:
            if join.one in tables or join.many in tables:
                if join.one == main_table:
                    sql += f"JOIN {join.many} ON {join.join}\n"
                else:
                    sql += f"JOIN {join.one} ON {join.join}\n"
    else:
        # Single table query
        sql += f"FROM {next(iter(tables))}\n"

    # Handle filters
    where_conditions = []
    having_conditions = []
    if "filters" in query:
        for filter_def in query["filters"]:
            filter_obj = Filter(**filter_def)
            
            # Handle metric filters (need to use HAVING)
            if filter_obj.field in requested_metrics:
                having_conditions.append(f"{filter_obj.field} {filter_obj.operator} {filter_obj.value}")
                continue
                
            # Handle date filters
            if isinstance(filter_obj.value, str) and filter_obj.field in [d.name for d in semantic_layer.dimensions or []]:
                try:
                    datetime.strptime(filter_obj.value, "%Y-%m-%d")
                    qualified_field = get_qualified_field(filter_obj.field, semantic_layer)
                    # Cast the TIMESTAMP to DATE for comparison
                    where_conditions.append(f"DATE({qualified_field}) {filter_obj.operator} DATE('{filter_obj.value}')")
                    continue
                except ValueError:
                    pass
            
            # Regular filters
            qualified_field = get_qualified_field(filter_obj.field, semantic_layer)
            if isinstance(filter_obj.value, (int, float)):
                where_conditions.append(f"{qualified_field} {filter_obj.operator} {filter_obj.value}")
            else:
                where_conditions.append(f"{qualified_field} {filter_obj.operator} '{filter_obj.value}'")

    if where_conditions:
        sql += f"WHERE {' AND '.join(where_conditions)}\n"

    # Add GROUP BY if we have dimensions
    if dimension_sql_parts:
        # Use the qualified dimension expressions for GROUP BY
        group_by_parts = []
        for dim_name in query['dimensions']:
            if dim_name.endswith("__week"):
                base_dim = dim_name.replace("__week", "")
                dimension = next((d for d in semantic_layer.dimensions if d.name == base_dim), None)
                if dimension:
                    group_by_parts.append(f"DATE_TRUNC({dimension.table}.{dimension.sql}, WEEK)")
            else:
                dimension = next((d for d in semantic_layer.dimensions if d.name == dim_name), None)
                if dimension:
                    group_by_parts.append(f"{dimension.table}.{dimension.sql}")
        
        sql += f"GROUP BY {', '.join(group_by_parts)}\n"

    # Add HAVING clause if we have metric filters
    if having_conditions:
        sql += f"HAVING {' AND '.join(having_conditions)}\n"

    return sql.strip() 