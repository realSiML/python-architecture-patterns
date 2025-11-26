from sqlalchemy import String, Integer, MetaData, Table, Column
from sqlalchemy.orm import registry
from src import model


metadata = MetaData()

order_lines = Table(
    "order_lines",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("sku", String(255)),
    Column("qty", Integer, nullable=False),
    Column("orderid", String(255)),
)


def start_mappers():
    # Используем registry для маппинга
    mapper_registry = registry()
    mapper_registry.map_imperatively(model.OrderLine, order_lines)
