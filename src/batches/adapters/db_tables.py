import sqlalchemy as sa
from sqlalchemy.engine import Engine

from batches.domain import model


metadata = sa.MetaData()


order_lines = sa.Table(
    'order_lines', metadata,
    sa.Column('id', sa.Integer, primary_key=True, autoincrement=True),
    sa.Column('sku', sa.String(255)),
    sa.Column('qty', sa.Integer, nullable=False),
    sa.Column('orderid', sa.String(255)),
)

sa.Index('idx_unq_orderline_sku_orderid', 
    order_lines.c.orderid,
    order_lines.c.sku,
    unique=True)

batches = sa.Table(
    'batches', metadata,
    sa.Column('id', sa.Integer, primary_key=True, autoincrement=True),
    sa.Column('reference', sa.String(255), unique=True),
    sa.Column('sku', sa.String(255)),
    sa.Column('purchased_quantity', sa.Integer, nullable=False),
    sa.Column('eta', sa.DateTime(timezone=True)),
)

allocations = sa.Table(
    "allocations", metadata,
    sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
    sa.Column("orderline_id", sa.ForeignKey("order_lines.id")),
    sa.Column("batch_id", sa.ForeignKey("batches.id")),
)

sa.Index('idx_unq_allocations_batch_line', 
         allocations.c.orderline_id,
         allocations.c.batch_id,
         unique=True)
