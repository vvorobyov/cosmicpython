from flask import Flask, request, jsonify
from sqlalchemy import create_engine

from batches import config
from batches.domain import model
from batches.adapters import repository
from batches.service_layer import services

get_session = create_engine(config.get_postgres_uri())
app = Flask(__name__)


def is_valid_sku(sku, batches):
    return sku in {b.sku for b in batches}


@app.route("/allocate", methods=["POST"])
def allocate_endpoint():
    connection = get_session.connect()
    repo = repository.SqlAlchemyRepository(connection)

    line = model.OrderLine(
        request.json['orderid'],
        request.json['sku'],
        request.json['qty'],
    )
    try:
        batchref = services.allocate(line, repo, connection)
    except (model.OutOfStock, services.InvalidSku) as e:
        return jsonify({'message': str(e)}), 400
    return jsonify({'batchref': batchref}), 201
