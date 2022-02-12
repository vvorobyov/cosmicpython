from datetime import datetime

from flask import Flask, request, jsonify
from sqlalchemy import create_engine

from batches import config
from batches.domain import model
from batches.adapters import repository
from batches.service_layer import services, unit_of_work

engine = create_engine(config.get_postgres_uri())
app = Flask(__name__)


def is_valid_sku(sku, batches):
    return sku in {b.sku for b in batches}


@app.route("/allocate", methods=["POST"])
def allocate_endpoint():
    uow = unit_of_work.SqlAlchemyUnitOfWork(engine)

    try:
        batchref = services.allocate(
            request.json['orderid'], request.json['sku'], request.json['qty'],
            uow)
    except (model.OutOfStock, services.InvalidSku) as e:
        return jsonify({'message': str(e)}), 400
    except Exception as err:
        raise err
    return jsonify({'batchref': batchref}), 201


@app.route("/add_batch", methods=['POST'])
def add_batch_endpoint():
    uow = unit_of_work.SqlAlchemyUnitOfWork(engine)
    eta = request.json['eta']
    if eta is not None:
        eta = datetime.fromisoformat(eta).date()
    services.add_batch(
        request.json['ref'], request.json['sku'], request.json['qty'],
        eta, uow
    )
    return 'OK', 201
