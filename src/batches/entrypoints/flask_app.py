from datetime import datetime

from flask import Flask, request, jsonify
from sqlalchemy import create_engine

from batches import config
from batches.domain import model
from batches.adapters import repository
from batches.service_layer import services

engine = create_engine(config.get_postgres_uri())
app = Flask(__name__)


def get_session():
    return engine.connect()


def is_valid_sku(sku, batches):
    return sku in {b.sku for b in batches}


@app.route("/allocate", methods=["POST"])
def allocate_endpoint():
    connection = get_session()
    repo = repository.SqlAlchemyRepository(connection)

    try:
        batchref = services.allocate(
            request.json['orderid'], request.json['sku'], request.json['qty'],
            repo, connection)
    except (model.OutOfStock, services.InvalidSku) as e:
        return jsonify({'message': str(e)}), 400
    except Exception as err:
        raise err
    return jsonify({'batchref': batchref}), 201


@app.route("/add_batch", methods=['POST'])
def add_batch_endpoint():
    session = get_session()
    repo = repository.SqlAlchemyRepository(session)
    eta = request.json['eta']
    if eta is not None:
        eta = datetime.fromisoformat(eta).date()
    services.add_batch(
        request.json['ref'], request.json['sku'], request.json['qty'],
        eta,
        repo, session
    )
    return 'OK', 201
