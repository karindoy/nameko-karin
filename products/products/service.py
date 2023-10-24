import logging

from nameko.events import event_handler
from nameko.rpc import rpc

from products import dependencies, schemas


logger = logging.getLogger(__name__)


class ProductsService:

    name = 'products'

    storage = dependencies.Storage()

    @rpc
    def get(self, product_id):
        product = self.storage.get(product_id)
        return schemas.Product().dump(product).data

    @rpc
    def list(self):
        products = self.storage.list()
        return schemas.Product(many=True).dump(products).data

    @rpc
    def list_ids(self):
        return self.storage.list_ids()
    
    @rpc
    def create(self, product):
        product = schemas.Product(strict=True).load(product).data
        self.storage.create(product)

    @rpc
    def delete(self, product_id):
        product = self.storage.delete(product_id)
        return schemas.Product().dump(product).data
        
    @event_handler('orders', 'order_created')
    def handle_order_created(self, payload):
        order_details = payload['order']['order_details']
        product_ids_quantities = { order_detail['product_id'] : order_detail['quantity'] for order_detail in order_details }
        self.storage.decrement_stock(product_ids_quantities)
