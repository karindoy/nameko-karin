from nameko import config
from nameko.extensions import DependencyProvider
import redis

from products.exceptions import NotFound


REDIS_URI_KEY = 'REDIS_URI'


class StorageWrapper:
    """
    Product storage

    A very simple example of a custom Nameko dependency. Simplified
    implementation of products database based on Redis key value store.
    Handling the product ID increments or keeping sorted sets of product
    names for ordering the products is out of the scope of this example.

    """

    NotFound = NotFound

    def __init__(self, client):
        self.client = client

    def _format_key(self, product_id):
        return 'products:{}'.format(product_id)

    def _from_hash(self, document):
        return {
            'id': document[b'id'].decode('utf-8'),
            'title': document[b'title'].decode('utf-8'),
            'passenger_capacity': int(document[b'passenger_capacity']),
            'maximum_speed': int(document[b'maximum_speed']),
            'in_stock': int(document[b'in_stock'])
        }

    def _format_ids(self, key):
        return key.decode('utf-8').replace('products:', '')
    
    def get(self, product_id):
        product = self.client.hgetall(self._format_key(product_id))
        if not product:
            raise NotFound('Product ID {} does not exist'.format(product_id))
        else:
            return self._from_hash(product)

    def list(self):
        keys = self.client.keys(self._format_key('*'))
        for key in keys:
            yield self._from_hash(self.client.hgetall(key))
        
    def list_ids(self):
        keys = self.client.keys(self._format_key('*'))
        return [{'id': self._format_ids(key) } for key in keys]


    def create(self, product):
        self.client.hmset(
            self._format_key(product['id']),
            product)

    def delete(self, product_id):
        
        if product_id not in {prod['id'] for prod in self.list_ids}:
            raise NotFound('Product ID {} does not exist'.format(product_id))
        else:
            return self.client.delete(self._format_key(product_id))
        
    def decrement_stock(self, product_ids_quantities):
        response_dict = {}
        with self.client.pipeline() as pipe: 
            pipe.multi()
            for product_id, amount in product_ids_quantities.items():
                pipe.hincrby(self._format_key(product_id), 'in_stock', -amount)
                x = pipe.execute()
                response_dict[product_id] = x[0]
                

        return response_dict


class Storage(DependencyProvider):

    def setup(self):
        self.client = redis.StrictRedis.from_url(config.get(REDIS_URI_KEY))

    def get_dependency(self, worker_ctx):
        return StorageWrapper(self.client)
