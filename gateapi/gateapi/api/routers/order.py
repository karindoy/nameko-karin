from os import name
from fastapi import APIRouter, status, HTTPException
from fastapi.params import Depends
from typing import List
from gateapi.api import schemas
from gateapi.api.dependencies import get_rpc, config
from .exceptions import OrderNotFound

router = APIRouter(
    prefix = "/orders",
    tags = ['Orders']
)

@router.get("/{order_id}", status_code=status.HTTP_200_OK)
def get_order(order_id: int, rpc = Depends(get_rpc)):
    try:
        return _get_order(order_id, rpc)
    except OrderNotFound as error:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(error)
        )

def _get_order(order_id, nameko_rpc):
    # Retrieve order data from the orders service.
    # Note - this may raise a remote exception that has been mapped to
    # raise``OrderNotFound``
    with nameko_rpc.next() as nameko:
        order = nameko.orders.get_order(order_id)

    # get the configured image root
    image_root = config['PRODUCT_IMAGE_ROOT']

    # Enhance order details with product and image details.
    for order_details in order['order_details']:
        product_id = order_details['product_id']
        if(bool(nameko.products.exist(product_id))):
            order_details['product'] = nameko.products.get(product_id)
        # Construct an image url.
        order_details['image'] = '{}/{}.jpg'.format(image_root, product_id)

    return order

@router.post("", status_code=status.HTTP_200_OK, response_model=schemas.CreateOrderSuccess)
def create_order(request: schemas.CreateOrder, rpc = Depends(get_rpc)):
    id_ =  _create_order(request.dict(), rpc)
    return {
        'id': id_
    }

def _create_order(order_data, nameko_rpc):
    # check order product ids are valid
    with nameko_rpc.next() as nameko:
        for item in order_data['order_details']:
            exist_product = nameko.products.exist(item['product_id'])
            if not exist_product:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                                    detail=f"Product with id {item['product_id']} not found"
            )
        # Call orders-service to create the order.
        result = nameko.orders.create_order(
            order_data['order_details']
        )
        return result['id']