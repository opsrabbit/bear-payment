import logging
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Dict

app = FastAPI()

logging.basicConfig(level=logging.INFO)

class PaymentRequest(BaseModel):
    amount: float
    currency: str

@app.post('/pay')
async def process_payment(request: PaymentRequest):
    if not isinstance(request.amount, (int, float)) or request.amount <= 0:
        logging.error('Invalid amount provided')
        raise HTTPException(status_code=400, detail='Invalid amount')
    if not isinstance(request.currency, str) or len(request.currency)!= 3:
        logging.error('Invalid currency provided')
        raise HTTPException(status_code=400, detail='Invalid currency')
    # Simulate payment processing
    logging.info(f'Processing payment for {request.amount} {request.currency}')
    return {'status': 'Payment processed successfully'}

@app.get('/health')
async def health_check():
    return {'status': 'OK'}
