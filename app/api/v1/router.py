from fastapi import APIRouter, Depends

from app.api.v1.payments import router as payments_router
from app.core.security import require_api_key

router = APIRouter(prefix="/api/v1", dependencies=[Depends(require_api_key)])
router.include_router(payments_router)
