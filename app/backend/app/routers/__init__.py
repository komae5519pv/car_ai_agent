"""API routers package."""

from app.routers.customers import router as customers_router
from app.routers.recommendations import router as recommendations_router
from app.routers.chat import router as chat_router
from app.routers.admin import router as admin_router

__all__ = [
    "customers_router",
    "recommendations_router",
    "chat_router",
    "admin_router",
]
