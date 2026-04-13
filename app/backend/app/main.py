"""Main FastAPI application entry point."""

import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

from app.config import get_settings, is_databricks_app
from app.database import db
from app.llm import llm
from app.models import HealthResponse
from app.routers import (
    customers_router,
    recommendations_router,
    chat_router,
    admin_router,
)

# Image directory for car photos
def find_images_dir() -> Optional[Path]:
    """Find _images folder."""
    possible_paths = [
        # Databricks Apps: _images is at root level
        Path(__file__).parent.parent.parent.parent / "_images",
        Path.cwd() / "_images",
        Path.cwd().parent / "_images",
    ]
    for path in possible_paths:
        if path.exists():
            print(f"Found images at: {path}")
            return path
    print("Images directory not found")
    return None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    # Startup
    print("Starting Car AI Demo Backend...")
    await db.initialize()
    llm.initialize()
    print("Initialization complete")

    yield

    # Shutdown
    print("Shutting down...")
    await db.close()


# Create FastAPI application
app = FastAPI(
    title="Car AI Demo API",
    description="AI-powered vehicle recommendation system for sales teams",
    version="0.1.0",
    lifespan=lifespan,
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, restrict this
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(customers_router)
app.include_router(recommendations_router)
app.include_router(chat_router)
app.include_router(admin_router)


@app.get("/api/health", response_model=HealthResponse)
async def health_check() -> HealthResponse:
    """Health check endpoint."""
    return HealthResponse(
        status="healthy",
        database="demo" if db.is_demo_mode else "connected",
        llm="demo" if llm.is_demo_mode else "connected",
    )


# Mount images directory
_images_dir = find_images_dir()
if _images_dir:
    app.mount("/api/images", StaticFiles(directory=str(_images_dir)), name="images")
    print(f"Images mounted at /api/images from {_images_dir}")


# Find frontend dist path
def find_frontend_dist() -> Optional[Path]:
    """Find frontend dist folder."""
    possible_paths = [
        # Databricks Apps: dist is at root level (same level as backend/)
        Path(__file__).parent.parent.parent / "dist",
        Path.cwd() / "dist",
        # Local development: frontend/dist
        Path(__file__).parent.parent.parent / "frontend" / "dist",
        Path.cwd() / "frontend" / "dist",
    ]
    for path in possible_paths:
        print(f"Checking frontend path: {path}")
        if path.exists() and (path / "index.html").exists():
            print(f"Found frontend at: {path}")
            return path
    print("Frontend dist not found in any location")
    return None


# Set up frontend serving
_frontend_dist = find_frontend_dist()

if _frontend_dist:
    # Mount static assets
    _assets_dir = _frontend_dist / "assets"
    if _assets_dir.exists():
        app.mount("/assets", StaticFiles(directory=str(_assets_dir)), name="assets")
        print("Static assets mounted at /assets")

    # Serve SPA for all non-API routes (including root)
    @app.get("/{full_path:path}")
    async def serve_spa(full_path: str):
        """Serve React SPA for all non-API routes."""
        if full_path.startswith("api/"):
            return {"error": "Not found"}, 404
        index_path = _frontend_dist / "index.html"
        if index_path.exists():
            return FileResponse(str(index_path))
        return {"error": "Frontend not built"}, 404

    print("SPA catch-all route configured")
else:
    # Fallback: API-only mode
    @app.get("/")
    async def root():
        """Root endpoint when frontend is not available."""
        return {
            "message": "Car AI Demo API",
            "docs": "/docs",
            "health": "/api/health",
        }
    print("Running in API-only mode (no frontend)")


if __name__ == "__main__":
    import uvicorn

    settings = get_settings()
    port = int(os.environ.get("PORT", settings.port))

    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=port,
        reload=settings.debug,
    )
