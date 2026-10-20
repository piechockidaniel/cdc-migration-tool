"""Web routes: the single-page application at ``/`` and the 1.x pages under ``/legacy``."""
from fastapi import APIRouter, Request
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.templating import Jinja2Templates

router = APIRouter()
templates = Jinja2Templates(directory="frontend/templates")

SPA_INDEX = "frontend/app/index.html"


@router.get("/", response_class=HTMLResponse, include_in_schema=False)
async def spa_index():
    """Serve the SPA (client-side hash routing)."""
    return FileResponse(SPA_INDEX, media_type="text/html")


# 1.x server rendered pages (visual workflow designer, multi-source builder, ...)
@router.get("/legacy", response_class=HTMLResponse, include_in_schema=False)
@router.get("/legacy/", response_class=HTMLResponse, include_in_schema=False)
async def legacy_index(request: Request):
    return templates.TemplateResponse(request, "index.html")


@router.get("/legacy/admin", response_class=HTMLResponse, include_in_schema=False)
async def legacy_admin(request: Request):
    return templates.TemplateResponse(request, "admin.html")


@router.get("/legacy/operations", response_class=HTMLResponse, include_in_schema=False)
async def legacy_operations(request: Request):
    return templates.TemplateResponse(request, "operations.html")


@router.get("/legacy/monitoring", response_class=HTMLResponse, include_in_schema=False)
async def legacy_monitoring(request: Request):
    return templates.TemplateResponse(request, "monitoring.html")


# old bookmarks keep working
@router.get("/admin", include_in_schema=False)
@router.get("/operations", include_in_schema=False)
@router.get("/monitoring", include_in_schema=False)
@router.get("/control", include_in_schema=False)
async def redirect_old_pages(request: Request):
    from fastapi.responses import RedirectResponse
    return RedirectResponse(url="/#/" + request.url.path.strip("/"))
