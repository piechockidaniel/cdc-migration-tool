"""
MSSQL CDC Database Synchronizer - Application Entry Point

This script starts the FastAPI application with Uvicorn server.
"""

import sys
import logging
from pathlib import Path
import uvicorn

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('app.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


def ensure_directories():
    """Ensure required directories exist."""
    directories = ['config', 'history']
    
    for directory in directories:
        path = Path(directory)
        if not path.exists():
            path.mkdir(parents=True, exist_ok=True)
            logger.info(f"Created directory: {directory}")


def main():
    """Main entry point for the application."""
    logger.info("=" * 70)
    logger.info("Starting MSSQL CDC Database Synchronizer")
    logger.info("=" * 70)
    
    # Ensure required directories exist
    ensure_directories()
    
    # Configuration
    host = "0.0.0.0"
    port = 8000
    
    logger.info(f"Web UI will be available at http://localhost:{port}")
    logger.info(f"API documentation at http://localhost:{port}/docs")
    logger.info(f"Alternative API docs at http://localhost:{port}/redoc")
    logger.info("Press CTRL+C to stop the application")
    logger.info("=" * 70)
    
    try:
        # Start the FastAPI application with Uvicorn
        uvicorn.run(
            "backend.api.app:app",
            host=host,
            port=port,
            reload=False,
            log_level="info",
            access_log=True
        )
    except KeyboardInterrupt:
        logger.info("\nApplication stopped by user")
    except Exception as e:
        logger.error(f"Failed to start application: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
