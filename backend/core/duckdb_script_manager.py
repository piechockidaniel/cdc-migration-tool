"""
DuckDB Script Manager

Manages DuckDB transformation scripts stored in the file system.
Provides script repository, loading, validation, and execution support.
"""

import logging
import os
from typing import List, Dict, Optional, Tuple
from pathlib import Path

logger = logging.getLogger(__name__)

# Script repository paths
SCRIPTS_BASE_DIR = Path(os.environ.get("CDCSYNC_SCRIPTS_DIR", "duckdb_scripts"))
TEMPLATES_DIR = SCRIPTS_BASE_DIR / "templates"
CUSTOM_DIR = SCRIPTS_BASE_DIR / "custom"


class DuckDBScriptManager:
    """Manager for DuckDB transformation scripts."""
    
    def __init__(self):
        """Initialize the script manager."""
        self._ensure_directories()
    
    def _ensure_directories(self):
        """Ensure script directories exist."""
        for directory in [SCRIPTS_BASE_DIR, TEMPLATES_DIR, CUSTOM_DIR]:
            directory.mkdir(parents=True, exist_ok=True)
            logger.info(f"Ensured directory exists: {directory}")
    
    def list_scripts(self, category: Optional[str] = None) -> List[Dict[str, str]]:
        """List available scripts.
        
        Args:
            category: Filter by category ('template' or 'custom'), None for all
            
        Returns:
            List of script info dictionaries
        """
        scripts = []
        
        # Scan templates directory
        if category is None or category == 'template':
            for script_file in TEMPLATES_DIR.glob("*.sql"):
                scripts.append({
                    "name": script_file.stem,
                    "filename": script_file.name,
                    "category": "template",
                    "path": str(script_file),
                    "description": self._get_script_description(script_file)
                })
        
        # Scan custom directory
        if category is None or category == 'custom':
            for script_file in CUSTOM_DIR.glob("*.sql"):
                scripts.append({
                    "name": script_file.stem,
                    "filename": script_file.name,
                    "category": "custom",
                    "path": str(script_file),
                    "description": self._get_script_description(script_file)
                })
        
        return sorted(scripts, key=lambda x: (x['category'], x['name']))
    
    def _get_script_description(self, script_path: Path) -> str:
        """Extract description from script file (first comment line).
        
        Args:
            script_path: Path to script file
            
        Returns:
            Description string
        """
        try:
            with open(script_path, 'r') as f:
                first_line = f.readline().strip()
                if first_line.startswith('--'):
                    return first_line[2:].strip()
        except Exception as e:
            logger.warning(f"Could not read script description from {script_path}: {e}")
        return "No description"
    
    def get_script(self, script_name: str) -> Optional[str]:
        """Get script content by name.
        
        Args:
            script_name: Name of the script (without .sql extension)
            
        Returns:
            Script content or None if not found
        """
        # Try templates first
        template_path = TEMPLATES_DIR / f"{script_name}.sql"
        if template_path.exists():
            return self._read_script_file(template_path)
        
        # Try custom scripts
        custom_path = CUSTOM_DIR / f"{script_name}.sql"
        if custom_path.exists():
            return self._read_script_file(custom_path)
        
        logger.warning(f"Script not found: {script_name}")
        return None
    
    def _read_script_file(self, script_path: Path) -> Optional[str]:
        """Read script file content.
        
        Args:
            script_path: Path to script file
            
        Returns:
            Script content
        """
        try:
            with open(script_path, 'r', encoding='utf-8') as f:
                content = f.read()
            logger.info(f"Loaded script: {script_path}")
            return content
        except Exception as e:
            logger.error(f"Error reading script {script_path}: {e}")
            return None
    
    def save_script(self, script_name: str, content: str, category: str = 'custom', 
                   description: Optional[str] = None, overwrite: bool = False) -> Tuple[bool, str]:
        """Save a new script or update existing.
        
        Args:
            script_name: Name of the script (without .sql extension)
            content: Script content
            category: 'template' or 'custom'
            description: Script description
            overwrite: Allow overwriting existing script
            
        Returns:
            Tuple of (success, message)
        """
        # Validate script name
        if not script_name or not script_name.replace('_', '').replace('-', '').isalnum():
            return False, "Invalid script name. Use alphanumeric characters, hyphens, and underscores only."
        
        # Determine directory
        if category == 'template':
            script_dir = TEMPLATES_DIR
        else:
            script_dir = CUSTOM_DIR
        
        script_path = script_dir / f"{script_name}.sql"
        
        # Check if exists
        if script_path.exists() and not overwrite:
            return False, f"Script '{script_name}' already exists. Use overwrite=True to replace."
        
        try:
            # Add description as first line if provided
            final_content = content
            if description:
                final_content = f"-- {description}\n{content}"
            
            # Write script file
            with open(script_path, 'w', encoding='utf-8') as f:
                f.write(final_content)
            
            logger.info(f"Saved script: {script_path}")
            return True, f"Script '{script_name}' saved successfully"
            
        except Exception as e:
            logger.error(f"Error saving script {script_name}: {e}")
            return False, f"Error saving script: {str(e)}"
    
    def delete_script(self, script_name: str, category: str = 'custom') -> Tuple[bool, str]:
        """Delete a script.
        
        Args:
            script_name: Name of the script (without .sql extension)
            category: 'template' or 'custom'
            
        Returns:
            Tuple of (success, message)
        """
        # Only allow deleting custom scripts for safety
        if category != 'custom':
            return False, "Can only delete custom scripts. Templates are protected."
        
        script_path = CUSTOM_DIR / f"{script_name}.sql"
        
        if not script_path.exists():
            return False, f"Script '{script_name}' not found"
        
        try:
            script_path.unlink()
            logger.info(f"Deleted script: {script_path}")
            return True, f"Script '{script_name}' deleted successfully"
        except Exception as e:
            logger.error(f"Error deleting script {script_name}: {e}")
            return False, f"Error deleting script: {str(e)}"
    
    def validate_script_syntax(self, content: str) -> Tuple[bool, str]:
        """Basic validation of DuckDB SQL script syntax.
        
        Args:
            content: Script content
            
        Returns:
            Tuple of (is_valid, message)
        """
        if not content or not content.strip():
            return False, "Script content is empty"
        
        # Basic checks
        content_lower = content.lower()
        
        # Check for required elements
        if 'select' not in content_lower and 'insert' not in content_lower:
            return False, "Script must contain SELECT or INSERT statements"
        
        # Check for dangerous operations (basic safety)
        dangerous_ops = ['drop table', 'drop database', 'truncate']
        for op in dangerous_ops:
            if op in content_lower:
                logger.warning(f"Script contains potentially dangerous operation: {op}")
        
        # Check for balanced parentheses
        if content.count('(') != content.count(')'):
            return False, "Unbalanced parentheses in script"
        
        return True, "Script syntax appears valid"
    
# Global instance
duckdb_script_manager = DuckDBScriptManager()

