"""
Filename Template Engine

Provides Jinja2-based template processing for flexible filename generation
with custom filters and comprehensive validation.
"""

import re
import time
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional, Union
from datetime import datetime

import jinja2
from jinja2 import Environment, BaseLoader, select_autoescape, TemplateSyntaxError

from redditdl.utils import sanitize_filename


class FilenameTemplateEngine:
    """
    Jinja2-based template engine for filename generation.
    
    Provides flexible filename templating with custom filters and comprehensive
    validation. Supports backward compatibility with simple {variable} patterns.
    """
    
    def __init__(self):
        """Initialize the template engine with custom filters."""
        self.logger = logging.getLogger(__name__)
        
        # Create Jinja2 environment with safe defaults
        self.env = Environment(
            loader=BaseLoader(),
            autoescape=select_autoescape([]),  # No autoescaping for filenames
            trim_blocks=True,
            lstrip_blocks=True,
            undefined=jinja2.StrictUndefined  # Raise errors for undefined variables
        )
        
        # Register custom filters
        self._register_custom_filters()
        
        # Template presets
        self.presets = {
            'default': "{{ subreddit }}/{{ post_id }}-{{ title|slugify }}.{{ ext }}",
            'date_organized': "{{ date|strftime('%Y/%m/%d') }}/{{ post_id }}.{{ ext }}",
            'user_organized': "{{ author }}/{{ post_id }}-{{ title|slugify }}.{{ ext }}",
            'score_organized': "{{ subreddit }}/{{ score }}-{{ post_id }}.{{ ext }}",
            'content_type_organized': "{{ content_type }}/{{ subreddit }}/{{ post_id }}.{{ ext }}",
            'flat': "{{ post_id }}-{{ title|slugify }}.{{ ext }}"
        }
    
    def _register_custom_filters(self):
        """Register custom Jinja2 filters for filename processing."""
        
        def slugify_filter(text: str, max_length: int = 50) -> str:
            """Convert text to a URL-friendly slug."""
            if not text:
                return "untitled"
            
            # Convert to lowercase and replace spaces/special chars with hyphens
            slug = re.sub(r'[^\w\s-]', '', str(text).lower())
            slug = re.sub(r'[-\s]+', '-', slug).strip('-')
            
            # Truncate to max_length
            if len(slug) > max_length:
                slug = slug[:max_length].rstrip('-')
            
            return slug if slug else "untitled"
        
        def sanitize_filter(text: str) -> str:
            """Sanitize text for safe filename usage."""
            return sanitize_filename(str(text)) if text else "unnamed"
        
        def strftime_filter(date_obj: Union[str, datetime, float], format_str: str = '%Y-%m-%d') -> str:
            """Format date object or ISO string using strftime."""
            try:
                if isinstance(date_obj, str):
                    # Try to parse ISO format
                    if 'T' in date_obj:
                        dt = datetime.fromisoformat(date_obj.replace('Z', '+00:00'))
                    else:
                        # Try basic date format
                        dt = datetime.strptime(date_obj, '%Y-%m-%d')
                elif isinstance(date_obj, (int, float)):
                    # Unix timestamp
                    dt = datetime.fromtimestamp(date_obj)
                elif isinstance(date_obj, datetime):
                    dt = date_obj
                else:
                    # Fallback to current time
                    dt = datetime.now()
                
                return dt.strftime(format_str)
            except (ValueError, TypeError) as e:
                self.logger.warning(f"Date formatting error: {e}, using current date")
                return datetime.now().strftime(format_str)
        
        def truncate_filter(text: str, length: int = 100, suffix: str = '') -> str:
            """Truncate text to specified length."""
            if not text or len(text) <= length:
                return text
            return text[:length - len(suffix)] + suffix
        
        def extension_filter(url: str, default: str = 'unknown') -> str:
            """Extract file extension from URL or path."""
            if not url:
                return default
            
            # Handle URLs with query parameters
            url_path = url.split('?')[0].split('#')[0]
            ext = Path(url_path).suffix.lower()
            
            # Remove leading dot and return
            return ext[1:] if ext else default
        
        # Register filters
        self.env.filters['slugify'] = slugify_filter
        self.env.filters['sanitize'] = sanitize_filter
        self.env.filters['strftime'] = strftime_filter
        self.env.filters['truncate'] = truncate_filter
        self.env.filters['ext'] = extension_filter
        self.env.filters['extension'] = extension_filter
    
    def render(self, template: str, variables: Dict[str, Any], max_length: int = 200) -> str:
        """
        Render a template with provided variables.
        
        Args:
            template: Template string (Jinja2 or simple {variable} format)
            variables: Dictionary of template variables
            max_length: Maximum filename length
            
        Returns:
            Rendered filename string
            
        Raises:
            TemplateSyntaxError: If template syntax is invalid
            jinja2.UndefinedError: If required variables are missing
        """
        try:
            # Check if this is a simple {variable} template and convert to Jinja2
            converted_template = self._convert_simple_template(template)
            
            # Add default variables
            template_vars = self._prepare_template_variables(variables)
            
            # Render the template
            jinja_template = self.env.from_string(converted_template)
            rendered = jinja_template.render(**template_vars)
            
            # Post-process the result
            result = self._post_process_filename(rendered, max_length)
            
            return result
            
        except TemplateSyntaxError as e:
            raise TemplateSyntaxError(f"Template syntax error: {e}")
        except jinja2.UndefinedError as e:
            raise jinja2.UndefinedError(f"Missing template variable: {e}")
        except Exception as e:
            self.logger.error(f"Template rendering error: {e}")
            # Fallback to safe filename
            return self._generate_fallback_filename(variables)
    
    def validate_template(self, template: str) -> List[str]:
        """
        Validate a template string for syntax and required variables.
        
        Args:
            template: Template string to validate
            
        Returns:
            List of validation error messages (empty if valid)
        """
        errors = []
        
        try:
            # Convert simple templates first
            converted_template = self._convert_simple_template(template)
            
            # Try to parse the template
            self.env.from_string(converted_template)
            
            # Check for required variables
            required_vars = self._extract_template_variables(converted_template)
            if 'ext' not in required_vars and 'extension' not in required_vars:
                errors.append("Template must include {{ ext }} or {{ extension }} for file extension")
            
            # Check for potentially dangerous patterns
            # First, remove valid placeholders to avoid false positives
            sanitized_template = re.sub(r'\{\{.*?\}\}', '', converted_template)
            
            # Now check the sanitized string for unsafe characters
            dangerous_chars = r'[<>:"|?*]'
            if re.search(dangerous_chars, sanitized_template):
                errors.append(f"Template contains forbidden characters: {dangerous_chars}")

            if "../" in template or "..\\" in template:
                errors.append("Template contains path traversal pattern ('../')")

        except TemplateSyntaxError as e:
            errors.append(f"Template syntax error: {e}")
        except Exception as e:
            errors.append(f"Template validation error: {e}")
        
        return errors
    
    def get_preset(self, preset_name: str) -> Optional[str]:
        """
        Get a predefined template preset.
        
        Args:
            preset_name: Name of the preset
            
        Returns:
            Template string or None if preset doesn't exist
        """
        return self.presets.get(preset_name)
    
    def list_presets(self) -> List[str]:
        """Get list of available preset names."""
        return list(self.presets.keys())
    
    def _convert_simple_template(self, template: str) -> str:
        """
        Convert simple {variable} templates to Jinja2 {{ variable }} format.
        
        Args:
            template: Template string that might use simple {variable} syntax
            
        Returns:
            Jinja2-compatible template string
        """
        # Pattern to match {variable} but not {{ variable }}
        simple_pattern = r'(?<!\{)\{([^{}]+)\}(?!\})'
        
        def replace_simple_var(match):
            var_name = match.group(1)
            return f"{{{{ {var_name} }}}}"
        
        # Convert simple variables to Jinja2 format
        converted = re.sub(simple_pattern, replace_simple_var, template)
        
        return converted
    
    def _prepare_template_variables(self, variables: Dict[str, Any]) -> Dict[str, Any]:
        """
        Prepare and normalize template variables.
        
        Args:
            variables: Raw template variables
            
        Returns:
            Processed template variables with defaults
        """
        # Start with provided variables
        template_vars = dict(variables)
        
        # Add default values for common variables
        defaults = {
            'subreddit': 'unknown',
            'post_id': 'unknown',
            'title': 'untitled',
            'author': 'unknown',
            'date': datetime.now().isoformat(),
            'ext': 'unknown',
            'content_type': 'unknown',
            'score': 0,
            'url': '',
            'is_video': False,
        }
        
        # Apply defaults for missing variables
        for key, default_value in defaults.items():
            if key not in template_vars or template_vars[key] is None:
                template_vars[key] = default_value
        
        # Ensure strings are properly encoded
        for key, value in template_vars.items():
            if isinstance(value, str):
                template_vars[key] = value.strip()
        
        return template_vars
    
    def _extract_template_variables(self, template: str) -> List[str]:
        """
        Extract variable names from a Jinja2 template.
        
        Args:
            template: Jinja2 template string
            
        Returns:
            List of variable names used in the template
        """
        try:
            parsed = self.env.parse(template)
            variables = jinja2.meta.find_undeclared_variables(parsed)
            return list(variables)
        except Exception:
            # Fallback to regex extraction
            pattern = r'\{\{\s*([a-zA-Z_][a-zA-Z0-9_]*)'
            matches = re.findall(pattern, template)
            return list(set(matches))
    
    def _post_process_filename(self, filename: str, max_length: int) -> str:
        """
        Post-process rendered filename for safety and length.
        
        Args:
            filename: Rendered filename
            max_length: Maximum allowed length
            
        Returns:
            Safe, length-limited filename
        """
        if not filename:
            return "unnamed_file"
        
        # Sanitize the filename
        safe_filename = sanitize_filename(filename)
        
        # Handle length limits
        if len(safe_filename) > max_length:
            # Try to preserve the extension
            path_obj = Path(safe_filename)
            name = path_obj.stem
            ext = path_obj.suffix
            
            # Calculate available space for name
            available_length = max_length - len(ext)
            if available_length > 10:  # Minimum reasonable filename length
                truncated_name = name[:available_length]
                safe_filename = f"{truncated_name}{ext}"
            else:
                # Fallback: truncate everything
                safe_filename = safe_filename[:max_length]
        
        return safe_filename
    
    def _generate_fallback_filename(self, variables: Dict[str, Any]) -> str:
        """
        Generate a safe fallback filename when template rendering fails.
        
        Args:
            variables: Template variables
            
        Returns:
            Safe fallback filename
        """
        post_id = variables.get('post_id', 'unknown')
        ext = variables.get('ext', 'unknown')
        timestamp = str(int(time.time()))
        
        return sanitize_filename(f"{timestamp}_{post_id}.{ext}")