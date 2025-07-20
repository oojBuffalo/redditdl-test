"""
Plugin Security Scanner

Provides security scanning and validation for plugins to detect potentially
malicious code patterns and enforce security policies.
"""

import ast
import inspect
import importlib.util
from pathlib import Path
from typing import List, Dict, Any, Set, Optional, Tuple
import logging

from .validation import SecurityValidationError
from .audit import get_auditor, EventType, Severity, SecurityError


class PluginSecurityScanner:
    """
    Security scanner for plugin code analysis.
    
    Scans plugin source code for potentially dangerous patterns,
    validates plugin structure, and enforces security policies.
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.auditor = get_auditor()
        
        # Dangerous function/module patterns
        self.dangerous_imports = {
            'os.system', 'subprocess', 'eval', 'exec', 'compile',
            'importlib.import_module', '__import__', 'open',
            'file', 'input', 'raw_input', 'execfile',
            'reload', 'vars', 'locals', 'globals', 'dir',
            'getattr', 'setattr', 'delattr', 'hasattr'
        }
        
        # Suspicious module imports
        self.suspicious_modules = {
            'os', 'subprocess', 'sys', 'importlib', 'ctypes',
            'multiprocessing', 'threading', 'socket', 'urllib',
            'requests', 'http', 'ftplib', 'smtplib', 'telnetlib',
            'pickle', 'marshal', 'shelve', 'dbm', 'sqlite3'
        }
        
        # Allowed modules for plugins
        self.allowed_modules = {
            'json', 'csv', 're', 'datetime', 'time', 'math',
            'statistics', 'collections', 'itertools', 'functools',
            'typing', 'dataclasses', 'enum', 'pathlib', 'logging',
            'base64', 'hashlib', 'uuid', 'random', 'string',
            'PIL', 'pillow', 'jinja2', 'yaml', 'pyyaml'
        }
        
        # File operation patterns
        self.file_ops = {
            'open', 'file', 'read', 'write', 'remove', 'unlink',
            'mkdir', 'makedirs', 'rmdir', 'removedirs', 'rename'
        }
        
        # Network operation patterns
        self.network_ops = {
            'socket', 'connect', 'send', 'recv', 'request',
            'urlopen', 'urlretrieve', 'get', 'post', 'put', 'delete'
        }
    
    def scan_plugin_file(self, plugin_path: Path) -> Dict[str, Any]:
        """
        Scan a plugin file for security issues.
        
        Args:
            plugin_path: Path to plugin file
            
        Returns:
            Security scan results with issues and metadata
        """
        results = {
            'plugin_path': str(plugin_path),
            'issues': [],
            'warnings': [],
            'imports': [],
            'functions': [],
            'classes': [],
            'risk_level': 'low',
            'allowed': True
        }
        
        try:
            # Read plugin source code
            with open(plugin_path, 'r', encoding='utf-8') as f:
                source_code = f.read()
            
            # Parse AST for analysis
            tree = ast.parse(source_code, filename=str(plugin_path))
            
            # Analyze the AST
            self._analyze_ast(tree, results)
            
            # Perform pattern-based analysis
            self._analyze_source_patterns(source_code, results)
            
            # Calculate risk level
            self._calculate_risk_level(results)
            
            # Log security scan
            self.auditor.log_plugin_event(
                action="security_scan",
                plugin_name=plugin_path.name,
                success=True,
                error_message=None
            )
            
        except Exception as e:
            self.logger.error(f"Security scan failed for {plugin_path}: {e}")
            results['issues'].append({
                'type': 'scan_error',
                'message': f"Failed to scan plugin: {e}",
                'severity': 'high'
            })
            results['risk_level'] = 'high'
            results['allowed'] = False
            
            self.auditor.log_plugin_event(
                action="security_scan",
                plugin_name=plugin_path.name,
                success=False,
                error_message=str(e)
            )
        
        return results
    
    def _analyze_ast(self, tree: ast.AST, results: Dict[str, Any]) -> None:
        """Analyze AST for security issues."""
        for node in ast.walk(tree):
            # Check imports
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                self._check_import(node, results)
            
            # Check function calls
            elif isinstance(node, ast.Call):
                self._check_function_call(node, results)
            
            # Check attribute access
            elif isinstance(node, ast.Attribute):
                self._check_attribute_access(node, results)
            
            # Check function definitions
            elif isinstance(node, ast.FunctionDef):
                results['functions'].append(node.name)
                self._check_function_definition(node, results)
            
            # Check class definitions
            elif isinstance(node, ast.ClassDef):
                results['classes'].append(node.name)
                self._check_class_definition(node, results)
            
            # Check exec/eval usage
            elif isinstance(node, ast.Expr) and isinstance(node.value, ast.Call):
                if hasattr(node.value.func, 'id'):
                    if node.value.func.id in ['exec', 'eval', 'compile']:
                        results['issues'].append({
                            'type': 'dangerous_function',
                            'message': f"Use of dangerous function: {node.value.func.id}",
                            'severity': 'critical',
                            'line': node.lineno
                        })
    
    def _check_import(self, node: ast.AST, results: Dict[str, Any]) -> None:
        """Check import statements for security issues."""
        if isinstance(node, ast.Import):
            for alias in node.names:
                module_name = alias.name
                results['imports'].append(module_name)
                
                # Check against suspicious modules
                if module_name in self.suspicious_modules:
                    if module_name not in self.allowed_modules:
                        results['issues'].append({
                            'type': 'suspicious_import',
                            'message': f"Suspicious module import: {module_name}",
                            'severity': 'high',
                            'line': node.lineno
                        })
                    else:
                        results['warnings'].append({
                            'type': 'monitored_import',
                            'message': f"Monitored module import: {module_name}",
                            'line': node.lineno
                        })
        
        elif isinstance(node, ast.ImportFrom):
            module_name = node.module or ''
            
            for alias in node.names:
                import_name = f"{module_name}.{alias.name}" if module_name else alias.name
                results['imports'].append(import_name)
                
                # Check for dangerous function imports
                if import_name in self.dangerous_imports:
                    results['issues'].append({
                        'type': 'dangerous_import',
                        'message': f"Dangerous function import: {import_name}",
                        'severity': 'critical',
                        'line': node.lineno
                    })
                
                # Check module
                if module_name in self.suspicious_modules and module_name not in self.allowed_modules:
                    results['issues'].append({
                        'type': 'suspicious_module',
                        'message': f"Import from suspicious module: {module_name}",
                        'severity': 'high',
                        'line': node.lineno
                    })
    
    def _check_function_call(self, node: ast.Call, results: Dict[str, Any]) -> None:
        """Check function calls for security issues."""
        func_name = None
        
        # Get function name
        if hasattr(node.func, 'id'):
            func_name = node.func.id
        elif hasattr(node.func, 'attr'):
            func_name = node.func.attr
        elif isinstance(node.func, ast.Attribute):
            if hasattr(node.func.value, 'id'):
                func_name = f"{node.func.value.id}.{node.func.attr}"
        
        if func_name:
            # Check for dangerous functions
            if func_name in self.dangerous_imports:
                results['issues'].append({
                    'type': 'dangerous_call',
                    'message': f"Call to dangerous function: {func_name}",
                    'severity': 'critical',
                    'line': node.lineno
                })
            
            # Check for file operations
            elif any(op in func_name.lower() for op in self.file_ops):
                results['warnings'].append({
                    'type': 'file_operation',
                    'message': f"File operation detected: {func_name}",
                    'line': node.lineno
                })
            
            # Check for network operations
            elif any(op in func_name.lower() for op in self.network_ops):
                results['warnings'].append({
                    'type': 'network_operation',
                    'message': f"Network operation detected: {func_name}",
                    'line': node.lineno
                })
    
    def _check_attribute_access(self, node: ast.Attribute, results: Dict[str, Any]) -> None:
        """Check attribute access for security issues."""
        # Check for __class__, __bases__, etc.
        if node.attr.startswith('__') and node.attr.endswith('__'):
            results['warnings'].append({
                'type': 'dunder_access',
                'message': f"Access to dunder attribute: {node.attr}",
                'line': node.lineno
            })
    
    def _check_function_definition(self, node: ast.FunctionDef, results: Dict[str, Any]) -> None:
        """Check function definitions for security issues."""
        # Check for suspicious function names
        suspicious_names = ['exec', 'eval', 'system', 'spawn', 'shell']
        
        if any(name in node.name.lower() for name in suspicious_names):
            results['warnings'].append({
                'type': 'suspicious_function_name',
                'message': f"Suspicious function name: {node.name}",
                'line': node.lineno
            })
        
        # Check for functions that override security methods
        security_methods = ['validate', 'authorize', 'authenticate', 'permission']
        
        if any(method in node.name.lower() for method in security_methods):
            results['warnings'].append({
                'type': 'security_override',
                'message': f"Function overrides security method: {node.name}",
                'line': node.lineno
            })
    
    def _check_class_definition(self, node: ast.ClassDef, results: Dict[str, Any]) -> None:
        """Check class definitions for security issues."""
        # Check for suspicious base classes
        for base in node.bases:
            if hasattr(base, 'id'):
                base_name = base.id
                
                # Check for metaclass manipulation
                if 'meta' in base_name.lower() or 'type' in base_name.lower():
                    results['warnings'].append({
                        'type': 'metaclass_usage',
                        'message': f"Potential metaclass manipulation: {base_name}",
                        'line': node.lineno
                    })
    
    def _analyze_source_patterns(self, source_code: str, results: Dict[str, Any]) -> None:
        """Analyze source code for dangerous patterns."""
        lines = source_code.split('\n')
        
        dangerous_patterns = [
            (r'exec\s*\(', 'exec function call'),
            (r'eval\s*\(', 'eval function call'),
            (r'__import__\s*\(', '__import__ function call'),
            (r'subprocess\.', 'subprocess usage'),
            (r'os\.system', 'os.system call'),
            (r'os\.popen', 'os.popen call'),
            (r'pickle\.loads', 'pickle.loads usage'),
            (r'marshal\.loads', 'marshal.loads usage'),
            (r'socket\.', 'socket usage'),
            (r'urllib\.request', 'urllib.request usage'),
            (r'http\.client', 'http.client usage'),
            (r'ctypes\.', 'ctypes usage'),
            (r'sys\.exit', 'sys.exit call'),
            (r'quit\s*\(', 'quit function call'),
            (r'exit\s*\(', 'exit function call'),
        ]
        
        import re
        
        for line_num, line in enumerate(lines, 1):
            for pattern, description in dangerous_patterns:
                if re.search(pattern, line, re.IGNORECASE):
                    results['issues'].append({
                        'type': 'dangerous_pattern',
                        'message': f"Dangerous pattern detected: {description}",
                        'severity': 'high',
                        'line': line_num,
                        'code': line.strip()
                    })
    
    def _calculate_risk_level(self, results: Dict[str, Any]) -> None:
        """Calculate overall risk level based on issues."""
        critical_count = sum(1 for issue in results['issues'] if issue.get('severity') == 'critical')
        high_count = sum(1 for issue in results['issues'] if issue.get('severity') == 'high')
        warning_count = len(results['warnings'])
        
        # Determine risk level
        if critical_count > 0:
            results['risk_level'] = 'critical'
            results['allowed'] = False
        elif high_count >= 3:
            results['risk_level'] = 'high'
            results['allowed'] = False
        elif high_count > 0 or warning_count >= 5:
            results['risk_level'] = 'medium'
            results['allowed'] = True  # Allow with warnings
        else:
            results['risk_level'] = 'low'
            results['allowed'] = True
    
    def validate_plugin_structure(self, plugin_path: Path) -> Tuple[bool, List[str]]:
        """
        Validate plugin structure and required components.
        
        Args:
            plugin_path: Path to plugin file
            
        Returns:
            Tuple of (is_valid, error_messages)
        """
        errors = []
        
        try:
            # Check file exists and is readable
            if not plugin_path.exists():
                errors.append(f"Plugin file does not exist: {plugin_path}")
                return False, errors
            
            if not plugin_path.is_file():
                errors.append(f"Plugin path is not a file: {plugin_path}")
                return False, errors
            
            # Check file extension
            if plugin_path.suffix != '.py':
                errors.append(f"Plugin must be a Python file (.py): {plugin_path}")
                return False, errors
            
            # Try to parse the file
            with open(plugin_path, 'r', encoding='utf-8') as f:
                source = f.read()
            
            try:
                ast.parse(source)
            except SyntaxError as e:
                errors.append(f"Plugin has syntax errors: {e}")
                return False, errors
            
            # Check for required plugin metadata
            if '__plugin_info__' not in source:
                errors.append("Plugin missing required __plugin_info__ metadata")
            
            # Check for required plugin class/function
            tree = ast.parse(source)
            has_class = any(isinstance(node, ast.ClassDef) for node in ast.walk(tree))
            has_function = any(isinstance(node, ast.FunctionDef) 
                             for node in ast.walk(tree) 
                             if isinstance(node, ast.FunctionDef) and 
                             node.name in ['initialize_plugin', 'cleanup_plugin'])
            
            if not has_class and not has_function:
                errors.append("Plugin must contain either a plugin class or required functions")
            
            return len(errors) == 0, errors
            
        except Exception as e:
            errors.append(f"Failed to validate plugin structure: {e}")
            return False, errors
    
    def generate_security_report(self, scan_results: Dict[str, Any]) -> str:
        """
        Generate a human-readable security report.
        
        Args:
            scan_results: Results from security scan
            
        Returns:
            Formatted security report
        """
        report_lines = []
        
        report_lines.append(f"Security Scan Report for: {scan_results['plugin_path']}")
        report_lines.append("=" * 60)
        
        # Risk level
        risk_level = scan_results['risk_level'].upper()
        allowed = "ALLOWED" if scan_results['allowed'] else "BLOCKED"
        report_lines.append(f"Risk Level: {risk_level}")
        report_lines.append(f"Status: {allowed}")
        report_lines.append("")
        
        # Issues
        if scan_results['issues']:
            report_lines.append("SECURITY ISSUES:")
            for issue in scan_results['issues']:
                severity = issue.get('severity', 'unknown').upper()
                message = issue.get('message', 'Unknown issue')
                line = issue.get('line', 'N/A')
                report_lines.append(f"  [{severity}] Line {line}: {message}")
            report_lines.append("")
        
        # Warnings
        if scan_results['warnings']:
            report_lines.append("WARNINGS:")
            for warning in scan_results['warnings']:
                message = warning.get('message', 'Unknown warning')
                line = warning.get('line', 'N/A')
                report_lines.append(f"  [WARNING] Line {line}: {message}")
            report_lines.append("")
        
        # Summary
        report_lines.append("SUMMARY:")
        report_lines.append(f"  Total Issues: {len(scan_results['issues'])}")
        report_lines.append(f"  Total Warnings: {len(scan_results['warnings'])}")
        report_lines.append(f"  Imports: {len(scan_results['imports'])}")
        report_lines.append(f"  Functions: {len(scan_results['functions'])}")
        report_lines.append(f"  Classes: {len(scan_results['classes'])}")
        
        return "\n".join(report_lines)