"""
Version Validator - For Flow Linter

Provides version syntax validation and compatibility checking
"""

import re
from typing import List, Dict, Optional, Tuple
from .version_manager import VersionManager
from .node_registry import NodeRegistry


class VersionValidationError:
    """Version validation error"""
    
    def __init__(
        self,
        node_id: str,
        error_type: str,
        message: str,
        severity: str = 'error',  # 'error', 'warning', 'info'
        suggestion: Optional[str] = None
    ):
        self.node_id = node_id
        self.error_type = error_type
        self.message = message
        self.severity = severity
        self.suggestion = suggestion
    
    def to_dict(self) -> Dict:
        """Convert to dictionary."""
        return {
            'node_id': self.node_id,
            'error_type': self.error_type,
            'message': self.message,
            'severity': self.severity,
            'suggestion': self.suggestion
        }


class VersionValidator:
    """Version validator"""
    
    # Valid version spec patterns
    VERSION_PATTERNS = {
        'exact': r'^\d+\.\d+\.\d+(-[a-zA-Z0-9.-]+)?(\+[a-zA-Z0-9.-]+)?$',  # 1.2.3, 1.2.3-beta.1
        'latest': r'^latest(-[a-zA-Z]+)?$',  # latest, latest-beta
        'caret': r'^\^\d+\.\d+\.\d+$',  # ^1.2.0
        'tilde': r'^~\d+\.\d+\.\d+$',  # ~1.2.0
        'comparison': r'^(>=?|<=?|>|<)\d+\.\d+\.\d+$',  # >=1.0.0, <2.0.0
    }
    
    @classmethod
    def validate_version_syntax(cls, version_spec: str) -> Tuple[bool, Optional[str]]:
        """
        Validate version spec syntax.
        
        Args:
            version_spec: Version spec string
            
        Returns:
            (is_valid, error_message) tuple
        """
        if not version_spec or not isinstance(version_spec, str):
            return False, "Version specification is required"
        
        # Check if matches any valid pattern
        for pattern_type, pattern in cls.VERSION_PATTERNS.items():
            if re.match(pattern, version_spec):
                return True, None
        
        return False, (
            f"Invalid version specification: '{version_spec}'. "
            f"Expected format: '1.2.3', 'latest', '^1.2.0', '~1.2.0', etc."
        )
    
    @classmethod
    def validate_node_version(
        cls,
        node_id: str,
        node_type: str,
        version_spec: str
    ) -> List[VersionValidationError]:
        """
        Validate node version.
        
        Args:
            node_id: Node ID
            node_type: Node type
            version_spec: Version spec
            
        Returns:
            List of validation errors
        """
        errors = []
        
        # 1. Validate version syntax
        is_valid, error_msg = cls.validate_version_syntax(version_spec)
        if not is_valid:
            errors.append(VersionValidationError(
                node_id=node_id,
                error_type='invalid_syntax',
                message=error_msg,
                severity='error',
                suggestion="Use a valid version format like '1.2.3', 'latest', or '^1.2.0'"
            ))
            return errors  # Return immediately on syntax error, skip further checks
        
        # 2. Check if node type exists
        try:
            available_versions = NodeRegistry.get_all_versions(node_type)
        except KeyError:
            errors.append(VersionValidationError(
                node_id=node_id,
                error_type='unknown_node_type',
                message=f"Unknown node type: '{node_type}'",
                severity='error',
                suggestion=f"Check if the node type '{node_type}' is registered"
            ))
            return errors
        
        # 3. Check if versions exist
        if not available_versions:
            errors.append(VersionValidationError(
                node_id=node_id,
                error_type='no_versions',
                message=f"No versions available for node type '{node_type}'",
                severity='error',
                suggestion="Register at least one version for this node type"
            ))
            return errors
        
        # 4. Try to resolve version spec
        try:
            resolved_version = NodeRegistry.resolve_version(node_type, version_spec)
            if not resolved_version:
                errors.append(VersionValidationError(
                    node_id=node_id,
                    error_type='no_matching_version',
                    message=f"No version matching '{version_spec}' found for '{node_type}'",
                    severity='error',
                    suggestion=f"Available versions: {', '.join(available_versions)}"
                ))
        except ValueError as e:
            errors.append(VersionValidationError(
                node_id=node_id,
                error_type='version_resolution_error',
                message=str(e),
                severity='error',
                suggestion=f"Available versions: {', '.join(available_versions)}"
            ))
        
        # 5. Prerelease version warning
        if '-' in version_spec and not version_spec.startswith('latest'):
            errors.append(VersionValidationError(
                node_id=node_id,
                error_type='prerelease_version',
                message=f"Using prerelease version: '{version_spec}'",
                severity='warning',
                suggestion="Consider using a stable version for production"
            ))
        
        return errors
    
    @classmethod
    def validate_flow_versions(cls, flow_data: Dict) -> List[VersionValidationError]:
        """
        Validate versions for entire Flow.
        
        Args:
            flow_data: Flow JSON data
            
        Returns:
            List of validation errors
        """
        errors = []
        
        nodes = flow_data.get('nodes', [])
        for node in nodes:
            node_id = node.get('id')
            node_type = node.get('type') or node.get('nodeType')
            
            if not node_type:
                errors.append(VersionValidationError(
                    node_id=node_id,
                    error_type='missing_node_type',
                    message="Node type is missing",
                    severity='error'
                ))
                continue
            
            # Get version spec
            version_spec = None
            if 'version' in node:
                version_spec = node['version']
            elif 'data' in node and isinstance(node['data'], dict):
                version_spec = node['data'].get('version')
            
            # If no version specified, use default
            if not version_spec:
                version_spec = 'latest'
                # Add info hint
                errors.append(VersionValidationError(
                    node_id=node_id,
                    error_type='missing_version',
                    message=f"No version specified, using default: 'latest'",
                    severity='info',
                    suggestion="Consider specifying an explicit version for better stability"
                ))
            
            # Validate node version
            node_errors = cls.validate_node_version(node_id, node_type, version_spec)
            errors.extend(node_errors)
        
        return errors
    
    @classmethod
    def check_version_compatibility(
        cls,
        source_node: Dict,
        target_node: Dict
    ) -> List[VersionValidationError]:
        """
        Check version compatibility between connected nodes.
        
        Args:
            source_node: Source node data
            target_node: Target node data
            
        Returns:
            List of compatibility warnings
        """
        warnings = []
        
        # TODO: Implement specific compatibility check logic
        # e.g., Check if major versions are compatible
        # e.g., Check if one is prerelease and other is stable
        
        return warnings
