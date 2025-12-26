"""
Version Manager
Manages node versions following semantic versioning (npm style)
"""

import re
from typing import List, Tuple, Optional
from functools import cmp_to_key


class VersionManager:
    """
    Version Manager - Follows semantic versioning (with prerelease support)
    
    Version format: MAJOR.MINOR.PATCH[-PRERELEASE][+BUILD]
    - MAJOR: Incompatible API changes
    - MINOR: Backward compatible new features
    - PATCH: Backward compatible bug fixes
    - PRERELEASE: Prerelease identifier (alpha, beta, rc, preview)
    - BUILD: Build metadata (optional)
    
    Supported prerelease tag priority (lowest to highest):
    alpha < beta < rc < preview < (stable)
    
    Examples:
        >>> vm = VersionManager()
        >>> vm.parse_version("1.2.3")
        (1, 2, 3, None, None)
        >>> vm.parse_version_with_prerelease("1.2.3-beta.1")
        (1, 2, 3, 'beta.1', None)
        >>> vm.compare_versions("1.2.3-beta.1", "1.2.3")
        -1
        >>> vm.get_latest_version(["1.0.0", "1.2.3-beta.1", "1.2.3"])
        "1.2.3"
    """
    
    # Basic version format: X.Y.Z
    VERSION_PATTERN = re.compile(r'^(\d+)\.(\d+)\.(\d+)$')
    
    # Full version format: X.Y.Z[-PRERELEASE][+BUILD]
    FULL_VERSION_PATTERN = re.compile(
        r'^(\d+)\.(\d+)\.(\d+)'
        r'(?:-((?:alpha|beta|rc|preview)(?:\.\d+)?))?' 
        r'(?:\+([0-9A-Za-z-.]+))?$'
    )
    
    # Prerelease tag priority
    PRERELEASE_PRIORITY = {
        'alpha': 1,
        'beta': 2,
        'rc': 3,
        'preview': 4,
        # Stable version (no prerelease tag) has highest priority
        None: 5
    }
    
    @staticmethod
    def parse_version(version_str: str) -> Tuple[int, int, int]:
        """
        Parse basic version string (only X.Y.Z format).
        
        Args:
            version_str: Version string, e.g., "1.2.3"
            
        Returns:
            (major, minor, patch) tuple
            
        Raises:
            ValueError: If version format is invalid
        """
        # Try full format parsing first, extract basic version
        full_match = VersionManager.FULL_VERSION_PATTERN.match(version_str)
        if full_match:
            major, minor, patch = map(int, full_match.groups()[:3])
            return (major, minor, patch)
        
        # Fallback to basic format
        match = VersionManager.VERSION_PATTERN.match(version_str)
        if not match:
            raise ValueError(f"Invalid version format: {version_str}. Expected format: X.Y.Z[-PRERELEASE][+BUILD]")
        
        major, minor, patch = map(int, match.groups())
        return (major, minor, patch)
    
    @staticmethod
    def parse_version_with_prerelease(version_str: str) -> Tuple[int, int, int, Optional[str], Optional[str]]:
        """
        Parse full version string (including prerelease tag and build metadata).
        
        Args:
            version_str: Version string, e.g., "1.2.3-beta.1+build.123"
            
        Returns:
            (major, minor, patch, prerelease, build) tuple
            
        Raises:
            ValueError: If version format is invalid
            
        Examples:
            >>> parse_version_with_prerelease("1.0.0")
            (1, 0, 0, None, None)
            >>> parse_version_with_prerelease("1.2.3-beta.1")
            (1, 2, 3, 'beta.1', None)
            >>> parse_version_with_prerelease("2.0.0-rc.1+build.456")
            (2, 0, 0, 'rc.1', 'build.456')
        """
        match = VersionManager.FULL_VERSION_PATTERN.match(version_str)
        if not match:
            raise ValueError(
                f"Invalid version format: {version_str}. "
                f"Expected format: X.Y.Z[-PRERELEASE][+BUILD]"
            )
        
        major, minor, patch, prerelease, build = match.groups()
        return (
            int(major),
            int(minor),
            int(patch),
            prerelease if prerelease else None,
            build if build else None
        )
    
    @staticmethod
    def compare_versions(v1: str, v2: str) -> int:
        """
        Compare two version numbers (with prerelease support).
        
        Comparison rules:
        1. Compare main version first (MAJOR.MINOR.PATCH)
        2. If main versions are equal, compare prerelease tags
        3. Prerelease version < Stable version
        4. Prerelease tag priority: alpha < beta < rc < preview < stable
        
        Args:
            v1: Version 1 (supports prerelease, e.g., "1.2.3-beta.1")
            v2: Version 2
            
        Returns:
            -1 if v1 < v2
             0 if v1 == v2
             1 if v1 > v2
             
        Examples:
            >>> compare_versions("1.0.0", "1.0.1")
            -1
            >>> compare_versions("1.2.3-beta.1", "1.2.3")
            -1
            >>> compare_versions("1.2.3-alpha.1", "1.2.3-beta.1")
            -1
        """
        try:
            # Parse full version info
            major1, minor1, patch1, pre1, _ = VersionManager.parse_version_with_prerelease(v1)
            major2, minor2, patch2, pre2, _ = VersionManager.parse_version_with_prerelease(v2)
            
            # Compare main version
            if (major1, minor1, patch1) < (major2, minor2, patch2):
                return -1
            elif (major1, minor1, patch1) > (major2, minor2, patch2):
                return 1
            
            # Main version equal, compare prerelease tags
            # Extract base type from prerelease tag (e.g., "beta.1" -> "beta")
            pre1_type = VersionManager._extract_prerelease_type(pre1)
            pre2_type = VersionManager._extract_prerelease_type(pre2)
            
            priority1 = VersionManager.PRERELEASE_PRIORITY.get(pre1_type, 5)
            priority2 = VersionManager.PRERELEASE_PRIORITY.get(pre2_type, 5)
            
            if priority1 < priority2:
                return -1
            elif priority1 > priority2:
                return 1
            
            # Same prerelease type, compare sequence number (e.g., beta.1 vs beta.2)
            if pre1 and pre2 and pre1_type == pre2_type:
                num1 = VersionManager._extract_prerelease_number(pre1)
                num2 = VersionManager._extract_prerelease_number(pre2)
                if num1 < num2:
                    return -1
                elif num1 > num2:
                    return 1
            
            return 0
            
        except ValueError as e:
            raise ValueError(f"Cannot compare versions: {e}")
    
    @staticmethod
    def _extract_prerelease_type(prerelease: Optional[str]) -> Optional[str]:
        """
        Extract prerelease tag type.
        
        Examples:
            >>> _extract_prerelease_type("beta.1")
            "beta"
            >>> _extract_prerelease_type("alpha")
            "alpha"
            >>> _extract_prerelease_type(None)
            None
        """
        if not prerelease:
            return None
        
        # Extract tag type (e.g., "beta.1" -> "beta")
        parts = prerelease.split('.')
        return parts[0] if parts else None
    
    @staticmethod
    def _extract_prerelease_number(prerelease: str) -> int:
        """
        Extract prerelease version number.
        
        Examples:
            >>> _extract_prerelease_number("beta.1")
            1
            >>> _extract_prerelease_number("alpha.10")
            10
            >>> _extract_prerelease_number("rc")
            0
        """
        parts = prerelease.split('.')
        if len(parts) > 1 and parts[1].isdigit():
            return int(parts[1])
        return 0
    
    @staticmethod
    def get_latest_version(versions: List[str], include_prerelease: bool = False) -> Optional[str]:
        """
        Get the latest version from a version list.
        
        Args:
            versions: List of versions
            include_prerelease: Whether to include prerelease versions (default False, returns stable only)
            
        Returns:
            Latest version string, None if list is empty
            
        Examples:
            >>> versions = ["1.0.0", "1.2.3-beta.1", "1.2.3", "1.2.4-alpha.1"]
            >>> get_latest_version(versions)
            "1.2.3"
            >>> get_latest_version(versions, include_prerelease=True)
            "1.2.4-alpha.1"
        """
        if not versions:
            return None
        
        # Filter valid versions
        valid_versions = []
        for v in versions:
            try:
                major, minor, patch, prerelease, _ = VersionManager.parse_version_with_prerelease(v)
                
                # If not including prerelease, filter them out
                if not include_prerelease and prerelease is not None:
                    continue
                
                valid_versions.append(v)
            except ValueError:
                continue
        
        if not valid_versions:
            return None
        
        # Sort and return the largest version
        sorted_versions = sorted(
            valid_versions,
            key=cmp_to_key(VersionManager.compare_versions),
            reverse=True
        )
        return sorted_versions[0]
    
    @staticmethod
    def get_latest_prerelease_version(versions: List[str], prerelease_type: str = None) -> Optional[str]:
        """
        Get the latest prerelease version from a version list.
        
        Args:
            versions: List of versions
            prerelease_type: Prerelease type filter (alpha, beta, rc, preview), None for all prerelease
            
        Returns:
            Latest prerelease version string, None if not found
            
        Examples:
            >>> versions = ["1.0.0", "1.2.3-beta.1", "1.2.3-beta.2", "1.2.4-alpha.1"]
            >>> get_latest_prerelease_version(versions, "beta")
            "1.2.3-beta.2"
            >>> get_latest_prerelease_version(versions)
            "1.2.4-alpha.1"
        """
        if not versions:
            return None
        
        # Filter matching prerelease versions
        filtered_versions = []
        for v in versions:
            try:
                major, minor, patch, prerelease, _ = VersionManager.parse_version_with_prerelease(v)
                
                # Must have prerelease tag
                if prerelease is None:
                    continue
                
                # If type specified, filter by type
                if prerelease_type:
                    pre_type = VersionManager._extract_prerelease_type(prerelease)
                    if pre_type != prerelease_type:
                        continue
                
                filtered_versions.append(v)
            except ValueError:
                continue
        
        if not filtered_versions:
            return None
        
        # Sort and return the largest version
        sorted_versions = sorted(
            filtered_versions,
            key=cmp_to_key(VersionManager.compare_versions),
            reverse=True
        )
        return sorted_versions[0]
    
    @staticmethod
    def increment_version(version: str, level: str = 'patch') -> str:
        """
        Increment version number.
        
        Args:
            version: Current version
            level: Increment level ('major', 'minor', 'patch')
            
        Returns:
            New version number
        """
        major, minor, patch = VersionManager.parse_version(version)
        
        if level == 'major':
            return f"{major + 1}.0.0"
        elif level == 'minor':
            return f"{major}.{minor + 1}.0"
        elif level == 'patch':
            return f"{major}.{minor}.{patch + 1}"
        else:
            raise ValueError(f"Invalid level: {level}. Must be 'major', 'minor', or 'patch'")
    
    @staticmethod
    def is_compatible(required_version: str, available_version: str, 
                     compatibility: str = 'minor') -> bool:
        """
        Check version compatibility.
        
        Args:
            required_version: Required version
            available_version: Available version
            compatibility: Compatibility level ('major', 'minor', 'patch')
            
        Returns:
            Whether compatible
        """
        req_major, req_minor, req_patch = VersionManager.parse_version(required_version)
        avail_major, avail_minor, avail_patch = VersionManager.parse_version(available_version)
        
        if compatibility == 'major':
            # Major version must match
            return avail_major == req_major and (
                avail_minor > req_minor or 
                (avail_minor == req_minor and avail_patch >= req_patch)
            )
        elif compatibility == 'minor':
            # Major and minor version must match
            return (avail_major == req_major and 
                   avail_minor == req_minor and 
                   avail_patch >= req_patch)
        elif compatibility == 'patch':
            # Exact match
            return (avail_major == req_major and 
                   avail_minor == req_minor and 
                   avail_patch == req_patch)
        else:
            raise ValueError(f"Invalid compatibility level: {compatibility}")
    
    @staticmethod
    def resolve_version_spec(version_spec: str, available_versions: List[str]) -> Optional[str]:
        """
        Resolve version spec to specific version number (supports 'latest' keyword).
        
        Supported patterns:
            - "latest": Latest stable version
            - "latest-alpha": Latest alpha version
            - "latest-beta": Latest beta version
            - "latest-rc": Latest rc version
            - "latest-preview": Latest preview version
            - "1.2.3": Exact version (no resolution, returns as-is)
            - "^1.2.3", "~1.2.3", etc.: Range expression (returns latest matching version)
            
        Args:
            version_spec: Version spec string
            available_versions: List of available versions
            
        Returns:
            Resolved specific version, None if cannot resolve
            
        Examples:
            >>> versions = ["1.0.0", "1.2.3", "1.2.4-beta.1", "1.3.0-alpha.1"]
            >>> resolve_version_spec("latest", versions)
            "1.2.3"
            >>> resolve_version_spec("latest-beta", versions)
            "1.2.4-beta.1"
            >>> resolve_version_spec("^1.2.0", versions)
            "1.2.3"
        """
        spec = version_spec.strip()
        
        # Handle "latest" keyword
        if spec == "latest":
            return VersionManager.get_latest_version(available_versions, include_prerelease=False)
        
        # Handle "latest-{prerelease_type}" keyword
        if spec.startswith("latest-"):
            prerelease_type = spec[7:]  # Remove "latest-" prefix
            if prerelease_type in ['alpha', 'beta', 'rc', 'preview']:
                return VersionManager.get_latest_prerelease_version(available_versions, prerelease_type)
            else:
                raise ValueError(f"Unknown prerelease type: {prerelease_type}")
        
        # If exact version, return directly (if exists)
        try:
            VersionManager.parse_version_with_prerelease(spec)
            if spec in available_versions:
                return spec
            else:
                return None
        except ValueError:
            pass
        
        # Handle range expressions (^, ~, >=, etc.)
        matched_versions = []
        for v in available_versions:
            try:
                if VersionManager.match_version_range(v, spec):
                    matched_versions.append(v)
            except (ValueError, AttributeError):
                continue
        
        if matched_versions:
            # Return latest version within range
            return VersionManager.get_latest_version(matched_versions, include_prerelease=True)
        
        return None
    
    @staticmethod
    def match_version_range(version: str, range_spec: str) -> bool:
        """
        Match version range (supports npm-style range expressions with prerelease).
        
        Supported patterns:
            - "1.2.3": Exact match
            - "1.2.3-beta.1": Exact match (with prerelease)
            - "^1.2.3": Compatible with 1.x.x (same major version)
            - "~1.2.3": Compatible with 1.2.x (same major and minor version)
            - ">=1.2.3": Greater than or equal
            - "<=1.2.3": Less than or equal
            - ">1.2.3": Greater than
            - "<1.2.3": Less than
            
        Args:
            version: Version to check (may contain prerelease tag)
            range_spec: Range expression
            
        Returns:
            Whether in range
        """
        range_spec = range_spec.strip()
        
        # Exact match (supports prerelease)
        try:
            VersionManager.parse_version_with_prerelease(range_spec)
            return version == range_spec
        except ValueError:
            pass
        
        # ^ symbol: Compatible with major version
        if range_spec.startswith('^'):
            target = range_spec[1:]
            return VersionManager.is_compatible(target, version, 'major')
        
        # ~ symbol: Compatible with minor version
        if range_spec.startswith('~'):
            target = range_spec[1:]
            return VersionManager.is_compatible(target, version, 'minor')
        
        # >= symbol
        if range_spec.startswith('>='):
            target = range_spec[2:].strip()
            return VersionManager.compare_versions(version, target) >= 0
        
        # <= symbol
        if range_spec.startswith('<='):
            target = range_spec[2:].strip()
            return VersionManager.compare_versions(version, target) <= 0
        
        # > symbol
        if range_spec.startswith('>'):
            target = range_spec[1:].strip()
            return VersionManager.compare_versions(version, target) > 0
        
        # < symbol
        if range_spec.startswith('<'):
            target = range_spec[1:].strip()
            return VersionManager.compare_versions(version, target) < 0
        
        raise ValueError(f"Unsupported version range: {range_spec}")
    
    @staticmethod
    def validate_version(version: str) -> bool:
        """
        Validate version format (supports prerelease).
        
        Args:
            version: Version string (may contain prerelease tag and build metadata)
            
        Returns:
            Whether valid
            
        Examples:
            >>> validate_version("1.2.3")
            True
            >>> validate_version("1.2.3-beta.1")
            True
            >>> validate_version("1.2.3-rc.1+build.123")
            True
            >>> validate_version("invalid")
            False
        """
        try:
            VersionManager.parse_version_with_prerelease(version)
            return True
        except ValueError:
            return False
    
    @staticmethod
    def version_to_filename(version: str) -> str:
        """
        Convert version to filename format.
        
        Args:
            version: Version string (e.g., "1.2.3-beta.1")
            
        Returns:
            Filename format (e.g., "v1_2_3_beta1")
            
        Examples:
            >>> version_to_filename("1.2.3")
            "v1_2_3"
            >>> version_to_filename("1.2.3-beta.1")
            "v1_2_3_beta1"
            >>> version_to_filename("2.0.0-rc.2")
            "v2_0_0_rc2"
        """
        try:
            major, minor, patch, prerelease, _ = VersionManager.parse_version_with_prerelease(version)
            
            # Basic version part
            filename = f"v{major}_{minor}_{patch}"
            
            # Add prerelease tag
            if prerelease:
                # Remove dots, e.g., "beta.1" -> "beta1"
                pre_tag = prerelease.replace('.', '')
                filename += f"_{pre_tag}"
            
            return filename
        except ValueError as e:
            raise ValueError(f"Invalid version format: {version}. {e}")
    
    @staticmethod
    def filename_to_version(filename: str) -> str:
        """
        Convert filename format to version.
        
        Args:
            filename: Filename (e.g., "v1_2_3_beta1.py" or "v1_2_3")
            
        Returns:
            Version string (e.g., "1.2.3-beta.1")
            
        Examples:
            >>> filename_to_version("v1_2_3.py")
            "1.2.3"
            >>> filename_to_version("v1_2_3_beta1")
            "1.2.3-beta.1"
            >>> filename_to_version("v2_0_0_rc2.py")
            "2.0.0-rc.2"
        """
        # Remove .py suffix and v prefix
        name = filename.replace('.py', '').replace('v', '', 1)
        
        # Split parts
        parts = name.split('_')
        
        if len(parts) < 3:
            raise ValueError(f"Invalid filename format: {filename}")
        
        # Extract basic version
        major, minor, patch = parts[0:3]
        version = f"{major}.{minor}.{patch}"
        
        # Extract prerelease tag
        if len(parts) > 3:
            pre_tag = '_'.join(parts[3:])
            
            # Identify prerelease type and sequence number
            for tag_type in ['alpha', 'beta', 'rc', 'preview']:
                if pre_tag.startswith(tag_type):
                    number_part = pre_tag[len(tag_type):]
                    if number_part:
                        version += f"-{tag_type}.{number_part}"
                    else:
                        version += f"-{tag_type}"
                    break
        
        return version
