"""
Version Manager
Manages node versions following semantic versioning (npm style)
"""

import re
from typing import List, Tuple, Optional
from functools import cmp_to_key


class VersionManager:
    """
    版本管理器 - 遵循语义化版本规范
    
    版本格式: MAJOR.MINOR.PATCH
    - MAJOR: 不兼容的 API 修改
    - MINOR: 向下兼容的功能性新增
    - PATCH: 向下兼容的问题修正
    
    Examples:
        >>> vm = VersionManager()
        >>> vm.parse_version("1.2.3")
        (1, 2, 3)
        >>> vm.compare_versions("1.2.3", "1.2.4")
        -1
        >>> vm.get_latest_version(["1.0.0", "1.2.3", "0.9.0"])
        "1.2.3"
    """
    
    VERSION_PATTERN = re.compile(r'^(\d+)\.(\d+)\.(\d+)$')
    
    @staticmethod
    def parse_version(version_str: str) -> Tuple[int, int, int]:
        """
        解析版本号字符串
        
        Args:
            version_str: 版本字符串，如 "1.2.3"
            
        Returns:
            (major, minor, patch) 元组
            
        Raises:
            ValueError: 如果版本格式不正确
        """
        match = VersionManager.VERSION_PATTERN.match(version_str)
        if not match:
            raise ValueError(f"Invalid version format: {version_str}. Expected format: X.Y.Z")
        
        major, minor, patch = map(int, match.groups())
        return (major, minor, patch)
    
    @staticmethod
    def compare_versions(v1: str, v2: str) -> int:
        """
        比较两个版本号
        
        Args:
            v1: 版本1
            v2: 版本2
            
        Returns:
            -1 if v1 < v2
             0 if v1 == v2
             1 if v1 > v2
        """
        try:
            t1 = VersionManager.parse_version(v1)
            t2 = VersionManager.parse_version(v2)
            
            if t1 < t2:
                return -1
            elif t1 > t2:
                return 1
            else:
                return 0
        except ValueError as e:
            raise ValueError(f"Cannot compare versions: {e}")
    
    @staticmethod
    def get_latest_version(versions: List[str]) -> Optional[str]:
        """
        从版本列表中获取最新版本
        
        Args:
            versions: 版本列表
            
        Returns:
            最新版本字符串，如果列表为空则返回 None
        """
        if not versions:
            return None
        
        # 过滤出有效版本
        valid_versions = []
        for v in versions:
            try:
                VersionManager.parse_version(v)
                valid_versions.append(v)
            except ValueError:
                continue
        
        if not valid_versions:
            return None
        
        # 排序并返回最大版本
        sorted_versions = sorted(
            valid_versions,
            key=cmp_to_key(VersionManager.compare_versions),
            reverse=True
        )
        return sorted_versions[0]
    
    @staticmethod
    def increment_version(version: str, level: str = 'patch') -> str:
        """
        增加版本号
        
        Args:
            version: 当前版本
            level: 增加级别 ('major', 'minor', 'patch')
            
        Returns:
            新版本号
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
        检查版本兼容性
        
        Args:
            required_version: 需要的版本
            available_version: 可用的版本
            compatibility: 兼容级别 ('major', 'minor', 'patch')
            
        Returns:
            是否兼容
        """
        req_major, req_minor, req_patch = VersionManager.parse_version(required_version)
        avail_major, avail_minor, avail_patch = VersionManager.parse_version(available_version)
        
        if compatibility == 'major':
            # 主版本号必须相同
            return avail_major == req_major and (
                avail_minor > req_minor or 
                (avail_minor == req_minor and avail_patch >= req_patch)
            )
        elif compatibility == 'minor':
            # 主版本和次版本号必须相同
            return (avail_major == req_major and 
                   avail_minor == req_minor and 
                   avail_patch >= req_patch)
        elif compatibility == 'patch':
            # 完全匹配
            return (avail_major == req_major and 
                   avail_minor == req_minor and 
                   avail_patch == req_patch)
        else:
            raise ValueError(f"Invalid compatibility level: {compatibility}")
    
    @staticmethod
    def match_version_range(version: str, range_spec: str) -> bool:
        """
        匹配版本范围 (支持 npm 风格的范围表达式)
        
        Supported patterns:
            - "1.2.3": 精确匹配
            - "^1.2.3": 兼容 1.x.x (主版本相同)
            - "~1.2.3": 兼容 1.2.x (主版本和次版本相同)
            - ">=1.2.3": 大于等于
            - "<=1.2.3": 小于等于
            - ">1.2.3": 大于
            - "<1.2.3": 小于
            
        Args:
            version: 要检查的版本
            range_spec: 范围表达式
            
        Returns:
            是否在范围内
        """
        range_spec = range_spec.strip()
        
        # 精确匹配
        if VersionManager.VERSION_PATTERN.match(range_spec):
            return version == range_spec
        
        # ^ 符号: 兼容主版本
        if range_spec.startswith('^'):
            target = range_spec[1:]
            return VersionManager.is_compatible(target, version, 'major')
        
        # ~ 符号: 兼容次版本
        if range_spec.startswith('~'):
            target = range_spec[1:]
            return VersionManager.is_compatible(target, version, 'minor')
        
        # >= 符号
        if range_spec.startswith('>='):
            target = range_spec[2:].strip()
            return VersionManager.compare_versions(version, target) >= 0
        
        # <= 符号
        if range_spec.startswith('<='):
            target = range_spec[2:].strip()
            return VersionManager.compare_versions(version, target) <= 0
        
        # > 符号
        if range_spec.startswith('>'):
            target = range_spec[1:].strip()
            return VersionManager.compare_versions(version, target) > 0
        
        # < 符号
        if range_spec.startswith('<'):
            target = range_spec[1:].strip()
            return VersionManager.compare_versions(version, target) < 0
        
        raise ValueError(f"Unsupported version range: {range_spec}")
    
    @staticmethod
    def validate_version(version: str) -> bool:
        """
        验证版本号格式是否正确
        
        Args:
            version: 版本字符串
            
        Returns:
            是否有效
        """
        try:
            VersionManager.parse_version(version)
            return True
        except ValueError:
            return False
