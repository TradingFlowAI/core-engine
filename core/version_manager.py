"""
Version Manager
Manages node versions following semantic versioning (npm style)
"""

import re
from typing import List, Tuple, Optional
from functools import cmp_to_key


class VersionManager:
    """
    版本管理器 - 遵循语义化版本规范（扩展支持预发布版本）
    
    版本格式: MAJOR.MINOR.PATCH[-PRERELEASE][+BUILD]
    - MAJOR: 不兼容的 API 修改
    - MINOR: 向下兼容的功能性新增
    - PATCH: 向下兼容的问题修正
    - PRERELEASE: 预发布标识（alpha, beta, rc, preview）
    - BUILD: 构建元数据（可选）
    
    支持的预发布标签优先级（从低到高）:
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
    
    # 基础版本格式: X.Y.Z
    VERSION_PATTERN = re.compile(r'^(\d+)\.(\d+)\.(\d+)$')
    
    # 完整版本格式: X.Y.Z[-PRERELEASE][+BUILD]
    FULL_VERSION_PATTERN = re.compile(
        r'^(\d+)\.(\d+)\.(\d+)'
        r'(?:-((?:alpha|beta|rc|preview)(?:\.\d+)?))?' 
        r'(?:\+([0-9A-Za-z-.]+))?$'
    )
    
    # 预发布标签优先级
    PRERELEASE_PRIORITY = {
        'alpha': 1,
        'beta': 2,
        'rc': 3,
        'preview': 4,
        # stable版本（无预发布标签）优先级最高
        None: 5
    }
    
    @staticmethod
    def parse_version(version_str: str) -> Tuple[int, int, int]:
        """
        解析基础版本号字符串（仅支持 X.Y.Z 格式）
        
        Args:
            version_str: 版本字符串，如 "1.2.3"
            
        Returns:
            (major, minor, patch) 元组
            
        Raises:
            ValueError: 如果版本格式不正确
        """
        # 先尝试完整格式解析，提取基础版本
        full_match = VersionManager.FULL_VERSION_PATTERN.match(version_str)
        if full_match:
            major, minor, patch = map(int, full_match.groups()[:3])
            return (major, minor, patch)
        
        # 回退到基础格式
        match = VersionManager.VERSION_PATTERN.match(version_str)
        if not match:
            raise ValueError(f"Invalid version format: {version_str}. Expected format: X.Y.Z[-PRERELEASE][+BUILD]")
        
        major, minor, patch = map(int, match.groups())
        return (major, minor, patch)
    
    @staticmethod
    def parse_version_with_prerelease(version_str: str) -> Tuple[int, int, int, Optional[str], Optional[str]]:
        """
        解析完整版本号字符串（包含预发布标签和构建元数据）
        
        Args:
            version_str: 版本字符串，如 "1.2.3-beta.1+build.123"
            
        Returns:
            (major, minor, patch, prerelease, build) 元组
            
        Raises:
            ValueError: 如果版本格式不正确
            
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
        比较两个版本号（支持预发布版本）
        
        比较规则:
        1. 先比较主版本号 (MAJOR.MINOR.PATCH)
        2. 如果主版本号相同，比较预发布标签
        3. 预发布版本 < 稳定版本
        4. 预发布标签优先级: alpha < beta < rc < preview < stable
        
        Args:
            v1: 版本1 (支持预发布版本，如 "1.2.3-beta.1")
            v2: 版本2
            
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
            # 解析完整版本信息
            major1, minor1, patch1, pre1, _ = VersionManager.parse_version_with_prerelease(v1)
            major2, minor2, patch2, pre2, _ = VersionManager.parse_version_with_prerelease(v2)
            
            # 比较主版本号
            if (major1, minor1, patch1) < (major2, minor2, patch2):
                return -1
            elif (major1, minor1, patch1) > (major2, minor2, patch2):
                return 1
            
            # 主版本号相同，比较预发布标签
            # 获取预发布标签的基础类型（如 "beta.1" -> "beta"）
            pre1_type = VersionManager._extract_prerelease_type(pre1)
            pre2_type = VersionManager._extract_prerelease_type(pre2)
            
            priority1 = VersionManager.PRERELEASE_PRIORITY.get(pre1_type, 5)
            priority2 = VersionManager.PRERELEASE_PRIORITY.get(pre2_type, 5)
            
            if priority1 < priority2:
                return -1
            elif priority1 > priority2:
                return 1
            
            # 同类型预发布版本，比较序号（如 beta.1 vs beta.2）
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
        提取预发布标签类型
        
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
        
        # 提取标签类型（如 "beta.1" -> "beta"）
        parts = prerelease.split('.')
        return parts[0] if parts else None
    
    @staticmethod
    def _extract_prerelease_number(prerelease: str) -> int:
        """
        提取预发布版本序号
        
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
        从版本列表中获取最新版本
        
        Args:
            versions: 版本列表
            include_prerelease: 是否包含预发布版本（默认 False，只返回稳定版本）
            
        Returns:
            最新版本字符串，如果列表为空则返回 None
            
        Examples:
            >>> versions = ["1.0.0", "1.2.3-beta.1", "1.2.3", "1.2.4-alpha.1"]
            >>> get_latest_version(versions)
            "1.2.3"
            >>> get_latest_version(versions, include_prerelease=True)
            "1.2.4-alpha.1"
        """
        if not versions:
            return None
        
        # 过滤出有效版本
        valid_versions = []
        for v in versions:
            try:
                major, minor, patch, prerelease, _ = VersionManager.parse_version_with_prerelease(v)
                
                # 如果不包含预发布版本，过滤掉预发布版本
                if not include_prerelease and prerelease is not None:
                    continue
                
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
    def get_latest_prerelease_version(versions: List[str], prerelease_type: str = None) -> Optional[str]:
        """
        从版本列表中获取最新的预发布版本
        
        Args:
            versions: 版本列表
            prerelease_type: 预发布类型过滤 (alpha, beta, rc, preview)，None 表示所有预发布版本
            
        Returns:
            最新预发布版本字符串，如果没有则返回 None
            
        Examples:
            >>> versions = ["1.0.0", "1.2.3-beta.1", "1.2.3-beta.2", "1.2.4-alpha.1"]
            >>> get_latest_prerelease_version(versions, "beta")
            "1.2.3-beta.2"
            >>> get_latest_prerelease_version(versions)
            "1.2.4-alpha.1"
        """
        if not versions:
            return None
        
        # 过滤出符合条件的预发布版本
        filtered_versions = []
        for v in versions:
            try:
                major, minor, patch, prerelease, _ = VersionManager.parse_version_with_prerelease(v)
                
                # 必须有预发布标签
                if prerelease is None:
                    continue
                
                # 如果指定了类型，进行过滤
                if prerelease_type:
                    pre_type = VersionManager._extract_prerelease_type(prerelease)
                    if pre_type != prerelease_type:
                        continue
                
                filtered_versions.append(v)
            except ValueError:
                continue
        
        if not filtered_versions:
            return None
        
        # 排序并返回最大版本
        sorted_versions = sorted(
            filtered_versions,
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
    def resolve_version_spec(version_spec: str, available_versions: List[str]) -> Optional[str]:
        """
        解析版本规范到具体版本号（支持 latest 关键字）
        
        Supported patterns:
            - "latest": 最新稳定版本
            - "latest-alpha": 最新 alpha 版本
            - "latest-beta": 最新 beta 版本
            - "latest-rc": 最新 rc 版本
            - "latest-preview": 最新 preview 版本
            - "1.2.3": 精确版本（不解析，直接返回）
            - "^1.2.3", "~1.2.3", etc.: 范围表达式（返回匹配的最新版本）
            
        Args:
            version_spec: 版本规范字符串
            available_versions: 可用版本列表
            
        Returns:
            解析后的具体版本号，如果无法解析则返回 None
            
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
        
        # 处理 "latest" 关键字
        if spec == "latest":
            return VersionManager.get_latest_version(available_versions, include_prerelease=False)
        
        # 处理 "latest-{prerelease_type}" 关键字
        if spec.startswith("latest-"):
            prerelease_type = spec[7:]  # 去掉 "latest-" 前缀
            if prerelease_type in ['alpha', 'beta', 'rc', 'preview']:
                return VersionManager.get_latest_prerelease_version(available_versions, prerelease_type)
            else:
                raise ValueError(f"Unknown prerelease type: {prerelease_type}")
        
        # 如果是精确版本号，直接返回（如果存在）
        try:
            VersionManager.parse_version_with_prerelease(spec)
            if spec in available_versions:
                return spec
            else:
                return None
        except ValueError:
            pass
        
        # 处理范围表达式（^, ~, >=, etc.）
        matched_versions = []
        for v in available_versions:
            try:
                if VersionManager.match_version_range(v, spec):
                    matched_versions.append(v)
            except (ValueError, AttributeError):
                continue
        
        if matched_versions:
            # 返回匹配范围内的最新版本
            return VersionManager.get_latest_version(matched_versions, include_prerelease=True)
        
        return None
    
    @staticmethod
    def match_version_range(version: str, range_spec: str) -> bool:
        """
        匹配版本范围 (支持 npm 风格的范围表达式，支持预发布版本)
        
        Supported patterns:
            - "1.2.3": 精确匹配
            - "1.2.3-beta.1": 精确匹配（包含预发布版本）
            - "^1.2.3": 兼容 1.x.x (主版本相同)
            - "~1.2.3": 兼容 1.2.x (主版本和次版本相同)
            - ">=1.2.3": 大于等于
            - "<=1.2.3": 小于等于
            - ">1.2.3": 大于
            - "<1.2.3": 小于
            
        Args:
            version: 要检查的版本（可以包含预发布标签）
            range_spec: 范围表达式
            
        Returns:
            是否在范围内
        """
        range_spec = range_spec.strip()
        
        # 精确匹配（支持预发布版本）
        try:
            VersionManager.parse_version_with_prerelease(range_spec)
            return version == range_spec
        except ValueError:
            pass
        
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
        验证版本号格式是否正确（支持预发布版本）
        
        Args:
            version: 版本字符串（可以包含预发布标签和构建元数据）
            
        Returns:
            是否有效
            
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
        将版本号转换为文件名格式
        
        Args:
            version: 版本字符串（如 "1.2.3-beta.1"）
            
        Returns:
            文件名格式（如 "v1_2_3_beta1"）
            
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
            
            # 基础版本部分
            filename = f"v{major}_{minor}_{patch}"
            
            # 添加预发布标签
            if prerelease:
                # 移除点号，如 "beta.1" -> "beta1"
                pre_tag = prerelease.replace('.', '')
                filename += f"_{pre_tag}"
            
            return filename
        except ValueError as e:
            raise ValueError(f"Invalid version format: {version}. {e}")
    
    @staticmethod
    def filename_to_version(filename: str) -> str:
        """
        将文件名格式转换为版本号
        
        Args:
            filename: 文件名（如 "v1_2_3_beta1.py" 或 "v1_2_3"）
            
        Returns:
            版本字符串（如 "1.2.3-beta.1"）
            
        Examples:
            >>> filename_to_version("v1_2_3.py")
            "1.2.3"
            >>> filename_to_version("v1_2_3_beta1")
            "1.2.3-beta.1"
            >>> filename_to_version("v2_0_0_rc2.py")
            "2.0.0-rc.2"
        """
        # 移除 .py 后缀和 v 前缀
        name = filename.replace('.py', '').replace('v', '', 1)
        
        # 分割部分
        parts = name.split('_')
        
        if len(parts) < 3:
            raise ValueError(f"Invalid filename format: {filename}")
        
        # 提取基础版本
        major, minor, patch = parts[0:3]
        version = f"{major}.{minor}.{patch}"
        
        # 提取预发布标签
        if len(parts) > 3:
            pre_tag = '_'.join(parts[3:])
            
            # 识别预发布类型和序号
            for tag_type in ['alpha', 'beta', 'rc', 'preview']:
                if pre_tag.startswith(tag_type):
                    number_part = pre_tag[len(tag_type):]
                    if number_part:
                        version += f"-{tag_type}.{number_part}"
                    else:
                        version += f"-{tag_type}"
                    break
        
        return version
