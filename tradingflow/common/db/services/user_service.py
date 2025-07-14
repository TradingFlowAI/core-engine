from typing import List, Optional

from sqlalchemy.orm import Session

from tradingflow.common.db.models.user import User
from tradingflow.common.db.models.vault_contract import VaultContract


class UserService:
    def __init__(self, db_session: Session):
        self.db = db_session

    def get_user_by_wallet(self, wallet_address: str) -> Optional[User]:
        """根据钱包地址获取用户"""
        return self.db.query(User).filter(User.wallet_address == wallet_address).first()

    def get_user_by_id(self, user_id: int) -> Optional[User]:
        """根据用户ID获取用户"""
        return self.db.query(User).filter(User.id == user_id).first()

    def create_user(
        self, wallet_address: str, nickname: str = None, email: str = None
    ) -> User:
        """创建新用户"""
        # 检查钱包地址是否已存在
        existing_user = self.get_user_by_wallet(wallet_address)
        if existing_user:
            return existing_user

        user = User(wallet_address=wallet_address, nickname=nickname, email=email)
        self.db.add(user)
        self.db.commit()
        self.db.refresh(user)
        return user

    def update_user(self, wallet_address: str, **kwargs) -> Optional[User]:
        """更新用户信息"""
        user = self.get_user_by_wallet(wallet_address)
        if not user:
            return None

        # 更新提供的字段
        for key, value in kwargs.items():
            if hasattr(user, key):
                setattr(user, key, value)

        self.db.commit()
        self.db.refresh(user)
        return user

    def get_deployed_vaults(self, wallet_address: str) -> List[VaultContract]:
        """获取用户部署的所有Vault"""
        user = self.get_user_by_wallet(wallet_address)
        if not user:
            return []
        return user.deployed_vaults.all()

    def get_invested_vaults(self, wallet_address: str) -> List[VaultContract]:
        """获取用户作为投资者的所有Vault"""
        user = self.get_user_by_wallet(wallet_address)
        if not user:
            return []
        return user.invested_vaults.all()

    def get_all_related_vaults(self, wallet_address: str) -> List[VaultContract]:
        """获取与用户相关的所有Vault(部署+投资)"""
        deployed = set(self.get_deployed_vaults(wallet_address))
        invested = set(self.get_invested_vaults(wallet_address))
        return list(deployed.union(invested))

    def get_vaults_by_chain(
        self, wallet_address: str, chain_id: int
    ) -> List[VaultContract]:
        """获取用户在特定链上的Vault"""
        user = self.get_user_by_wallet(wallet_address)
        if not user:
            return []

        deployed = user.deployed_vaults.filter(VaultContract.chain_id == chain_id).all()
        invested = user.invested_vaults.filter(VaultContract.chain_id == chain_id).all()
        return list(set(deployed + invested))

    def record_login(self, wallet_address: str) -> Optional[User]:
        """记录用户登录时间"""
        from datetime import datetime

        return self.update_user(wallet_address, last_login_at=datetime.now())
