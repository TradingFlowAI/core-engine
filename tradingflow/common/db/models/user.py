from sqlalchemy import Column, DateTime, Integer, String
from sqlalchemy.sql import func

from tradingflow.common.db.base import Base


class User(Base):
    __tablename__ = "users"

    # 使用自增主键
    id = Column(Integer, primary_key=True, index=True)

    # 钱包地址设为唯一索引
    wallet_address = Column(String, unique=True, index=True, nullable=False)

    # 可选的用户信息
    nickname = Column(String, nullable=True)
    email = Column(String, nullable=True, index=True)

    # 记录时间
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )
    # 上次登录时间
    last_login_at = Column(DateTime(timezone=True), nullable=True)
