from sqlalchemy import String, Double, JSON, Integer
from sqlalchemy.orm import Mapped, mapped_column
from src.config.database import Base
class Backup(Base):
    __tablename__ = 'backups'
    id: Mapped[int] = mapped_column(primary_key=True)
    created_at: Mapped[float] = mapped_column(Double)
    status: Mapped[str] = mapped_column(String(20), default='ready')
    error: Mapped[str] = mapped_column(String(1000), default='')
    filename: Mapped[str] = mapped_column(String(200), default='')
    checksum: Mapped[str] = mapped_column(String(64), default='')
    size_bytes: Mapped[int] = mapped_column(Integer, default=0)
    counts: Mapped[dict] = mapped_column(JSON, default=dict)
class SiteState(Base):
    __tablename__ = 'site_state'
    name: Mapped[str] = mapped_column(String(40), primary_key=True)
    value: Mapped[str] = mapped_column(String(200), default='')
