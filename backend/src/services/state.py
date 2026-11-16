from sqlalchemy import select
from src.models import Backup, SiteState
RESTORE_STATUSES = ('verifying', 'restoring')
def current_epoch(db) -> int:
    row = db.get(SiteState, 'epoch')
    return int(row.value) if row and row.value.isdigit() else 0
def bump_epoch(db) -> int:
    epoch = current_epoch(db) + 1
    row = db.get(SiteState, 'epoch')
    if row: row.value = str(epoch)
    else: db.add(SiteState(name='epoch', value=str(epoch)))
    return epoch
def restore_in_progress(db) -> Backup | None:
    return db.scalar(select(Backup).where(Backup.status.in_(RESTORE_STATUSES)).order_by(Backup.id).limit(1))
