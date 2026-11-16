from fastapi import APIRouter, Depends
from sqlalchemy import select
from src.config.database import get_db
from src.models import Backup
from src.middlewares.auth import current_user, admin_only
from src.middlewares.maintenance import site_writable
from src.services import backup as svc
from src.utils.serialization import backup_dict
router = APIRouter(prefix='/backups')
@router.get('')
def listing(db=Depends(get_db), u=Depends(current_user)):
    return [backup_dict(b) for b in db.scalars(select(Backup).order_by(Backup.id.desc()))]
@router.post('', status_code=201)
def create(u=Depends(admin_only), _=Depends(site_writable)):
    return backup_dict(svc.create_backup(u.email))
@router.post('/{id}/restore')
def restore(id: int, u=Depends(admin_only)):
    return backup_dict(svc.restore_backup(id, u.email))
