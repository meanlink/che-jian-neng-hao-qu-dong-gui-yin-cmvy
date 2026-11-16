from fastapi import Depends, HTTPException
from src.config.database import get_db
from src.services.state import restore_in_progress
PHASE = {'verifying': '校验备份包', 'restoring': '恢复数据'}
def site_writable(db=Depends(get_db)):
    busy = restore_in_progress(db)
    if busy:
        raise HTTPException(423, f'站点正在{PHASE.get(busy.status, "恢复")}（备份 #{busy.id}），暂停写入与下载')
    return True
