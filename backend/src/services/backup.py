"""站点级备份与恢复：同一时点快照样本、试验、计划与归档文件，原子切换恢复。"""
import hashlib, json, os, re, shutil, threading, time, zipfile
from fastapi import HTTPException
from sqlalchemy import delete, select
from redis import Redis
from src.config.database import Session
from src.config.settings import ARTIFACT_DIR, REDIS_URL
from src.models import Backup, Dataset, Delivery, Job, Report, SiteState
from src.services.audit import record
from src.services.state import bump_epoch, restore_in_progress

BACKUP_DIR = os.path.join(ARTIFACT_DIR, 'backups')
TABLES = ('datasets', 'jobs', 'reports', 'deliveries')
SAFE_NAME = re.compile(r'^[\w.\-一-鿿]+$')
_restore_lock = threading.Lock()

class BackupError(Exception):
    pass

def _sha256_file(path):
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(1 << 20), b''): h.update(chunk)
    return h.hexdigest()

def _archive_path(backup_id, filename=None):
    return os.path.join(BACKUP_DIR, os.path.basename(filename) if filename else f'site-backup-{backup_id}.zip')

def _snapshot(db):
    """单事务读取四张表，得到同一时点的快照（REPEATABLE READ 不阻塞并发写入与分发）。"""
    datasets = [{'id': d.id, 'name': d.name, 'source': d.source, 'records': d.records,
                 'checksum': d.checksum, 'created_at': d.created_at} for d in db.scalars(select(Dataset))]
    jobs = [{'id': j.id, 'name': j.name, 'dataset_id': j.dataset_id, 'params': j.params, 'status': j.status,
             'result': j.result, 'error': j.error, 'approved': j.approved, 'created_by': j.created_by,
             'created_at': j.created_at, 'finished_at': j.finished_at} for j in db.scalars(select(Job))]
    reports = [{'id': r.id, 'name': r.name, 'job_id': r.job_id, 'destination': r.destination,
                'interval_seconds': r.interval_seconds, 'next_run': r.next_run, 'enabled': r.enabled}
               for r in db.scalars(select(Report))]
    deliveries = [{'id': d.id, 'report_id': d.report_id, 'created_at': d.created_at, 'checksum': d.checksum,
                   'filename': d.filename, 'destination': d.destination} for d in db.scalars(select(Delivery))]
    return {'datasets': datasets, 'jobs': jobs, 'reports': reports, 'deliveries': deliveries}

def _build_archive(backup_id, snap, taken_at):
    """把快照与两份归档文件打成 zip，manifest 记录逐文件 SHA256。返回 (counts, checksum, size)。"""
    tmp = os.path.join(BACKUP_DIR, f'tmp-{backup_id}-{int(time.time() * 1000)}')
    os.makedirs(os.path.join(tmp, 'payload'), exist_ok=True)
    os.makedirs(os.path.join(tmp, 'artifacts'), exist_ok=True)
    try:
        counts = {name: len(snap[name]) for name in TABLES}
        files = {}
        for name in TABLES:
            data = json.dumps(snap[name], ensure_ascii=False, sort_keys=True, allow_nan=False).encode()
            files[f'payload/{name}.json'] = hashlib.sha256(data).hexdigest()
            with open(os.path.join(tmp, 'payload', f'{name}.json'), 'wb') as f: f.write(data)
        artifacts = {}
        for d in snap['deliveries']:
            for fn in (d['filename'], d['filename'] + '.json'):
                src = os.path.join(ARTIFACT_DIR, fn)
                if not os.path.isfile(src): raise BackupError(f'归档文件 {fn} 缺失，无法形成一致备份')
                with open(src, 'rb') as f: data = f.read()
                artifacts[fn] = hashlib.sha256(data).hexdigest()
                with open(os.path.join(tmp, 'artifacts', fn), 'wb') as f: f.write(data)
        manifest = {'version': 1, 'created_at': taken_at, 'counts': counts, 'files': files, 'artifacts': artifacts}
        with open(os.path.join(tmp, 'manifest.json'), 'w') as f:
            json.dump(manifest, f, ensure_ascii=False, sort_keys=True)
        final = _archive_path(backup_id)
        tmpzip = final + '.tmp'
        with zipfile.ZipFile(tmpzip, 'w', zipfile.ZIP_DEFLATED) as z:
            z.write(os.path.join(tmp, 'manifest.json'), 'manifest.json')
            for member in sorted(files): z.write(os.path.join(tmp, member), member)
            for fn in sorted(artifacts): z.write(os.path.join(tmp, 'artifacts', fn), 'artifacts/' + fn)
        os.replace(tmpzip, final)
        return counts, _sha256_file(final), os.path.getsize(final)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)

def create_backup(actor):
    os.makedirs(BACKUP_DIR, exist_ok=True)
    with Session() as db:
        busy = restore_in_progress(db)
        if busy: raise HTTPException(409, f'备份 #{busy.id} 正在恢复站点，暂不能创建备份')
        taken_at = time.time()
        snap = _snapshot(db)
        b = Backup(created_at=taken_at, status='ready')
        db.add(b)
        db.flush()
        backup_id = b.id
        db.commit()
    try:
        counts, checksum, size = _build_archive(backup_id, snap, taken_at)
    except Exception as e:
        with Session() as db:
            b = db.get(Backup, backup_id)
            if b: b.status, b.error = 'failed', ('备份失败：' + str(e))[:990]
            record(db, actor, 'backup.failed', {'id': backup_id, 'error': str(e)[:200]})
            db.commit()
        raise HTTPException(500, '备份失败：' + str(e))
    with Session() as db:
        b = db.get(Backup, backup_id)
        b.filename, b.checksum, b.size_bytes, b.counts, b.status = os.path.basename(_archive_path(backup_id)), checksum, size, counts, 'ready'
        record(db, actor, 'backup.create', {'id': backup_id, 'counts': counts})
        db.commit()
        return b

def _verify(backup_id, filename, checksum):
    """恢复前完整性核验：任何一步失败都抛 BackupError，原站数据不被触碰。"""
    path = _archive_path(backup_id, filename)
    if not os.path.isfile(path): raise BackupError('备份包文件缺失，无法恢复')
    if not checksum or _sha256_file(path) != checksum: raise BackupError('备份包校验和不匹配，文件已损坏')
    try:
        zf = zipfile.ZipFile(path)
    except Exception:
        raise BackupError('备份包无法打开，文件已损坏') from None
    with zf:
        bad = zf.testzip()
        if bad: raise BackupError(f'备份包成员 {bad} 已损坏')
        names = set(zf.namelist())
        if 'manifest.json' not in names: raise BackupError('备份包缺少 manifest，不是有效的站点备份')
        try:
            manifest = json.loads(zf.read('manifest.json'))
        except Exception:
            raise BackupError('备份包 manifest 已损坏') from None
        if manifest.get('version') != 1: raise BackupError('备份包版本不受支持')
        for member, digest in (manifest.get('files') or {}).items():
            if member not in names: raise BackupError(f'备份包缺少 {member}')
            if hashlib.sha256(zf.read(member)).hexdigest() != digest: raise BackupError(f'备份包 {member} 校验失败')
        try:
            payloads = {name: json.loads(zf.read(f'payload/{name}.json')) for name in TABLES}
        except Exception:
            raise BackupError('备份包数据载荷已损坏') from None
        artifacts = manifest.get('artifacts') or {}
        for fn, digest in artifacts.items():
            if not SAFE_NAME.match(fn): raise BackupError(f'归档文件名 {fn} 非法')
            if 'artifacts/' + fn not in names: raise BackupError(f'备份包缺少归档文件 {fn}')
            if hashlib.sha256(zf.read('artifacts/' + fn)).hexdigest() != digest: raise BackupError(f'归档文件 {fn} 校验失败')
        ds_ids = {d['id'] for d in payloads['datasets']}
        job_ids = {j['id'] for j in payloads['jobs']}
        report_ids = {r['id'] for r in payloads['reports']}
        for j in payloads['jobs']:
            if j['dataset_id'] not in ds_ids: raise BackupError(f"试验 {j['id']} 引用了快照外的数据批次")
        for r in payloads['reports']:
            if r['job_id'] not in job_ids: raise BackupError(f"计划 {r['id']} 引用了快照外的试验")
        for d in payloads['deliveries']:
            if d['report_id'] not in report_ids: raise BackupError(f"归档 {d['id']} 引用了快照外的计划")
            if d['filename'] not in artifacts or d['filename'] + '.json' not in artifacts:
                raise BackupError(f"归档 {d['id']} 缺少配套文件")
            if artifacts[d['filename']] != d['checksum']: raise BackupError(f"归档 {d['id']} 与记录指纹不一致")
        return manifest, payloads

def _staging_dir(backup_id):
    return os.path.join(BACKUP_DIR, f'restore-{backup_id}')

def _load_state(staging):
    try:
        with open(os.path.join(staging, 'state.json')) as f: return set(json.load(f).get('done', []))
    except Exception:
        return set()

def _save_state(staging, done):
    with open(os.path.join(staging, 'state.json'), 'w') as f: json.dump({'done': sorted(done)}, f)

def _stage_files(backup_id, filename, artifacts):
    """逐个解出归档文件并落位，state.json 记录进度；中断后重试跳过已完成文件，不重复归档。"""
    staging = _staging_dir(backup_id)
    os.makedirs(staging, exist_ok=True)
    done = _load_state(staging)
    with zipfile.ZipFile(_archive_path(backup_id, filename)) as zf:
        for fn, digest in artifacts.items():
            target = os.path.join(ARTIFACT_DIR, fn)
            if fn in done and os.path.isfile(target) and _sha256_file(target) == digest: continue
            data = zf.read('artifacts/' + fn)
            if hashlib.sha256(data).hexdigest() != digest: raise BackupError(f'归档文件 {fn} 校验失败')
            tmp = os.path.join(staging, fn + '.tmp')
            with open(tmp, 'wb') as f: f.write(data)
            os.replace(tmp, target)
            done.add(fn)
            _save_state(staging, done)
    return staging

def _apply(payloads):
    """单事务原子切换：先锁定并推进站点 epoch，再整体替换四张表。失败回滚则保留旧版。"""
    with Session() as db:
        epoch = bump_epoch(db)
        for model in (Delivery, Report, Job, Dataset): db.execute(delete(model))
        for row in payloads['datasets']: db.add(Dataset(**row))
        for row in payloads['jobs']:
            if row['status'] == 'running': row['status'] = 'queued'
            db.add(Job(**row, epoch=epoch))
        for row in payloads['reports']:
            db.add(Report(**{**row, 'enabled': False}, epoch=epoch))
        for row in payloads['deliveries']: db.add(Delivery(**row))
        db.commit()
    return epoch

def _finalize(backup_id, payloads, staging, actor):
    """切换成功后：清掉旧队列，重新排队未完成试验，清理快照外归档与暂存区。"""
    queued_ids = [j['id'] for j in payloads['jobs'] if j['status'] == 'queued']
    try:
        r = Redis.from_url(REDIS_URL, socket_timeout=3)
        r.delete('analysis:queue')
        for job_id in queued_ids: r.rpush('analysis:queue', job_id)
        r.close()
    except Exception:
        pass  # SQL 排队状态是持久兜底，worker 会按状态捞起
    keep = set()
    for d in payloads['deliveries']: keep.update((d['filename'], d['filename'] + '.json'))
    try:
        for fn in os.listdir(ARTIFACT_DIR):
            p = os.path.join(ARTIFACT_DIR, fn)
            if os.path.isfile(p) and fn.startswith('report-') and 'delivery-' in fn and fn not in keep:
                os.remove(p)
    except Exception:
        pass
    shutil.rmtree(staging, ignore_errors=True)
    with Session() as db:
        record(db, actor, 'backup.restored', {'id': backup_id, 'requeued': len(queued_ids)})
        db.commit()

def _mark_failed(backup_id, actor, error):
    with Session() as db:
        b = db.get(Backup, backup_id)
        if b: b.status, b.error = 'failed', error[:990]
        record(db, actor, 'backup.restore_failed', {'id': backup_id, 'error': error[:300]})
        db.commit()

def restore_backup(backup_id, actor):
    if not _restore_lock.acquire(blocking=False):
        raise HTTPException(409, '已有恢复任务进行中，请稍候')
    try:
        return _restore(backup_id, actor)
    finally:
        _restore_lock.release()

def _restore(backup_id, actor):
    with Session() as db:
        # 行级锁串行化恢复入口（MySQL FOR UPDATE；SQLite 退化为进程内锁兜底）
        db.execute(select(SiteState).where(SiteState.name == 'restore_lock').with_for_update())
        b = db.get(Backup, backup_id)
        if not b: raise HTTPException(404, '备份不存在')
        busy = restore_in_progress(db)
        if busy and busy.id != backup_id:
            raise HTTPException(409, f'备份 #{busy.id} 正在恢复站点，请稍候')
        b.status, b.error = 'verifying', ''
        record(db, actor, 'backup.restore.start', {'id': backup_id})
        db.commit()
        filename, checksum = b.filename, b.checksum
    try:
        manifest, payloads = _verify(backup_id, filename, checksum)
    except BackupError as e:
        _mark_failed(backup_id, actor, str(e))
        raise HTTPException(422, str(e))
    with Session() as db:
        b = db.get(Backup, backup_id)
        b.status = 'restoring'
        db.commit()
    try:
        staging = _stage_files(backup_id, filename, manifest['artifacts'])
        _apply(payloads)
        _finalize(backup_id, payloads, staging, actor)
    except Exception as e:
        _mark_failed(backup_id, actor, '恢复中断：' + str(e))
        raise HTTPException(500, '恢复中断，原站保持不变或可在重试中继续：' + str(e))
    with Session() as db:
        b = db.get(Backup, backup_id)
        b.status = 'restored'
        db.commit()
        return b
