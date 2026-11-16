import time, json, logging
from pathlib import Path
from sqlalchemy import select, update
from redis import Redis
from src.config.database import Session
from src.config.settings import REDIS_URL
from src.models import Job, Dataset, Report, SiteState
from src.services.business import analyze
from src.services.reports import deliver
from src.services.audit import record
from src.services.state import current_epoch, restore_in_progress
logging.basicConfig(level=logging.INFO)
def execute(job_id):
    with Session() as db:
        if restore_in_progress(db):return
        j=db.get(Job,job_id)
        if not j or j.status!='queued':return
        epoch=current_epoch(db)
        if j.epoch!=epoch:return # 旧队列的迟到条目，丢弃
        claimed=db.execute(update(Job).where(Job.id==job_id,Job.status=='queued',Job.epoch==epoch).values(status='running')).rowcount
        db.commit()
        if not claimed:return
        try:
            d=db.get(Dataset,j.dataset_id)
            result=analyze(d.records,j.params)
            json.dumps(result,allow_nan=False)
            done=db.execute(update(Job).where(Job.id==job_id,Job.status=='running',Job.epoch==epoch).values(result=result,status='completed',finished_at=time.time())).rowcount
            if done:record(db,'worker','job.completed',{'id':job_id})
            else:record(db,'worker','job.discarded',{'id':job_id,'reason':'站点已恢复，旧执行结果作废'})
        except Exception as e:
            logging.exception('Analysis failed')
            done=db.execute(update(Job).where(Job.id==job_id,Job.status=='running',Job.epoch==epoch).values(status='failed',error=str(e)[:990],finished_at=time.time())).rowcount
            if done:record(db,'worker','job.failed',{'id':job_id,'error':str(e)[:200]})
        db.commit()
def deliver_if_due(report_id,epoch):
    with Session() as db:
        if restore_in_progress(db):return
        # 与恢复切换互斥：恢复会先锁定并推进 epoch，锁释放后此处看到的即新世代
        db.execute(select(SiteState).where(SiteState.name=='epoch').with_for_update())
        if current_epoch(db)!=epoch:return
        r=db.get(Report,report_id)
        if not r or not r.enabled or r.epoch!=epoch or r.next_run>time.time():return
        d=deliver(db,r)
        if d:record(db,'worker','report.delivered',{'id':r.id,'delivery':d.id})
        db.commit()
def tick():
    with Session() as db:
        if restore_in_progress(db):return
        epoch=current_epoch(db)
        queued=list(db.scalars(select(Job.id).where(Job.status=='queued',Job.epoch==epoch).limit(3)))
        due=list(db.scalars(select(Report.id).where(Report.enabled==True,Report.next_run<=time.time(),Report.epoch==epoch)))
    for job_id in queued:execute(job_id)
    for report_id in due:deliver_if_due(report_id,epoch)
def main():
    r=Redis.from_url(REDIS_URL,socket_timeout=3)
    # This Compose stack has a single worker. A crash must not strand its jobs.
    with Session() as db:
        if not restore_in_progress(db):
            epoch=current_epoch(db)
            for j in db.scalars(select(Job).where(Job.status=='running',Job.epoch==epoch)):
                j.status='queued';record(db,'worker','job.recovered',{'id':j.id})
            db.commit()
    while True:
        Path('/tmp/heartbeat').touch()
        try:
            item=r.blpop('analysis:queue',timeout=1)
            if item:execute(int(item[1]))
            tick()
        except Exception:
            logging.exception('Worker retry');time.sleep(2)
if __name__=='__main__':main()
