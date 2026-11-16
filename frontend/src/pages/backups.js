import {backups} from '../api/entities.js';
import {session} from '../stores/session.js';
import {heading} from '../components/common/shell.js';
import {BackupStatus} from '../constants/enums.js';
import {esc,date,notify} from '../utils/dom.js';
const chip=s=>'<span class="status bk-'+esc(s)+'">'+esc(BackupStatus[s]||s)+'</span>';
const size=n=>n>=1048576?(n/1048576).toFixed(1)+' MB':n>=1024?(n/1024).toFixed(1)+' KB':(n||0)+' B';
const counts=c=>'样本 '+(c.datasets||0)+' · 试验 '+(c.jobs||0)+' · 计划 '+(c.reports||0)+' · 归档 '+(c.deliveries||0);
export async function backupPage(){
 const list=await backups.list(),admin=session.user.role==='admin',busy=list.some(b=>b.status==='verifying'||b.status==='restoring');
 return heading('06 / SITE SNAPSHOT','站点备份与恢复','误操作后整站回退：同一时点的样本、试验、计划与两份归档文件打成一个备份包。恢复前逐文件核验，恢复期间暂停写入与下载，全部就绪后一次性切换。',(admin?'<button class="button primary" id="create-backup" '+(busy?'disabled':'')+'>立即备份当前站点 ＋</button>':''))
  +'<section class="panel"><div class="section-title"><h2>备份包</h2><span>'+list.length+' 个时点'+(busy?' · 正在校验或恢复，页面自动刷新':'')+'</span></div><div class="schedule-list" data-busy="'+(busy?1:0)+'">'
  +list.map(b=>'<article><div><strong>'+date(b.created_at)+'</strong> '+chip(b.status)+'<p>'+counts(b.counts||{})+' · '+size(b.size_bytes)+' · SHA256 '+esc((b.checksum||'—').slice(0,24))+'…</p>'+(b.error?'<p class="warning">'+esc(b.error)+'</p>':'')+'</div>'
  +(admin?'<button class="button small" data-restore="'+b.id+'" data-at="'+esc(date(b.created_at))+'" '+(busy||!b.checksum?'disabled':'')+'>'+((b.status==='verifying'||b.status==='restoring')?'继续恢复':'恢复到此时点')+'</button>':'')
  +'</article>').join('')
  +(!list.length?'<p class="empty">尚无备份。负责人点击右上角「立即备份当前站点」形成第一个可回退时点。</p>':'')
  +'</div></section><section class="panel"><div class="section-title"><h2>恢复口径</h2></div><ol class="questions"><li>恢复前逐文件核验 SHA256，坏包直接拒绝，原站数据不变。</li><li>恢复期间暂停数据接入、任务提交与归档下载；旧执行者与旧队列的迟到结果不会混入恢复后的站点。</li><li>数据与归档全部就绪后一次性切换；恢复中断可重试继续，不会重复归档；切换失败保留旧版。</li><li>恢复出的未完成试验会重新排队；分发计划保持停用，需负责人手动启用，避免补发旧简报。</li></ol></section>';
}
export function bindBackups(refresh){
 document.querySelector('#create-backup')?.addEventListener('click',async()=>{try{await backups.create();notify('备份已生成，可作为回退时点');}catch(e){notify(e.message,true);}await refresh();});
 document.querySelectorAll('[data-restore]').forEach(b=>b.onclick=async()=>{
  if(!confirm('确认将整站恢复到 '+b.dataset.at+' 的时点？\n恢复期间暂停写入与下载；未完成试验将重新排队，分发计划保持停用。'))return;
  try{const r=await backups.restore(b.dataset.restore);notify(r.status==='restored'?'站点已恢复到备份时点':'恢复未完成：'+(r.error||'未知原因'),r.status!=='restored');}catch(e){notify(e.message,true);}
  await refresh();
 });
 if(document.querySelector('[data-busy="1"]'))setTimeout(refresh,2500);
}
