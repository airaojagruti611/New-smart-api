'use strict';
const $ = id => document.getElementById(id);
const number = value => new Intl.NumberFormat('en-IN').format(value || 0);
const time = value => value ? new Intl.DateTimeFormat('en-IN',{timeZone:'Asia/Kolkata',hour:'2-digit',minute:'2-digit',hour12:true}).format(new Date(value)) : 'â€”';
const fullTime = value => value ? new Intl.DateTimeFormat('en-IN',{timeZone:'Asia/Kolkata',day:'numeric',month:'short',hour:'2-digit',minute:'2-digit',second:'2-digit',hour12:true}).format(new Date(value)) + ' IST' : 'Not recorded';
const compact = value => new Intl.NumberFormat('en-IN',{notation:'compact',maximumFractionDigits:2}).format(value || 0);
const bytes = value => { if(!value) return 'â€”'; const i=Math.min(3,Math.floor(Math.log(value)/Math.log(1024))); return `${(value/1024**i).toFixed(i>1?1:0)} ${['B','KB','MB','GB'][i]}`; };
const text = (id,value) => { $(id).textContent=value; };
const el = (tag,className,content) => {const node=document.createElement(tag); if(className) node.className=className; if(content!==undefined) node.textContent=content; return node;};
let snapshot = null, busy = false, refreshPending = false, selected = new URLSearchParams(location.search).get('date');

function badge(id,label,kind){text(id,label);$(id).className=`badge ${kind}`;}
function timelineItem(title,detail,clock,done){
 const row=el('li'); row.append(el('span',`timeline-mark ${done?'done':''}`,done?'âœ“':'Â·'));
 const content=el('div');content.append(el('div','timeline-title',title),el('div','timeline-note',detail));
 row.append(content,el('time','timeline-time',clock || 'â€”')); return row;
}
function renderStreams(){
 const body=$('stream-rows');body.replaceChildren();
 const query=$('stream-search').value.toLowerCase();
 const rows=(snapshot?.streams || []).filter(row=>row.name.toLowerCase().includes(query));
 if(!rows.length){const tr=el('tr'),td=el('td','empty',query?'No streams match this filter.':'No archived stream records are available for this date.');td.colSpan=5;tr.append(td);body.append(tr);return;}
 for(const row of rows){
  const tr=el('tr'),name=el('td','stream-name');name.append(el('span','stream-indicator'),document.createTextNode(row.name.replace(/^md:/,'')));
  const verified=el('td'),group=el('div','stream-progress'),track=el('span','mini-track'),fill=el('span');
  fill.style.width=`${row.files?100*row.verified/row.files:0}%`;track.append(fill);group.append(track,el('span','',`${number(row.verified)} / ${number(row.files)}`));verified.append(group);
  tr.append(name,el('td','number',row.rows===null?'Not recorded':number(row.rows)),el('td','number',number(row.files)),verified,el('td','number',bytes(row.bytes)));body.append(tr);
 }
}
function render(data){
 snapshot=data;
 if(!$('session-date').value)$('session-date').value=data.date;
 $('session-date').max=data.today;
 const u=data.uploads,a=data.archive,s=data.session;
 text('headline',data.headline);text('headline-detail',data.detail);
 const states={running:['Collecting','active'],stopped:['Stopped safely','good'],attention:['Needs attention','bad'],recorded:['Recorded','neutral'],scheduled:['No session data','neutral']};
 badge('session-badge',...(states[data.collection_state] || states.recorded));
 text('banner-symbol',data.collection_state==='running'?'â†—':data.collection_state==='attention'?'!':'âœ“');
 text('tick-count',a.equity_ticks===null || a.option_ticks===null?'Not recorded':compact(a.equity_ticks+a.option_ticks));text('tick-detail',`${a.equity_ticks===null?'Unknown':compact(a.equity_ticks)} equity Â· ${a.option_ticks===null?'Unknown':compact(a.option_ticks)} options`);
 text('file-count',number(a.files));text('file-detail',`${bytes(a.bytes)} Â· ${a.stream_count} streams with records`);
 text('verified-count',`${u.percent.toFixed(1)}%`);text('verified-detail',`${number(u.verified)} of ${number(u.total)} files`);
 text('worker-label','Workers right now');text('worker-count',number(data.workers.length));text('worker-detail',data.workers.length?'Active tracked processes':'Collection is currently stopped');
 const alerts=$('alerts');alerts.replaceChildren();for(const message of data.alerts){const node=el('div','alert',message);node.setAttribute('role','status');alerts.append(node);}
 const timeline=$('timeline');timeline.replaceChildren();
 if(!s.trading_day){timeline.append(timelineItem('Non-trading session','The configured calendar does not schedule collection.','â€”',false));}
 else{
  timeline.append(timelineItem('Prepare the pipeline','Docker, Redis and history checks',s.prepare,!!s.started_at || !!s.drain));
  timeline.append(timelineItem('Market collection',s.started_at?`Launched ${time(s.started_at)} Â· scheduled ${s.open}`:`Scheduled ${s.open} IST`,s.started_at?time(s.started_at):s.open,!!s.started_at));
  timeline.append(timelineItem('Stop & drain archives',s.drain?`${s.drain.streams} streams flushed successfully`:`Collection cutoff ${s.close} IST`,s.drain?time(s.drain.at):s.close,!!s.drain));
  timeline.append(timelineItem('Verify on Google Drive',!u.total?'Waiting for archive files':u.complete?'All tracked archive files verified':`${number(u.pending)} files still awaiting verification`,u.complete?'Complete':u.working_on_session?'In progress':'Pending',u.complete));
 }
 text('timeline-day',new Intl.DateTimeFormat('en-IN',{day:'numeric',month:'short',timeZone:'Asia/Kolkata'}).format(new Date(data.date+'T12:00:00+05:30'))+' Â· IST');
 text('next-session',data.next_open?`Next collection Â· ${fullTime(data.next_open)}`:'Next session needs calendar configuration');
 let uploadLabel=u.paused?'Paused':u.complete?'Verified':u.working_on_session?'Uploading':u.running?'Queued':'Waiting';
 badge('upload-state',uploadLabel,u.paused?'warn':u.complete?'good':u.working_on_session?'active':'neutral');
 $('upload-ring').style.setProperty('--progress',`${u.percent}%`);$('upload-ring').setAttribute('aria-label',`${u.percent}% of tracked files verified`);
 text('upload-percent',u.complete?'100%':`${u.percent.toFixed(1)}%`);text('upload-title',!u.total?'No archive files yet':u.complete?'Available files verified':`${number(u.pending)} files to verify`);
 text('upload-description',u.paused?'Waiting for Google account verification.':u.complete?(u.running?'These archives are verified. The worker continues other pending archives.':'Every tracked archive file has a saved checksum-verification checkpoint.'):u.working_on_session?'This session is included in the batch being copied and checked.':u.running?'This session is queued while the worker handles other archives.':'The upload schedule will resume pending work when this laptop is online.');
 $('upload-progress').setAttribute('aria-valuenow',String(u.percent));$('upload-progress').firstElementChild.style.width=`${u.percent}%`;
 text('upload-verified',`${number(u.verified)} verified`);text('upload-pending',`${number(u.pending)} pending`);
 text('drive-account',u.account || 'No Drive account configured');text('drive-folder',`My Drive / ${u.folder.replaceAll('/',' / ')}`);
 text('last-verified',u.last_verified_at?`Latest Drive activity Â· ${fullTime(u.last_verified_at)}`:'No verified upload recorded yet');
 text('batch-progress',u.working_on_session && u.batch.total!==null?`Current batch ${number(u.batch.copied)} / ${number(u.batch.total)} copied`:u.running?'Upload service is active':'');
 text('stream-count',number(data.streams.length));text('archive-summary',a.rows===null?'Record counts are not available for this date':`${number(a.rows)} reported archived records, including derived signals`);renderStreams();
 const activity=$('activity-list');activity.replaceChildren();
 for(const event of data.events){const row=el('li'),content=el('div');content.append(el('div','event-title',event.title),el('div','event-detail',event.detail));row.append(el('span','event-icon','âœ“'),content,el('time','event-time',time(event.at)));activity.append(row);}
 if(!data.events.length)activity.append(el('li','empty','No recorded milestones are available yet.'));
 text('collection-task',data.tasks['SmartAPI-MarketHours']?.state || 'Not available');text('upload-task',data.tasks['SmartAPI-DriveUploads']?.state || 'Not available');text('disk-free',bytes(data.disk_free_bytes));
 text('updated-at',`Updated ${fullTime(data.observed_at)}`);text('connection','Auto-refresh Â· 15s');$('connection').prepend(el('i'));$('connection').classList.add('live');
}
async function refresh(){
 if(busy){refreshPending=true;return;}busy=true;$('refresh').disabled=true;const requested=selected;
 try{
  const response=await fetch('/api/status'+(selected?'?date='+encodeURIComponent(selected):''),{cache:'no-store',signal:AbortSignal.timeout(20000)});
  if(!response.ok)throw new Error('Status unavailable');
  const data=await response.json();if(requested!==selected){refreshPending=true;return;}render(data);$('error-banner').hidden=true;
 }catch(error){
  if(requested!==selected){refreshPending=true;return;}
  text('error-banner',snapshot?'The dashboard could not refresh. The values below are from the last successful snapshot. Retrying automatically.':'Status is unavailable right now. Retrying automatically; collection and uploads are unaffected.');$('error-banner').hidden=false;
  if(snapshot){text('headline','Last known session snapshot');text('headline-detail',`Session ${snapshot.date} Â· recorded ${fullTime(snapshot.observed_at)}. Current activity is not confirmed.`);badge('session-badge','Snapshot stale','warn');badge('upload-state','Snapshot stale','warn');text('worker-label','Workers at last snapshot');text('worker-detail','Current process state is not confirmed');text('batch-progress','Current transfer state is not confirmed');const last=$('timeline').lastElementChild;if(last){last.querySelector('.timeline-time').textContent='Last known';}}
  else{text('headline','Session data unavailable');text('headline-detail','No records from another date are shown. Retrying this session automatically.');badge('session-badge','Unavailable','warn');badge('upload-state','Unavailable','warn');}
  text('connection','Snapshot unavailable');$('connection').prepend(el('i'));$('connection').classList.remove('live');
 }finally{busy=false;$('refresh').disabled=false;if(refreshPending){refreshPending=false;refresh();}}
}
$('refresh').addEventListener('click',refresh);$('stream-search').addEventListener('input',renderStreams);
$('session-date').addEventListener('change',()=>{selected=$('session-date').value;snapshot=null;text('headline','Loading selected sessionâ€¦');text('headline-detail','Reading records for '+(selected || 'today')+'.');badge('session-badge','Loading','neutral');badge('upload-state','Loading','neutral');for(const id of ['tick-count','file-count','verified-count','worker-count','upload-percent'])text(id,'â€”');for(const id of ['tick-detail','file-detail','verified-detail','worker-detail','upload-verified','upload-pending','archive-summary'])text(id,'Loading selected session');text('upload-title','Loading selected sessionâ€¦');text('upload-description','Previous session values are hidden until this session is loaded.');$('upload-ring').style.setProperty('--progress','0%');$('upload-progress').firstElementChild.style.width='0';$('upload-progress').removeAttribute('aria-valuenow');$('upload-ring').setAttribute('aria-label','Verification progress unavailable while session loads');text('batch-progress','');text('timeline-day','Loading selected date');text('stream-count','?');for(const id of ['timeline','stream-rows','activity-list'])$(id).replaceChildren();$('error-banner').hidden=true;const url=new URL(location);if(selected)url.searchParams.set('date',selected);else url.searchParams.delete('date');history.replaceState(null,'',url);refresh();});
refresh();setInterval(()=>{if(!document.hidden)refresh();},15000);document.addEventListener('visibilitychange',()=>{if(!document.hidden)refresh();});
