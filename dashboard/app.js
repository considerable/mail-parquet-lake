'use strict';
let snapshot;
let visibleRows = 20;
const $ = id => document.getElementById(id);
const operational = ['Gmail Sync', 'Job Tracker'];
const descriptions = {'Gmail Sync':'Gmail → incremental sync → S3 Parquet', 'Job Tracker':'S3 Parquet → DuckDB → job-search report'};
const el = (tag, text, cls) => {const node=document.createElement(tag); if(text!==undefined)node.textContent=text; if(cls)node.className=cls; return node;};
function date(value){return value ? new Date(value).toLocaleString(undefined,{month:'short',day:'numeric',hour:'2-digit',minute:'2-digit'}) : 'Not recorded';}
function ago(value){if(!value)return 'Not observed'; const hours=Math.max(0,(Date.now()-Date.parse(value))/3600000); return hours<1?`${Math.floor(hours*60)}m ago`:hours<48?`${hours.toFixed(1)}h ago`:`${Math.floor(hours/24)}d ago`;}
function state(run){return run.status!=='completed'?'pending':run.conclusion==='success'?'success':['failure','timed_out','action_required','startup_failure'].includes(run.conclusion)?'failure':'neutral';}
function duration(run){const start=Date.parse(run.run_started_at||run.created_at), end=Date.parse(run.updated_at);return run.status==='completed'&&Number.isFinite(start)&&Number.isFinite(end)?Math.max(0,(end-start)/1000):null;}
function formatDuration(seconds){return seconds===null?'—':seconds<60?`${Math.round(seconds)}s`:`${Math.floor(seconds/60)}m ${Math.round(seconds%60)}s`;}
function safeLink(text,url,cls){const a=el('a',text,cls);try{const parsed=new URL(url);if(parsed.origin==='https://github.com'){a.href=parsed.href;a.target='_blank';a.rel='noopener';}}catch{}return a;}
function filtered(){const days=$('window').value;return snapshot.runs.filter(r=>days==='all'||Date.parse(r.created_at)>=Date.now()-Number(days)*86400000);}
function workflowState(name){return (snapshot.workflows||[]).find(w=>w.name===name)?.state||'unknown';}
function stateLabel(value){return ({active:'Enabled',disabled_inactivity:'Disabled · inactivity',disabled_manually:'Disabled · manually',disabled_fork:'Disabled · fork',deleted:'Deleted',unknown:'Unknown'})[value]||value;}
function stateReason(value){return ({active:'Workflow enabled. Execution success and data freshness are separate checks.',disabled_inactivity:'GitHub disabled scheduling after 60 days of repository inactivity. Re-enable deliberately in Actions.',disabled_manually:'Workflow was manually disabled. Confirm intent before re-enabling.',disabled_fork:'Workflow is disabled in a fork. Review and enable explicitly if needed.',deleted:'Workflow is deleted.',unknown:'Enablement could not be verified. Do not treat missing runs as healthy.'})[value]||'GitHub reported this workflow state; inspect the workflow for details.';}
function renderFactors(){
 const target=$('factors');target.replaceChildren();
 for(const name of operational){const status=workflowState(name),row=el('article',undefined,'factor');row.append(el('h3',name),el('span',stateLabel(status),`badge ${status==='active'?'success':status==='unknown'?'neutral':'failure'}`),el('p',stateReason(status)));
 const latest=snapshot.runs.find(r=>r.name===name);row.append(el('small',latest?`Last execution: ${latest.conclusion||latest.status} · ${date(latest.created_at)}. This is distinct from enablement.`:'No execution found in fetched history.'));
 row.append(safeLink('Inspect workflow ↗',`https://github.com/${snapshot.repository}/actions/workflows/${name==='Gmail Sync'?'gmail-sync.yml':'job-tracker.yml'}`,'pipeline-link'));target.append(row);}
 const blocked=operational.filter(name=>workflowState(name).startsWith('disabled')||workflowState(name)==='deleted');
 const stale=Date.now()-Date.parse(snapshot.generated_at)>90*60000;
 $('notice').className=`notice${stale||blocked.length||!snapshot.workflow_states_available?' warning':''}`;
 $('notice').textContent=[stale?'Snapshot is over 90 minutes old; inspect the Dashboard workflow.':'',blocked.length?`${blocked.length} operational workflows are disabled. Past run failures do not explain this scheduling state.`:'',!snapshot.workflow_states_available?'Workflow enablement is unknown.':'','Workflow metadata is separate from the executive checkpoint evidence above.'].filter(Boolean).join(' ');
}
function render(){
 renderFactors();
 const rows=filtered(), ops=rows.filter(r=>operational.includes(r.name)), completed=ops.filter(r=>r.status==='completed');
 $('run-count').textContent=ops.length;
 $('success-rate').textContent=completed.length?`${Math.round(100*completed.filter(r=>r.conclusion==='success').length/completed.length)}%`:'—';
 $('failure-count').textContent=ops.filter(r=>state(r)==='failure').length;
 const durations=completed.map(duration).filter(v=>v!==null).sort((a,b)=>a-b);
 $('median').textContent=durations.length?formatDuration((durations[Math.floor((durations.length-1)/2)]+durations[Math.floor(durations.length/2)])/2):'—';
 $('pipelines').replaceChildren();$('history').replaceChildren();
 for(const name of operational){
  const all=snapshot.runs.filter(r=>r.name===name), latest=all[0], success=all.find(r=>r.conclusion==='success');
  const card=el('article',undefined,'pipeline'),top=el('div',undefined,'pipeline-top');top.append(el('h3',name),el('span',stateLabel(workflowState(name)),`badge ${workflowState(name)==='active'?'success':workflowState(name)==='unknown'?'neutral':'failure'}`));card.append(top,el('p',descriptions[name]));
  const facts=el('div',undefined,'facts');
  for(const [label,value]of [['Last attempt',latest?`${date(latest.created_at)} · ${ago(latest.created_at)}`:'Not observed'],['Last success',success?`${date(success.updated_at)} · ${ago(success.updated_at)}`:'Not in fetched history'],['Last execution outcome',latest?(latest.conclusion||latest.status):'Unknown'],['Latest run',latest?`#${latest.run_number} · attempt ${latest.run_attempt||1}`:'—'],['Data freshness',name==='Gmail Sync'?'See executive overview':'Report freshness not independently verified']]){const fact=el('div');fact.append(el('span',label),el('strong',value));facts.append(fact);}card.append(facts);if(latest)card.append(safeLink('Inspect latest run ↗',latest.html_url,'pipeline-link'));$('pipelines').append(card);
  const row=el('div',undefined,'history-row'),cells=el('div',undefined,'cells');row.append(el('strong',name),cells);
  const history=ops.filter(r=>r.name===name).slice(0,48).reverse();
  for(const run of history){const cell=safeLink('',run.html_url,`cell ${state(run)}`);cell.title=`${name} #${run.run_number}: ${run.conclusion||run.status} · ${date(run.created_at)}`;cell.setAttribute('aria-label',cell.title);cells.append(cell);}if(!history.length)cells.append(el('span','No recorded runs in this window','muted'));$('history').append(row);
 }
 const chartRuns=ops.filter(r=>duration(r)!==null).slice(0,60).reverse(),max=Math.max(1,...chartRuns.map(duration));$('chart').replaceChildren();
 for(const run of chartRuns){const bar=safeLink('',run.html_url,`bar ${state(run)}`);bar.style.height=`${Math.max(3,100*duration(run)/max)}%`;bar.title=`${run.name} #${run.run_number} · ${formatDuration(duration(run))}`;bar.setAttribute('aria-label',bar.title);$('chart').append(bar);}
 $('chart-summary').textContent=chartRuns.length?`${chartRuns.length} runs · scale 0–${formatDuration(max)} · newest at right. Exact job times are available in GitHub.`:'No completed operational runs in this window.';
 let tableRows=rows.filter(r=>($('workflow').value==='all'||r.name===$('workflow').value)&&($('outcome').value==='all'||state(r)===$('outcome').value));
 $('runs').replaceChildren();for(const run of tableRows.slice(0,visibleRows)){
  const tr=el('tr'),name=el('td');name.append(el('strong',run.name),el('small',`#${run.run_number} · ${run.event} · attempt ${run.run_attempt||1}`));
  const outcome=el('td');outcome.append(el('span',run.conclusion||run.status,`badge ${state(run)}`));
  const detail=el('td',undefined,'step-list'),jobs=run.jobs||[],failed=jobs.filter(j=>['failure','timed_out','cancelled'].includes(j.conclusion));
  let details=[];for(const job of failed){const steps=(job.steps||[]).filter(s=>['failure','cancelled'].includes(s.conclusion));details.push(...(steps.length?steps.map(s=>`${job.name} → ${s.name}`):[`${job.name}: ${job.conclusion}`]));}
  detail.textContent=details.join('; ')||(run.jobs?(run.job_details_complete?'No failed step reported':'Job detail incomplete'):'Details not collected for this older run');
  const link=el('td');link.append(safeLink('Open run ↗',run.html_url));tr.append(name,el('td',date(run.run_started_at||run.created_at)),outcome,detail,el('td',formatDuration(duration(run))),link);$('runs').append(tr);
 }
 if(!tableRows.length){const td=el('td','No runs match these filters.');td.colSpan=6;const tr=el('tr');tr.append(td);$('runs').append(tr);}
 $('table-summary').textContent=`Showing ${Math.min(visibleRows,tableRows.length)} of ${tableRows.length} matching runs`;$('more').hidden=tableRows.length<=visibleRows;
}
async function load(){
 $('reload').disabled=true;
 try{const response=await fetch(`data.json?t=${Date.now()}`,{cache:'no-store'});if(!response.ok)throw new Error('Snapshot unavailable');snapshot=await response.json();if(snapshot.schema_version!==1||!Array.isArray(snapshot.runs))throw new Error('Invalid snapshot');
 $('snapshot').textContent=date(snapshot.generated_at);$('snapshot-age').textContent=`${ago(snapshot.generated_at)} · times shown in your local timezone`;
 const stale=Date.now()-Date.parse(snapshot.generated_at)>90*60000;$('notice').className=`notice${stale?' warning':''}`;$('notice').textContent=stale?'Snapshot is over 90 minutes old. This page may not reflect new runs; inspect the Dashboard workflow in GitHub.':'Recorded workflow history. See the executive view for checkpoint freshness and completeness.';
 const previous=$('workflow').value;$('workflow').replaceChildren(el('option','All workflows'));$('workflow').firstChild.value='all';for(const name of [...new Set(snapshot.runs.map(r=>r.name))].sort()){const option=el('option',name);option.value=name;$('workflow').append(option);}if([...$('workflow').options].some(o=>o.value===previous))$('workflow').value=previous;
 $('coverage').textContent=`${snapshot.runs.length} / ${snapshot.total_available_runs} available runs${snapshot.history_truncated?' · bounded history':''} · ${snapshot.warnings.length} detail warnings`;render();
 }catch(error){$('notice').className='notice warning';$('notice').textContent='Cannot load the snapshot. The dashboard is not reporting healthy status. Open GitHub Actions to inspect runs.';$('snapshot').textContent='Unavailable';}
 finally{$('reload').disabled=false;}
}
for(const id of ['window','workflow','outcome'])$(id).addEventListener('change',()=>{visibleRows=20;render();});$('more').addEventListener('click',()=>{visibleRows+=30;render();});$('reload').addEventListener('click',load);load();

// Refresh the published snapshot and stale warning while the page remains open.
setInterval(load, 60000);
