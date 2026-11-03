'use strict';
(() => {
  const byId = id => document.getElementById(id);
  let evidence = null;
  const finite = v => typeof v === 'number' && Number.isFinite(v) && v >= 0;
  const count = v => finite(v) ? v.toLocaleString(undefined, {maximumFractionDigits:0}) : 'Unknown';
  const time = v => v && Number.isFinite(Date.parse(v)) ? new Date(v).toLocaleString() : 'Unknown';
  const age = v => {const ms=Date.parse(v);if(!Number.isFinite(ms))return 'Unknown';const h=Math.max(0,(Date.now()-ms)/3600000);return h<1?`${Math.round(h*60)}m ago`:h<48?`${h.toFixed(1)}h ago`:`${(h/24).toFixed(1)}d ago`;};
  function node(tag,text){const n=document.createElement(tag);if(text!==undefined)n.textContent=text;return n;}
  function chart(id, rows, key){
    const target=byId(id);target.replaceChildren();
    const points=(Array.isArray(rows)?rows:[]).filter(r=>finite(r[key])&&Number.isFinite(Date.parse(r.date))).sort((a,b)=>a.date.localeCompare(b.date));
    if(!points.length){target.append(node('p','No dated inventory history available yet.'));return;}
    const ns='http://www.w3.org/2000/svg',svg=document.createElementNS(ns,'svg');svg.setAttribute('viewBox','0 0 520 160');svg.setAttribute('role','img');svg.setAttribute('aria-label',`${points.length} dated inventory observations`);
    const title=document.createElementNS(ns,'title');title.textContent=`${points.length} snapshots. Latest ${count(points.at(-1)[key])}.`;svg.append(title);
    const low=Math.min(...points.map(p=>p[key])),high=Math.max(...points.map(p=>p[key])),range=Math.max(1,high-low),first=Date.parse(points[0].date),span=Math.max(86400000,Date.parse(points.at(-1).date)-first);
    const coords=points.map(p=>[50+430*(Date.parse(p.date)-first)/span,125-95*(p[key]-low)/range]);
    const baseline=document.createElementNS(ns,'line');for(const[k,v]of Object.entries({x1:50,y1:125,x2:490,y2:125,stroke:'#d8dfd6'}))baseline.setAttribute(k,v);svg.append(baseline);
    if(coords.length>1){const line=document.createElementNS(ns,'polyline');line.setAttribute('points',coords.map(p=>p.join(',')).join(' '));line.setAttribute('fill','none');line.setAttribute('stroke','#1f6a49');line.setAttribute('stroke-width','2.5');svg.append(line);}
    for(const[x,y]of coords){const dot=document.createElementNS(ns,'circle');dot.setAttribute('cx',x);dot.setAttribute('cy',y);dot.setAttribute('r','3.5');dot.setAttribute('fill','#1f6a49');svg.append(dot);}
    for(const[text,x,y]of [[count(high),0,25],[count(low),0,130],[points[0].date,50,153],[points.at(-1).date,400,153]]){const label=document.createElementNS(ns,'text');label.textContent=text;label.setAttribute('x',x);label.setAttribute('y',y);label.setAttribute('font-size','11');label.setAttribute('fill','#68736c');svg.append(label);}
    target.append(svg,node('p',points.length===1?'One day recorded; more snapshots are needed for a trend.':`${points.length} observed days. Lines connect recorded snapshots; missing dates are not filled with zero.`));
  }
  function render(){
    const good=evidence&&evidence.available!==false&&evidence.schema_version===1&&Number.isFinite(Date.parse(evidence.observed_at));
    const future=good&&Date.parse(evidence.observed_at)>Date.now()+300000;
    const stale=good&&!future&&Date.now()-Date.parse(evidence.observed_at)>45*60000;
    const valid=good&&!future;
    const health=valid?evidence.health:null;
    const statuses={HEALTHY:['healthy','Healthy'],ATTENTION:['attention','Needs attention'],CRITICAL:['critical','Action needed'],UNKNOWN:['unknown','Unknown']};
    const [cls,label]=stale?['stale','Stale data']:statuses[health?.status]||['unknown','Unavailable'];
    byId('executive-health').className=`executive-health ${cls}`;byId('executive-status').textContent=label;
    byId('executive-time').textContent=valid?`Observed ${time(evidence.observed_at)} · ${age(evidence.observed_at)}`:'Executive snapshot unavailable';
    const reasons=valid&&Array.isArray(health?.reasons)?health.reasons.filter(s=>typeof s==='string'):[];
    if(stale)reasons.unshift('Executive evidence is over 45 minutes old. Check the observer and Pages deployment; the last health status is not current.');
    if(!valid)reasons.unshift('Executive evidence could not be verified. No healthy status is assumed.');
    byId('executive-message').textContent=reasons[0]||'The available evidence establishes current mailbox sync and completed attachment backfill.';
    const inv=valid?evidence.inventory:null,sync=valid?evidence.sync:null,bf=valid?evidence.backfill:null,r=valid?evidence.reliability:null;
    byId('executive-emails').textContent=count(inv?.emails);byId('executive-attachments').textContent=count(inv?.attachments);byId('executive-storage').textContent=finite(inv?.storage_gib)?`${inv.storage_gib.toFixed(2)} GiB`:'Unknown';
    byId('executive-inventory-time').textContent=inv?`Measured ${age(inv.measured_at)} · ${time(inv.measured_at)}`:'Inventory unavailable';
    byId('executive-freshness').textContent=sync?age(sync.last_success_at):'Unknown';byId('executive-success-time').textContent=sync?time(sync.last_success_at):'No verified successful mailbox sync';
    byId('executive-backfill').textContent=bf?bf.complete===true?'Complete':bf.complete===false?'Incomplete':'Unknown':'Unknown';
    byId('executive-progress').textContent=bf?`${count(bf.scanned)} messages scanned · ${count(bf.attachments)} attachments captured. Last committed progress ${time(bf.updated_at)}. No percentage is inferred without a verified total.`:'No verified attachment checkpoint.';
    byId('executive-reliability').textContent=r&&finite(r.completed)&&r.completed>0&&finite(r.successful)?`${(100*r.successful/r.completed).toFixed(1)}% · ${count(r.failed)} failed / ${count(r.completed)} completed`:'Unknown — no classified completed sync jobs';
    byId('executive-reliability-note').textContent=`Connection tests, running/cancelled jobs and ${count(r?.unclassified)} unclassified legacy runs are excluded. A successful pass does not prove backfill is complete.`;
    const list=byId('executive-attention');list.replaceChildren();for(const reason of reasons.length?reasons:['No operational action is indicated by the current evidence.'])list.append(node('li',reason));
    chart('executive-email-chart',valid?evidence.history:[],'emails');chart('executive-attachment-chart',valid?evidence.history:[],'attachments');
  }
  async function load(){try{const r=await fetch(`executive.json?t=${Date.now()}`,{cache:'no-store'});if(!r.ok)throw new Error('Unavailable');evidence=await r.json();}catch{evidence=null;}render();}
  byId('reload').addEventListener('click',load);load();setInterval(load,60000);
})();
