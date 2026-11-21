// Multiplayer server for "Birket Mai" - shared world. No npm packages needed.
const http=require('http'),fs=require('fs'),path=require('path'),crypto=require('crypto');
const PORT=process.env.PORT||3000, MAX=20, GUID='258EAFA5-E914-47DA-95CA-C5AB0DC85B11', START=Date.now();
const page=()=>fs.readFileSync(path.join(__dirname,'index.html'));
const players=new Map(); let nextId=1;
const cleanName=n=>String(n||'').replace(/[<>&"']/g,'').trim().slice(0,14)||'لاعب';
function allList(now){const list=[];for(const [id,p] of players)if(p.s&&now-p.last<5000)list.push([id,p.name,p.c,p.s.x,p.s.y,p.s.z,p.s.r,p.s.d,p.s.l,p.s.m]);return list;}
function deliver(p,ev){if(p.sock)send(p,ev);else if(p.q){p.q.push(ev);if(p.q.length>80)p.q.shift();}}
function applyState(p,m){
  let mm=0;
  if(Array.isArray(m.m)&&m.m.length===7)mm=[num(m.m[0]),num(m.m[1]),String(m.m[2]).slice(0,8),num(m.m[3]),m.m[4]?1:0,num(m.m[5]),m.m[6]?1:0];
  p.s={x:num(m.x),y:num(m.y),z:num(m.z),r:num(m.r),d:m.d?1:0,l:(+m.l|0)||1,m:mm};p.last=Date.now();
}
function applyEvent(id,p,m){
  if(!m||!KINDS.has(m.k))return;
  const now=Date.now();if(now-p.evT>1000){p.evT=now;p.evN=0;}if(++p.evN>60)return;
  const ev={t:'e',f:id,k:m.k,l:(+m.l|0)||1,a:(Array.isArray(m.a)?m.a:[]).slice(0,4).map(v=>typeof v==='number'?num(v):v?1:0)};
  for(const [oid,o] of players)if(oid!==id&&o.name)deliver(o,ev);
}
const server=http.createServer((req,res)=>{
  if(req.url==='/health'){res.end('ok');return;}
  if(req.method==='POST'&&req.url==='/sync'){
    let b='';
    req.on('data',d=>{b+=d;if(b.length>4096)req.destroy();});
    req.on('end',()=>{
      let m;try{m=JSON.parse(b);}catch(e){res.writeHead(400);res.end();return;}
      let id=+m.id|0,p=players.get(id);
      if(!p||p.sock){
        if(players.size>=MAX){res.writeHead(503);res.end();return;}
        id=nextId++;p={sock:null,q:[],name:cleanName(m.name),c:(+m.c||0)%8,s:null,last:0,evN:0,evT:0,seen:0};players.set(id,p);
      }
      p.seen=Date.now();
      if(m.s)applyState(p,m.s);
      if(Array.isArray(m.e))for(const e of m.e.slice(0,10))applyEvent(id,p,e);
      const out={id,T:Date.now()-START,list:allList(Date.now()),ev:p.q.splice(0)};
      res.writeHead(200,{'Content-Type':'application/json','Cache-Control':'no-store'});res.end(JSON.stringify(out));
    });
    return;
  }
  res.writeHead(200,{'Content-Type':'text/html; charset=utf-8','Cache-Control':'no-cache'});
  res.end(page());
});
function frame(str){
  const b=Buffer.from(str),n=b.length;let h;
  if(n<126)h=Buffer.from([0x81,n]);
  else if(n<65536){h=Buffer.alloc(4);h[0]=0x81;h[1]=126;h.writeUInt16BE(n,2);}
  else{h=Buffer.alloc(10);h[0]=0x81;h[1]=127;h.writeBigUInt64BE(BigInt(n),2);}
  return Buffer.concat([h,b]);
}
function send(p,obj){try{p.sock.write(frame(JSON.stringify(obj)));}catch(e){}}
function drop(id){const p=players.get(id);if(!p)return;players.delete(id);try{p.sock.destroy();}catch(e){}}
const num=v=>Number.isFinite(+v)?Math.round(+v*100)/100:0;
const KINDS=new Set(['w','wd','bomb','blk']);

server.on('upgrade',(req,sock)=>{
  if(req.url!=='/ws'||players.size>=MAX){sock.destroy();return;}
  const key=req.headers['sec-websocket-key'];if(!key){sock.destroy();return;}
  const acc=crypto.createHash('sha1').update(key+GUID).digest('base64');
  sock.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: '+acc+'\r\n\r\n');
  const id=nextId++, p={sock,name:'',c:0,s:null,last:0,evN:0,evT:0};players.set(id,p);
  let buf=Buffer.alloc(0);
  sock.on('data',d=>{
    buf=Buffer.concat([buf,d]);if(buf.length>8192){drop(id);return;}
    while(buf.length>=2){
      const op=buf[0]&15,masked=buf[1]&128;let len=buf[1]&127,off=2;
      if(len===126){if(buf.length<4)return;len=buf.readUInt16BE(2);off=4;}
      else if(len===127){drop(id);return;}
      if(len>2048||!masked){drop(id);return;}
      if(buf.length<off+4+len)return;
      const mask=buf.slice(off,off+4),data=Buffer.from(buf.slice(off+4,off+4+len));
      for(let i=0;i<len;i++)data[i]^=mask[i&3];
      buf=buf.slice(off+4+len);
      if(op===8){drop(id);return;}
      if(op===9){sock.write(Buffer.concat([Buffer.from([0x8a,len]),data]));continue;}
      if(op!==1)continue;
      let m;try{m=JSON.parse(data.toString());}catch(e){continue;}
      if(m.t==='join'){p.name=cleanName(m.name);p.c=(+m.c||0)%8;send(p,{t:'hi',id});}
      else if(m.t==='s'&&p.name)applyState(p,m);
      else if(m.t==='e'&&p.name)applyEvent(id,p,m);
    }
  });
  sock.on('close',()=>players.delete(id));
  sock.on('error',()=>players.delete(id));
});
setInterval(()=>{
  const now=Date.now(),msg={t:'p',T:now-START,list:allList(now)};
  for(const [id,p] of players)if(p.sock&&p.name)send(p,msg);
  for(const [id,p] of players)if(!p.sock&&now-p.seen>6000)players.delete(id);
},66);
server.listen(PORT,()=>console.log('Game running on port '+PORT));
