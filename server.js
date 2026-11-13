// Standalone multiplayer server for "Birket Mai" - no npm packages needed.
const http=require('http'),fs=require('fs'),path=require('path'),crypto=require('crypto');
const PORT=process.env.PORT||3000, MAX=20, GUID='258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
const page=()=>fs.readFileSync(path.join(__dirname,'index.html'));
const players=new Map(); let nextId=1;

const server=http.createServer((req,res)=>{
  if(req.url==='/health'){res.end('ok');return;}
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

server.on('upgrade',(req,sock)=>{
  if(req.url!=='/ws'||players.size>=MAX){sock.destroy();return;}
  const key=req.headers['sec-websocket-key'];if(!key){sock.destroy();return;}
  const acc=crypto.createHash('sha1').update(key+GUID).digest('base64');
  sock.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: '+acc+'\r\n\r\n');
  const id=nextId++, p={sock,name:'',c:0,s:null,last:0};players.set(id,p);
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
      if(m.t==='join'){p.name=String(m.name||'').replace(/[<>&"']/g,'').trim().slice(0,14)||'لاعب';p.c=(+m.c||0)%8;send(p,{t:'hi',id});}
      else if(m.t==='s'&&p.name){p.s={x:num(m.x),y:num(m.y),z:num(m.z),r:num(m.r),d:m.d?1:0};p.last=Date.now();}
    }
  });
  sock.on('close',()=>players.delete(id));
  sock.on('error',()=>players.delete(id));
});

setInterval(()=>{
  const now=Date.now(),list=[];
  for(const [id,p] of players)if(p.s&&now-p.last<5000)list.push([id,p.name,p.c,p.s.x,p.s.y,p.s.z,p.s.r,p.s.d]);
  for(const [id,p] of players)if(p.name)send(p,{t:'p',list});
},66);

server.listen(PORT,()=>console.log('Game running on port '+PORT));
