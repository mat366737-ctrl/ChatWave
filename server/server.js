'use strict';
const http=require('http'), crypto=require('crypto'), fs=require('fs'), path=require('path');
const WebSocket=require('ws');
const PORT=process.env.PORT||8080, DATA_FILE=process.env.CHATWAVE_DATA||path.join(__dirname,'data.json');
let db={users:[],messages:[]};try{if(fs.existsSync(DATA_FILE))db=JSON.parse(fs.readFileSync(DATA_FILE,'utf8'));}catch(e){console.error('Cannot read data file:',e.message)}
const tokens=new Map(), sockets=new Map();
function persist(){const tmp=DATA_FILE+'.tmp';fs.writeFileSync(tmp,JSON.stringify(db));fs.renameSync(tmp,DATA_FILE)}
function sendJson(res,status,obj){res.writeHead(status,{'content-type':'application/json; charset=utf-8','access-control-allow-origin':'*','access-control-allow-headers':'Content-Type, Authorization','access-control-allow-methods':'GET,POST,OPTIONS'});res.end(JSON.stringify(obj))}
function readBody(req){return new Promise((resolve,reject)=>{let raw='';req.on('data',c=>{raw+=c;if(raw.length>1e6){reject(new Error('Слишком большой запрос'));req.destroy()}});req.on('end',()=>{try{resolve(JSON.parse(raw||'{}'))}catch{reject(new Error('Некорректный JSON'))}});req.on('error',reject)})}
function pub(u){return {name:u.name,email:u.email,avatar:u.avatar||'🌊',bio:u.bio||''}}
function auth(req){const token=(req.headers.authorization||'').replace(/^Bearer\s+/i,'');return tokens.get(token)||null}
function issue(u){const token=crypto.randomBytes(32).toString('hex');tokens.set(token,u.email);return token}
function hash(password,salt){return crypto.scryptSync(password,salt,64).toString('hex')}
const server=http.createServer(async(req,res)=>{if(req.method==='OPTIONS'){res.writeHead(204,{'access-control-allow-origin':'*','access-control-allow-headers':'Content-Type, Authorization','access-control-allow-methods':'GET,POST,OPTIONS'});return res.end()}
 const url=new URL(req.url,'http://localhost');
 if(url.pathname==='/health')return sendJson(res,200,{ok:true,service:'ChatWave messaging server'});
 try{
  if(req.method==='POST'&&['/api/register','/api/login'].includes(url.pathname)){
   const b=await readBody(req),email=String(b.email||'').trim().toLowerCase(),password=String(b.password||'');
   if(!/^\S+@\S+\.\S+$/.test(email)||email.length>254)return sendJson(res,400,{error:'Укажите корректный email.'});
   if(password.length<8||password.length>256)return sendJson(res,400,{error:'Пароль должен содержать минимум 8 символов.'});
   let u=db.users.find(x=>x.email===email);
   if(url.pathname==='/api/register'){
    if(u)return sendJson(res,409,{error:'Этот email уже зарегистрирован. Войдите в аккаунт.'});
    const salt=crypto.randomBytes(16).toString('hex');u={email,name:String(b.name||'').trim().slice(0,32),salt,passwordHash:hash(password,salt),avatar:['🌊','🦊','🐼','🐸','🐱'][crypto.randomInt(5)],bio:''};
    if(!u.name)return sendJson(res,400,{error:'Введите имя.'});db.users.push(u);persist();
   }else{if(!u||hash(password,u.salt)!==u.passwordHash)return sendJson(res,401,{error:'Неверный email или пароль.'})}
   return sendJson(res,200,{token:issue(u),user:pub(u)});
  }
  const userEmail=auth(req);if(!userEmail)return sendJson(res,401,{error:'Войдите в аккаунт заново.'});const me=db.users.find(u=>u.email===userEmail);if(!me)return sendJson(res,401,{error:'Аккаунт не найден.'});
  if(req.method==='GET'&&url.pathname==='/api/users'){
   const q=(url.searchParams.get('q')||'').toLowerCase();return sendJson(res,200,{users:db.users.filter(u=>u.email!==me.email&&(!q||u.name.toLowerCase().includes(q)||u.email.includes(q))).slice(0,100).map(pub)});
  }
  if(req.method==='GET'&&url.pathname==='/api/messages'){
   const other=String(url.searchParams.get('with')||'').toLowerCase();if(!db.users.some(u=>u.email===other))return sendJson(res,404,{error:'Пользователь не найден.'});
   const messages=db.messages.filter(m=>(m.from===me.email&&m.to===other)||(m.from===other&&m.to===me.email)).slice(-500).map(m=>({...m,fromName:db.users.find(u=>u.email===m.from)?.name||m.from}));return sendJson(res,200,{messages});
  }
  if(req.method==='POST'&&url.pathname==='/api/messages'){
   const b=await readBody(req),to=String(b.to||'').toLowerCase(),text=String(b.text||'').trim();if(!db.users.some(u=>u.email===to))return sendJson(res,404,{error:'Такой пользователь не зарегистрирован.'});if(to===me.email)return sendJson(res,400,{error:'Нельзя отправить сообщение самому себе.'});if(!text||text.length>5000)return sendJson(res,400,{error:'Сообщение пустое или слишком длинное (максимум 5000 символов).'});
   const message={id:crypto.randomBytes(12).toString('hex'),from:me.email,to,text,time:Date.now()};db.messages.push(message);persist();return sendJson(res,201,{message});
  }
  return sendJson(res,404,{error:'Маршрут не найден.'});
 }catch(e){console.error(e);return sendJson(res,400,{error:e.message||'Ошибка запроса.'})}
});
const wss=new WebSocket.Server({server});
function wsSend(ws,obj){if(ws&&ws.readyState===WebSocket.OPEN)ws.send(JSON.stringify(obj))}
wss.on('connection',ws=>{ws.email=null;ws.on('message',raw=>{let m;try{m=JSON.parse(raw.toString())}catch{return}if(m.type==='register'){const email=String(m.email||'').trim().toLowerCase();if(!email||!db.users.some(u=>u.email===email))return wsSend(ws,{type:'error',message:'Сначала зарегистрируйтесь в ChatWave'});ws.email=email;sockets.set(email,ws);return wsSend(ws,{type:'registered',email})}if(!ws.email)return wsSend(ws,{type:'error',message:'Не авторизовано'});const to=String(m.to||'').toLowerCase(),target=sockets.get(to);if(!target||target.readyState!==WebSocket.OPEN)return wsSend(ws,{type:'error',message:'Собеседник не в сети'});if(['call','answer','candidate','hangup'].includes(m.type))wsSend(target,{...m,from:ws.email,name:db.users.find(u=>u.email===ws.email)?.name||ws.email})});ws.on('close',()=>{if(ws.email&&sockets.get(ws.email)===ws)sockets.delete(ws.email)})});
server.listen(PORT,'0.0.0.0',()=>console.log(`ChatWave API and call signaling listening on ${PORT}`));
