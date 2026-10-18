/** Isolated RPC transport. Never reads credentials or calls a model/network. */
import { appendFileSync, readFileSync, existsSync } from 'node:fs';
import { createInterface } from 'node:readline';
import { randomUUID } from 'node:crypto';
const session=process.argv[process.argv.indexOf('--session')+1];
if(!session||!existsSync(session))throw Error('fixture_requires_exact_existing_session');
const header=JSON.parse(readFileSync(session,'utf8').split('\n')[0]);
if(header.type!=='session')throw Error('fixture_bad_session');
const reply=value=>process.stdout.write(JSON.stringify(value)+'\n');
createInterface({input:process.stdin}).on('line',line=>{
  const command=JSON.parse(line);
  if(command.type==='get_state')reply({type:'response',id:command.id,command:command.type,success:true,data:{sessionId:header.id,sessionFile:session,isStreaming:false,isCompacting:false,pendingMessageCount:0}});
  else if(command.type==='get_entries')reply({type:'response',id:command.id,command:command.type,success:true,data:{entries:[]}});
  else if(command.type==='prompt'){
    appendFileSync(session,JSON.stringify({type:'message',id:randomUUID(),message:{role:'user',content:[{type:'text',text:command.message}]}})+'\n');
    // A durable switch allows exercising an RPC effect whose acknowledgement is lost.
    if(!existsSync(session+'.drop-ack'))reply({type:'response',id:command.id,command:command.type,success:true});
    reply({type:'agent_start'});reply({type:'agent_end'});
  }
});
