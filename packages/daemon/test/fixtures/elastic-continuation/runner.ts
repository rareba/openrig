/** Synthetic pre-existing runner harness. Resume uses the actual production main. */
import { spawn } from 'node:child_process';
import { mkdirSync, writeFileSync } from 'node:fs';
import { createInterface } from 'node:readline';
import { RunnerCore, main, parseRunnerArgs } from '../../../src/adapters/pi-runner.js';
import { currentContinuationProcess } from '../../../src/adapters/elastic-owned-continuation.js';
import { piSeatPaths } from '../../../src/adapters/pi-runner-protocol.js';
const args=process.argv.slice(2);
if(args.includes('--elastic-continuation')) await main(args);
else {
  const a=parseRunnerArgs(args),paths=piSeatPaths(a.stateRoot,a.sessionName);
  mkdirSync(paths.agentDir,{recursive:true});mkdirSync(paths.sessionsDir,{recursive:true});
  const child=spawn(process.execPath,[process.env.OPENRIG_ELASTIC_PI!,'--session',a.sessionFile!],{stdio:['pipe','pipe','pipe']});
  const ids=currentContinuationProcess(child.pid!);
  const core=new RunnerCore({processIdentity:()=>ids,sendRpc:cmd=>{child.stdin.write(JSON.stringify(cmd)+'\n');},
    mirrorLine:line=>process.stdout.write(line+'\n'),mirrorAppend:text=>process.stdout.write(text),postActivity:()=>{},
    writeSidecar:state=>writeFileSync(paths.runnerStatePath,JSON.stringify(state)),now:()=>new Date().toISOString()},
    {sessionName:a.sessionName,nodeId:'node',generation:'occupant',launchId:a.launchId});
  createInterface({input:child.stdout}).on('line',line=>core.handlePiLine(line));
  createInterface({input:process.stdin}).on('line',line=>core.handleUserBlock(line));
  child.stderr.pipe(process.stderr);core.start();
  process.on('SIGTERM',()=>{child.kill('SIGTERM');});
  child.on('exit',()=>process.exit());
}
