import {afterEach,describe,expect,it,vi} from 'vitest';
const invoked=vi.hoisted(()=>({count:0}));
vi.mock('node:child_process',async original=>({...await original<object>(),execFile:(_file:string,_args:string[],_opts:unknown,callback:(error:null,stdout:string,stderr:string)=>void)=>{invoked.count++;callback(null,'','');}}));
import {execArgvCommand} from '../src/adapters/tmux-exec.js';
afterEach(()=>{delete process.env.OPENRIG_ELASTIC_GUARD;invoked.count=0;});
describe('native executor final seal check',()=>{it('denies the actual final spawn even if an earlier caller checked before awaiting',async()=>{await Promise.resolve();process.env.OPENRIG_ELASTIC_GUARD='/missing-elastic-marker';await expect(execArgvCommand(['tmux','send-keys','-t','seat','Enter'])).rejects.toThrow('elastic_worker_effect_denied');expect(invoked.count).toBe(0);});});
