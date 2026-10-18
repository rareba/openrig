import { describe, it, expect } from 'vitest';
import { Hono } from 'hono';
import { elasticWorkerRoutes } from '../src/routes/elastic-worker.js';
describe('elastic worker read-only capability routes',()=>{
 it('returns unavailable for unenrolled/old capability and requires explicit bearer for reads',async()=>{
  const missing=new Hono().route('/api/elastic-worker',elasticWorkerRoutes({bearerToken:'secret'}));
  expect((await missing.request('/api/elastic-worker/probe',{headers:{Authorization:'Bearer secret'}})).status).toBe(503);
  expect((await missing.request('/api/elastic-worker/probe')).status).toBe(401);
  const noAuth=new Hono().route('/api/elastic-worker',elasticWorkerRoutes({bearerToken:null}));expect((await noAuth.request('/api/elastic-worker/probe')).status).toBe(503);
 });
 it('returns only bounded generic errors from failing native observations',async()=>{
  const app=new Hono().route('/api/elastic-worker',elasticWorkerRoutes({bearerToken:'secret',probe:()=>{throw Error('plaintext-token');}}));
  const response=await app.request('/api/elastic-worker/probe',{headers:{Authorization:'Bearer secret'}});expect(response.status).toBe(503);expect(await response.text()).not.toContain('plaintext-token');
 });
});
