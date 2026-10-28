import {execFileSync} from 'node:child_process';
import {mkdtempSync, readFileSync, rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {fileURLToPath} from 'node:url';
import {after} from 'node:test';
import {parse} from 'yaml';

const workflow = parse(readFileSync(new URL('../workflows/backend-deploy.yml', import.meta.url), 'utf8'));
const planner = workflow.jobs['wiki-plan'].steps.find(step => step.with?.operation === 'plan-wiki');
if (planner.uses !== './.clouseau-ci') throw new Error('Wiki tests require the run-frozen Clouseau engine');
export const root = mkdtempSync(join(tmpdir(), 'colombo-frozen-wiki-'));
after(() => rmSync(root, {recursive: true, force: true}));
const repository = fileURLToPath(new URL('../../.clouseau-ci/', import.meta.url));
// The wiki jobs execute the engine this run froze; test exactly that revision.
export const pin = execFileSync('git', ['-C', repository, 'rev-parse', 'HEAD'], {encoding: 'utf8'}).trim();
if (pin !== process.env.CLOUSEAU_ENGINE_SHA) throw new Error('Wiki tests require .clouseau-ci at CLOUSEAU_ENGINE_SHA');
const archive = execFileSync('git', ['-C', repository, 'archive', pin, 'src', 'policy', 'action.yml']);
execFileSync('tar', ['-x', '-C', root], {input: archive});
