import {execFileSync} from 'node:child_process';
import {mkdtempSync, readFileSync, rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {fileURLToPath} from 'node:url';
import {after} from 'node:test';
import {parse} from 'yaml';

const workflow = parse(readFileSync(new URL('../workflows/backend-deploy.yml', import.meta.url), 'utf8'));
const planner = workflow.jobs['wiki-plan'].steps.find(step => step.with?.operation === 'plan-wiki');
const match = /^gaulatti\/clouseau@([a-f0-9]{40})$/.exec(planner.uses);
if (!match) throw new Error('Wiki tests require an immutable public Clouseau revision');
export const pin = match[1];
export const root = mkdtempSync(join(tmpdir(), 'colombo-pinned-wiki-'));
after(() => rmSync(root, {recursive: true, force: true}));
const repository = fileURLToPath(new URL('../../.clouseau-ci/', import.meta.url));
const archive = execFileSync('git', ['-C', repository, 'archive', pin, 'src', 'policy', 'action.yml']);
execFileSync('tar', ['-x', '-C', root], {input: archive});
