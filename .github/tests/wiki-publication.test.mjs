import assert from 'node:assert/strict';
import {execFileSync} from 'node:child_process';
import {mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync} from 'node:fs';
import {createRequire} from 'node:module';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {fileURLToPath} from 'node:url';
import test from 'node:test';

const root = fileURLToPath(new URL('../../.clouseau-ci/src/', import.meta.url));
const {snapshot, publishWiki} = createRequire(import.meta.url)(`${root}/wiki-git.cjs`);
const config = JSON.parse(readFileSync(new URL('../clouseau-wiki.json', import.meta.url)));
const git = (directory, args) => execFileSync('git', ['-C', directory, ...args], {encoding: 'utf8', stdio: 'pipe'}).trim();
function commit(directory, files) {
  for (const [path, content] of Object.entries(files)) writeFileSync(join(directory, path), content);
  git(directory, ['add', '--all']); git(directory, ['commit', '-m', 'fictional fixture']);
  return git(directory, ['rev-parse', 'HEAD']);
}
function fixture() {
  const directory = mkdtempSync(join(tmpdir(), 'colombo-wiki-'));
  const source = join(directory, 'source'), wiki = join(directory, 'wiki'), remote = join(directory, 'wiki.git');
  mkdirSync(source); git(source, ['init', '-b', 'main']);
  git(source, ['config', 'user.name', 'Fixture']); git(source, ['config', 'user.email', 'fixture@example.invalid']);
  const sha = commit(source, {'README.md': 'Uploads use a persistent spool.'});
  mkdirSync(remote); git(remote, ['init', '--bare', '-b', 'master']);
  git(directory, ['clone', remote, wiki]);
  git(wiki, ['config', 'user.name', 'Fixture']); git(wiki, ['config', 'user.email', 'fixture@example.invalid']);
  commit(wiki, {'Home.md': '# Home\n[Deployment](Deployment)', 'Deployment.md': '# Deployment\nOriginal text.'});
  git(wiki, ['push', 'origin', 'master']);
  const context = {repo: {owner: 'example', repo: 'gateway'}, sha, runId: 123};
  const github = {rest: {actions: {getWorkflowRun: async () => ({data: {head_sha: sha, head_branch: 'main', path: '.github/workflows/backend-deploy.yml'}}), listJobsForWorkflowRun: () => {}}},
    paginate: async () => [{name: 'deploy', status: 'completed', conclusion: 'success'}]};
  const current = snapshot({sourceDirectory: source, wikiDirectory: wiki, config, repository: 'example/gateway', revision: sha, runId: 123});
  const updates = [
    {path: 'Home.md', content: '# Home\n[Deployment](Deployment) [Operations](Operations)'},
    {path: 'Deployment.md', content: '# Deployment\nPersistent spool. [Operations](Operations)'},
    {path: 'Operations.md', content: '# Operations\n[Deployment](Deployment)'},
  ];
  const bundle = {version: 1, ...current.identity, updates};
  return {directory, source, wiki, remote, context, current, bundle,
    publish: () => publishWiki({github, context, config, sourceDirectory: source, wikiDirectory: wiki, bundle}),
    close: () => rmSync(directory, {recursive: true, force: true})};
}

test('real pinned publisher atomically pushes every page and exact deployment receipt to a real Git remote', async () => {
  const f = fixture();
  try {
    const before = git(f.remote, ['rev-parse', 'master']);
    const result = await f.publish();
    assert.equal(result.changedPages, 3);
    assert.equal(git(f.remote, ['rev-parse', 'master']), result.commit);
    assert.equal(git(f.remote, ['rev-parse', 'master^']), before, 'one child commit publishes the whole batch');
    assert.deepEqual(git(f.remote, ['diff', '--name-only', before, 'master']).split('\n').sort(), ['.clouseau-deployment.json', 'Deployment.md', 'Home.md', 'Operations.md']);
    for (const page of f.bundle.updates) assert.equal(git(f.remote, ['show', `master:${page.path}`]), page.content);
    const receipt = JSON.parse(git(f.remote, ['show', 'master:.clouseau-deployment.json']));
    assert.equal(receipt.source_sha, f.context.sha);
    assert.equal(receipt.deployment_run_id, f.context.runId);
    assert.equal(receipt.repository, 'example/gateway');
    assert.equal(git(f.wiki, ['status', '--porcelain']), '', 'private publication index leaves the checkout untouched');
  } finally {f.close();}
});

test('real pinned publisher preserves a human edit pushed while generation used an older wiki revision', async () => {
  const f = fixture();
  try {
    const peer = join(f.directory, 'human'); git(f.directory, ['clone', f.remote, peer]);
    git(peer, ['config', 'user.name', 'Human']); git(peer, ['config', 'user.email', 'human@example.invalid']);
    const human = commit(peer, {'Deployment.md': '# Deployment\nHuman operational correction.'});
    git(peer, ['push', 'origin', 'master']);
    await assert.rejects(f.publish(), /Wiki changed/);
    assert.equal(git(f.remote, ['rev-parse', 'master']), human);
    assert.equal(git(f.remote, ['show', 'master:Deployment.md']), '# Deployment\nHuman operational correction.');
    assert.equal(git(f.remote, ['ls-tree', '--name-only', 'master']).includes('Operations.md'), false);
    assert.equal(git(f.remote, ['rev-list', '--count', 'master']), '2', 'no generated child commit was published');
  } finally {f.close();}
});
