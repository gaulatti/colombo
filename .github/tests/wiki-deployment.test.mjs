import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {createRequire} from 'node:module';
import {fileURLToPath} from 'node:url';
import test from 'node:test';
import {parse} from 'yaml';
import {allowed} from './workflow-expressions.mjs';

const workflow = parse(readFileSync(new URL('../workflows/backend-deploy.yml', import.meta.url), 'utf8'));
const ci = parse(readFileSync(new URL('../workflows/backend-ci.yml', import.meta.url), 'utf8'));
const config = JSON.parse(readFileSync(new URL('../clouseau-wiki.json', import.meta.url), 'utf8'));
const require = createRequire(import.meta.url);
const shared = fileURLToPath(new URL('../../.clouseau-ci/src/', import.meta.url));
const {planWiki, validateBundle, validateConfig, evaluateWiki} = require(`${shared}/wiki.cjs`);
const {publishWiki} = require(`${shared}/wiki-git.cjs`);
const facts = (pages, source) => ({supported: true, findings: [], evidence: pages.map(page => ({page: page.path, source_id: `${Object.keys(source)[0]}:L1`}))});
const identity = {repository: 'example/upload-gateway', source_sha: 'a'.repeat(40), wiki_base_sha: 'b'.repeat(40), deployment_run_id: 123};
const context = (event, ref, draft, deployed = 'success', planned = 'success') => ({
  github: {event_name: event, ref}, inputs: {validate_wiki_only: draft},
  needs: {deploy: {result: deployed}, 'wiki-plan': {result: planned}},
});

for (const [label, c, expected] of [
  ['successful main push', context('push', 'refs/heads/main', false), [true, true, true, true, false]],
  ['successful main dispatch', context('workflow_dispatch', 'refs/heads/main', false), [true, true, true, true, false]],
  ['branch preflight', context('workflow_dispatch', 'refs/heads/test', true, 'skipped'), [false, false, true, false, true]],
  ['main preflight', context('workflow_dispatch', 'refs/heads/main', true, 'skipped'), [false, false, true, false, true]],
  ['unintended branch deploy', context('workflow_dispatch', 'refs/heads/test', false), [false, false, false, false, false]],
  ['failed deployment', context('push', 'refs/heads/main', false, 'failure'), [true, true, false, false, false]],
  ['cancelled deployment', context('push', 'refs/heads/main', false, 'cancelled'), [true, true, false, false, false]],
  ['failed generation', context('push', 'refs/heads/main', false, 'success', 'failure'), [true, true, true, false, false]],
]) {
  test(`wiki workflow routing: ${label}`, () => {
    const names = ['build-and-push', 'deploy', 'wiki-plan', 'wiki-publish', 'wiki-access'];
    assert.deepEqual(names.map(name => allowed(workflow.jobs[name].if, c)), expected);
    // GitHub's implicit success() must not prevent a safe draft after skipped deploy.
    if (c.inputs.validate_wiki_only) assert.ok(allowed(workflow.jobs['wiki-plan'].if, c, {success: false}));
    // Explicit always() guards still refuse failed deployment and generation.
    if (c.needs.deploy.result === 'failure') assert.equal(allowed(workflow.jobs['wiki-publish'].if, c, {success: false}), false);
  });
}

test('deployment graph binds generation and atomic publication to the same exact revision and run', () => {
  validateConfig(config);
  assert.equal(config.deployment_workflow, 'backend-deploy.yml');
  assert.equal(config.deployment_job, 'deploy');
  assert.equal(config.wiki_branch, 'master');
  assert.equal(workflow.concurrency['cancel-in-progress'], false);
  assert.equal(workflow.jobs['wiki-plan'].needs, 'deploy');
  assert.deepEqual(workflow.jobs['wiki-publish'].needs, ['deploy', 'wiki-plan']);
  const plan = workflow.jobs['wiki-plan'].steps;
  const publish = workflow.jobs['wiki-publish'].steps;
  for (const steps of [plan, publish]) {
    assert.equal(steps[0].with.ref, '${{ github.sha }}');
    assert.equal(steps[0].with['persist-credentials'], false);
    assert.equal(steps[1].with.repository, '${{ github.repository }}.wiki');
    assert.equal(steps[1].with.ref, config.wiki_branch);
  }
  const upload = plan.find(step => step.uses?.startsWith('actions/upload-artifact@'));
  const download = publish.find(step => step.uses?.startsWith('actions/download-artifact@'));
  assert.equal(upload.with.name, 'wiki-plan-${{ github.run_id }}-${{ github.sha }}');
  assert.equal(download.with.name, upload.with.name);
  assert.equal(upload.with['if-no-files-found'], 'error');
  assert.equal(upload.if, 'always()', 'keep factual diagnostics even when the draft is rejected');
  assert.equal(upload.with.path, 'work/wiki-plan.json*');
  assert.equal(download.with['run-id'], undefined, 'artifacts must come from this same workflow run');
  const pin = ci.jobs.verify.steps.find(step => step.with?.repository === 'gaulatti/clouseau').with.ref;
  for (const step of [...plan, ...publish].filter(step => step.uses?.startsWith('gaulatti/clouseau@'))) {
    assert.equal(step.uses, `gaulatti/clouseau@${pin}`);
  }
});

test('provider and publication authority are isolated and preflight cannot write', () => {
  assert.deepEqual(workflow.permissions, {});
  assert.deepEqual(workflow.jobs['wiki-plan'].permissions, {contents: 'read', actions: 'read'});
  assert.deepEqual(workflow.jobs['wiki-publish'].permissions, {contents: 'write', actions: 'read'});
  for (const [name, job] of Object.entries(workflow.jobs)) {
    const text = JSON.stringify(job);
    if (name !== 'wiki-plan') assert.ok(!text.includes('DEEPSEEK_API_KEY'));
    if (name.startsWith('wiki-')) assert.ok(!text.includes('secrets.DEPLOY'));
  }
  const access = workflow.jobs['wiki-access'].steps.at(-1);
  assert.equal(access.run, 'git push --dry-run origin HEAD:refs/heads/master');
  assert.equal(workflow.jobs['wiki-plan'].steps[1].with['persist-credentials'], false);
  const generate = workflow.jobs['wiki-plan'].steps.find(step => step.with?.operation === 'plan-wiki');
  const diagnostic = workflow.jobs['wiki-plan'].steps.find(step => step.with?.operation === 'evaluate-wiki');
  assert.equal(diagnostic.if, 'inputs.validate_wiki_only');
  assert.ok(workflow.jobs['wiki-plan'].steps.indexOf(diagnostic) < workflow.jobs['wiki-plan'].steps.indexOf(generate));
  assert.equal(diagnostic.with['provider-key'], '${{ secrets.DEEPSEEK_API_KEY }}');
  assert.equal(generate.with['wiki-draft'], "${{ inputs.validate_wiki_only && 'true' || 'false' }}");
});

test('pinned production planner coordinates multi-page navigation and preserves human text', async () => {
  const wiki = {'Home.md': '# Home\nHuman introduction.\n[Deployment](Deployment)', 'Deployment.md': '# Deployment\nOld deployment.'};
  const pages = [
    {path: 'Home.md', content: '# Home\nHuman introduction.\n[Deployment](Deployment)\n[Wiki maintenance](Wiki-Maintenance)'},
    {path: 'Deployment.md', content: '# Deployment\nPersistent spool.\n[Wiki maintenance](Wiki-Maintenance)'},
    {path: 'Wiki-Maintenance.md', content: '# Wiki maintenance\nPublish only after deployment.\n[Deployment](Deployment)'},
  ];
  const requests = [];
  const source = {'README.md': 'Spool is persistent. Update wiki after deployment.'};
  const bundle = await planWiki({identity, source, wiki,
    complete: async ({messages}) => {
      const evidence = JSON.parse(messages[1].content); requests.push(evidence);
      if (evidence.proposed_update) return {content: JSON.stringify(facts(evidence.proposed_update, source))};
      return {content: JSON.stringify(requests.length === 1 ? {pages: pages.map(page => ({path: page.path, reason: 'Deployment evidence'}))} : pages[requests.length - 2])};
    }});
  assert.equal(requests.length, 5);
  assert.deepEqual(requests[3].proposed_pages, pages.slice(0, 2));
  assert.deepEqual(bundle.updates, pages);
  assert.match(bundle.updates[0].content, /Human introduction/);
  assert.deepEqual(validateBundle(bundle, identity, wiki), bundle);
});

test('pinned production planner rejects incomplete batches, broken links and unauthorized pages', async () => {
  const wiki = {'Home.md': '# Home'};
  const invalid = [
    {path: 'Home.md', content: '[Missing](Missing)'},
    {path: '../outside.md', content: 'Outside'},
    {path: 'AGENTS.md', content: 'Rules'},
  ];
  for (const page of invalid) {
    let calls = 0;
    await assert.rejects(planWiki({identity, source: {}, wiki, complete: async () => ({content: JSON.stringify(++calls === 1 ? {pages: [{path: 'Home.md', reason: 'Maintenance'}]} : page)})}));
  }
  let calls = 0;
  await assert.rejects(planWiki({identity, source: {}, wiki, complete: async () => ++calls === 1 ?
    {content: JSON.stringify({pages: [{path: 'Home.md', reason: 'Maintenance'}]})} : {failure: 'Provider stopped'}}), /nothing will be published/);
  assert.equal(calls, 2);
});

test('publisher independently refuses a failed real deployment before any Git write', async () => {
  const apiContext = {repo: {owner: 'example', repo: 'upload-gateway'}, sha: identity.source_sha, runId: identity.deployment_run_id};
  const github = {rest: {actions: {getWorkflowRun: async () => ({data: {head_sha: identity.source_sha, head_branch: 'main', path: '.github/workflows/backend-deploy.yml'}}), listJobsForWorkflowRun: () => {}}},
    paginate: async () => [{name: 'deploy', status: 'completed', conclusion: 'failure'}]};
  await assert.rejects(publishWiki({github, context: apiContext, config, sourceDirectory: '/never-read', wikiDirectory: '/never-written', bundle: {}}), /completed successful deployment/);
});


test('wiki-only format recovery keeps evidence identical and rejects an exhausted envelope budget', async () => {
  const wiki = {'Home.md': '# Home'};
  const page = {path: 'Home.md', content: '# Home\nPreserved introduction.'};
  const requests = [];
  const source = {'README.md': 'Upload gateway deployment.'};
  const result = await planWiki({identity, source, wiki, complete: async ({messages}) => {
    requests.push(messages);
    if (JSON.parse(messages[1].content).proposed_update) return {content: JSON.stringify(facts([page], source))};
    return {content: requests.length === 1 ? JSON.stringify({pages: [{path: 'Home.md', reason: 'Deployment'}]}) :
      requests.length === 2 ? JSON.stringify(page) + '\n' + JSON.stringify(page) : JSON.stringify(page)};
  }});
  assert.deepEqual(result.updates, [page]);
  assert.equal(requests.length, 4);
  assert.deepEqual(requests[1][1], requests[2][1]);
  let calls = 0;
  await assert.rejects(planWiki({identity, source: {}, wiki, complete: async () => {calls++; return {content: '{malformed'};}}), /invalid JSON twice/);
  assert.equal(calls, 2);
});


test('factual gate blocks the live-preflight error: temporary CMS failure is not denied credentials', async () => {
  const source = {'src/cms.rs': 'Err(ValidationError::Unavailable(error.into()))'};
  const wiki = {'Home.md': '# Home'};
  const page = {path: 'Home.md', content: '# Home\nNetwork failures are denial.'};
  for (const review of [
    {supported: false, findings: ['Network failures are Unavailable in production, not Denied.'], evidence: []},
    {supported: true, findings: [], evidence: [{page: 'Home.md', source_id: 'src/cms.rs:L99999'}]},
    {supported: true, findings: [], evidence: []},
  ]) {
    let calls = 0;
    await assert.rejects(planWiki({identity, source, wiki, allowCorrection: false, complete: async () => ({content: JSON.stringify(++calls === 1 ?
      {pages: [{path: page.path, reason: 'Authentication contract'}]} : calls === 2 ? page : review)})}));
    assert.equal(calls, 3, 'a substantive factual rejection is final, never retried until favorable');
  }
});


test('live wiki diagnostic uses the production non-thinking transport while retaining substantive negative review', async () => {
  const original = globalThis.fetch;
  const requests = [];
  globalThis.fetch = async (_, options) => {
    const body = JSON.parse(options.body); requests.push(body);
    const evidence = JSON.parse(body.messages[1].content);
    const supported = evidence.proposed_update[0].content.includes('Temporary upstream unavailability returns 502');
    const verdict = supported ? {supported: true, findings: [], evidence: evidence.proposed_update.map(page => ({page: page.path, source_id: Object.keys(evidence.source_evidence)[0]}))} :
      {supported: false, findings: ['The production match maps Unavailable to 502.'], evidence: []};
    return {ok: true, json: async () => ({choices: [{finish_reason: 'stop', message: {content: JSON.stringify(verdict)}}]})};
  };
  try {await evaluateWiki();}
  finally {globalThis.fetch = original;}
  assert.equal(requests.length, 2);
  for (const body of requests) {
    assert.deepEqual(body.thinking, {type: 'disabled'});
    assert.equal(body.reasoning_effort, undefined);
    assert.deepEqual(body.response_format, {type: 'json_object'});
    assert.equal(body.tools, undefined);
  }
});

test('a rejected wiki draft is corrected from findings and only changed, freshly supported text is returned', async () => {
  const source = {'src/auth.rs': 'Failure::Unavailable => 502'};
  const wiki = {'Home.md': '# Home'};
  const requests = [], diagnostics = [];
  const bundle = await planWiki({identity, source, wiki, diagnostic: result => diagnostics.push(result), complete: async ({messages}) => {
    const input = JSON.parse(messages[1].content); requests.push(input);
    if (input.proposed_update) return {content: JSON.stringify(input.proposed_update[0].content.includes('502') ? facts(input.proposed_update, source) :
      {supported: false, findings: ['Unavailable maps to 502, not 401.'], evidence: []})};
    if (input.requested_page) return {content: JSON.stringify({path: 'Home.md', content: '# Home\nUnavailable returns ' + (input.review_findings.length ? '502' : '401')})};
    return {content: JSON.stringify({pages: [{path: 'Home.md', reason: 'HTTP status contract'}]})};
  }});
  assert.equal(requests.length, 5);
  assert.deepEqual(requests[3].review_findings, ['Unavailable maps to 502, not 401.']);
  assert.deepEqual(requests[3].previous_update, requests[2].proposed_update);
  assert.notDeepEqual(requests[2].proposed_update, requests[4].proposed_update);
  assert.equal(bundle.updates[0].content, '# Home\nUnavailable returns 502');
  assert.deepEqual(diagnostics.map(result => result.revision), [0, 1]);
});

test('wiki correction never resubmits identical rejected text or continues after a second rejection', async () => {
  for (const unchanged of [true, false]) {
    let renders = 0, reviews = 0;
    await assert.rejects(planWiki({identity, source: {'README.md': 'Unavailable returns 502.'}, wiki: {'Home.md': '# Home'}, complete: async ({messages}) => {
      const input = JSON.parse(messages[1].content);
      if (input.proposed_update) {reviews++; return {content: JSON.stringify({supported: false, findings: ['Wrong status.'], evidence: []})};}
      if (input.requested_page) {renders++; return {content: JSON.stringify({path: 'Home.md', content: '# Home\nWrong status.' + (!unchanged && renders === 2 ? ' Still wrong.' : '')})};}
      return {content: JSON.stringify({pages: [{path: 'Home.md', reason: 'HTTP status'}]})};
    }}), unchanged ? /did not change/ : /did not support/);
    assert.equal(renders, 2);
    assert.equal(reviews, unchanged ? 1 : 2);
  }
});

test('wiki citations resolve immutable source lines and refuse blanks, invented IDs and model quotations', async () => {
  const source = {'README.md': '\n# Upload gateway\n'};
  for (const evidence of [
    {page: 'Home.md', source_id: 'README.md:L2'},
    {page: 'Home.md', source_id: 'README.md:L1'},
    {page: 'Home.md', source_id: 'README.md:L9999'},
    {page: 'Home.md', source_id: 'README.md:L2', quote: 'invented quotation'},
  ]) {
    const diagnostics = [];
    let calls = 0;
    const result = planWiki({identity, source, wiki: {'Home.md': '# Home'}, diagnostic: value => diagnostics.push(value), complete: async ({messages}) => {
      calls++;
      const input = JSON.parse(messages[1].content);
      return {content: JSON.stringify(input.proposed_update ? {supported: true, findings: [], evidence: [evidence]} :
        input.requested_page ? {path: 'Home.md', content: '# Upload gateway'} : {pages: [{path: 'Home.md', reason: 'Gateway documentation'}]})};
    }});
    if (evidence.source_id === 'README.md:L2' && !Object.hasOwn(evidence, 'quote')) assert.equal((await result).updates[0].content, '# Upload gateway');
    else await assert.rejects(result, /exact source evidence/);
    assert.equal(calls, 3);
    assert.deepEqual(diagnostics[0].source_evidence, {'README.md:L1': '', 'README.md:L2': '# Upload gateway', 'README.md:L3': ''});
  }
});
