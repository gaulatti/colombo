import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import test from 'node:test';
import {data, Evaluator, Lexer, Parser} from '@actions/expressions';
import {Expr, FunctionCall} from '@actions/expressions/ast';
import {truthy} from '@actions/expressions/result';
import {parse} from 'yaml';

const workflow = parse(readFileSync(new URL('../workflows/backend-ci.yml', import.meta.url), 'utf8'));
const config = JSON.parse(readFileSync(new URL('../clouseau.json', import.meta.url), 'utf8'));
const statuses = new Set(['always', 'success', 'failure', 'cancelled']);

function expressionData(value) {
  if (typeof value === 'boolean') return new data.BooleanData(value);
  if (typeof value === 'number') return new data.NumberData(value);
  if (typeof value === 'string') return new data.StringData(value);
  if (value == null) return new data.Null();
  return new data.Dictionary(...Object.entries(value).map(([key, value]) => ({key, value: expressionData(value)})));
}

function hasStatusFunction(node) {
  if (node instanceof FunctionCall && statuses.has(node.functionName.lexeme.toLowerCase())) return true;
  return Object.values(node).some(value =>
    value instanceof Expr ? hasStatusFunction(value) :
      Array.isArray(value) && value.some(child => child instanceof Expr && hasStatusFunction(child)));
}

// Use GitHub's expression engine on the actual YAML, including its implicit
// success() condition. This does not execute workflow commands or replace CI.
function evaluate(expression, context, {condition = false, success = true, cancelled = false} = {}) {
  const source = String(expression ?? 'success()').trim().replace(/^\$\{\{\s*|\s*\}\}$/g, '');
  const functions = new Map([...statuses].map(name => [name, {
    name, minArgs: 0, maxArgs: 0,
    call: () => new data.BooleanData({always: true, success, cancelled, failure: !success && !cancelled}[name]),
  }]));
  const ast = new Parser(new Lexer(source).lex().tokens, Object.keys(context), [...functions.values()]).parse();
  if (condition && !hasStatusFunction(ast) && !success) return new data.BooleanData(false);
  return new Evaluator(ast, expressionData(context), functions).evaluate();
}

function allowed(expression, context, options = {}) {
  return truthy(evaluate(expression, context, {condition: true, ...options}));
}

function context(event, {draft = false, pullRequest = '', provider = false, assessment = 'success'} = {}) {
  return {
    github: {event_name: event, event: {pull_request: {number: 14, draft}}, run_id: 1000},
    inputs: {pull_request: pullRequest, check_provider: provider},
    needs: {'assess-intent': {result: assessment}},
    steps: {coverage: {outcome: 'success'}},
  };
}

const cases = [
  ['PR', context('pull_request'), true, false],
  ['draft PR', context('pull_request', {draft: true}), true, false],
  ['PR review', context('pull_request_target'), false, true],
  ['draft review', context('pull_request_target', {draft: true}), false, false],
  ['comment', context('issue_comment'), false, false],
  ['manual tests', context('workflow_dispatch'), true, false],
  ['manual reassessment', context('workflow_dispatch', {pullRequest: '14'}), false, false],
  ['provider diagnostic', context('workflow_dispatch', {provider: true}), false, false],
];

function assertRoute(w, label, c, verify, approve) {
  const v = w.jobs.verify;
  assert.ok(allowed(v.if, c), `${label}: verification label must be resolved`);
  assert.equal(evaluate(v.name, c).coerceString(), verify ? 'verify' : 'metadata-only', `${label}: verification context`);
  assert.equal(evaluate(v.services.postgres.image, c).coerceString(), verify ? 'postgres:17-alpine' : '', `${label}: database routing`);
  for (const step of v.steps) {
    assert.equal(allowed(step.if, c), verify, `${label}: verification step ${step.name || step.uses}`);
  }
  const a = w.jobs['approve-and-merge'];
  assert.ok(allowed(a.if, c), `${label}: approval label must be resolved`);
  assert.equal(evaluate(a.name, c).coerceString(), approve ? 'approve-and-merge' : 'metadata-only-approval', `${label}: approval context`);
  assert.equal(allowed(a.steps[0].if, c), false, `${label}: successful assessment`);
  for (const step of a.steps.slice(1)) {
    assert.equal(allowed(step.if, c), approve, `${label}: approval step ${step.name || step.uses}`);
  }
}

for (const [label, c, verify, approve] of cases) {
  test(`${label}: readable labels and no unintended service, code, or approval work`, () => {
    assertRoute(workflow, label, c, verify, approve);
  });
}

for (const assessment of ['failure', 'cancelled', 'skipped']) {
  test(`${assessment} assessment fails the real approval job before checkout or approval`, () => {
    const c = context('pull_request_target', {assessment});
    const job = workflow.jobs['approve-and-merge'];
    assert.ok(allowed(job.if, c, {success: false}));
    assert.equal(evaluate(job.name, c).coerceString(), 'approve-and-merge');
    assert.ok(allowed(job.steps[0].if, c));
    assert.equal(job.steps[0].run.trim(), 'exit 1');
    for (const step of job.steps.slice(1)) assert.equal(allowed(step.if, c, {success: false}), false);
  });
}

test('failed verification cannot proceed to downstream build or coverage evidence', () => {
  const c = context('pull_request');
  c.steps.coverage.outcome = 'failure';
  for (const step of workflow.jobs.verify.steps) assert.equal(allowed(step.if, c, {success: false}), false);
});

test('trusted checkout, provider separation, and required evidence steps are preserved', () => {
  assert.deepEqual(workflow.jobs.verify.permissions, {contents: 'read'});
  assert.ok(!JSON.stringify(workflow.jobs.verify).includes('secrets.'));
  const approval = workflow.jobs['approve-and-merge'];
  const checkout = approval.steps.find(step => step.uses?.startsWith('actions/checkout@'));
  assert.equal(checkout.with.ref, 'main');
  assert.equal(checkout.with['persist-credentials'], false);
  assert.ok(!JSON.stringify(approval).includes('provider-key'));
  assert.ok(config.verification.steps.includes('Test workflow routing'));
  for (const name of config.verification.steps) {
    assert.ok(workflow.jobs.verify.steps.some(step => step.name === name), `required step ${name}`);
  }
});

test('the routing checks reject missing guards and a skipped assessment-failure path', () => {
  for (const jobName of ['verify', 'approve-and-merge']) {
    const mutant = structuredClone(workflow);
    const checkout = mutant.jobs[jobName].steps.find(step => step.uses?.startsWith('actions/checkout@'));
    delete checkout.if;
    assert.throws(() => assertRoute(mutant, 'comment', context('issue_comment'), false, false), /comment: .*step actions\/checkout/);
  }
  const mutant = structuredClone(workflow);
  mutant.jobs['approve-and-merge'].steps[0].if = 'false';
  const c = context('pull_request_target', {assessment: 'failure'});
  assert.equal(allowed(mutant.jobs['approve-and-merge'].steps[0].if, c), false);
  assert.ok(allowed(workflow.jobs['approve-and-merge'].steps[0].if, c));
});
