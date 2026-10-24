import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import test from 'node:test';
import {parse} from 'yaml';
import {assertFrozenWorkflow} from '../../.clouseau-ci/test/automatic-upgrade-contract.mjs';
import {evaluate, allowed} from './workflow-expressions.mjs';
const workflow = parse(readFileSync(new URL('../workflows/backend-ci.yml', import.meta.url), 'utf8'));

test('actual consumer job expressions freeze one authorized SHA and digest across a central advance and use newer main next run', () => {
  assertFrozenWorkflow(workflow, evaluate);
  const trust = JSON.parse(readFileSync(new URL('../clouseau.json', import.meta.url))).local_review;
  assert.deepEqual(trust.central_source, {repository: 'gaulatti/clouseau', ref: 'refs/heads/main'});
  assert.equal(trust.reviewer_digest, undefined);
  assert.ok(trust.keys.length > 0);
});

test('resolution failure cannot turn required PR approval into a skipped passing check', () => {
  const gate = workflow.jobs['approve-and-merge'];
  assert.equal(gate.if, 'always()');
  for (const result of ['failure', 'cancelled', 'skipped']) {
    const context = {github: {event_name: 'pull_request_target', event: {pull_request: {draft: false}}},
      needs: {'assess-intent': {result}, 'resolve-clouseau': {result, outputs: {}}}};
    assert.equal(evaluate(gate.name, context).coerceString(), 'approve-and-merge');
    assert.equal(allowed(gate.steps[0].if, context), true);
    assert.equal(gate.steps[0].run, 'exit 1');
    assert.equal(allowed(gate.steps.at(-1).if, context, {success: false}), false);
  }
});
