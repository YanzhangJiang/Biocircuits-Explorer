import assert from 'node:assert/strict';
import {
  architectureDemoCard,
  architectureDemoOptions,
  architectureDemoRequest,
} from '../public/js/architecture-discovery-demo.js';

const options = architectureDemoOptions();
assert.deepEqual(options.map(option => option.id), [
  'branched', 'symmetric-ambiguous', 'symmetric-resolved',
]);

const request = architectureDemoRequest('branched', 0.1);
assert.equal(request.reactions.length, 5);
assert.equal(request.samples.length, 64);
assert.equal(request.simulation_noise, 0.1);
assert.equal('initial_kd' in request, false);
assert.ok(request.samples.every(sample => !('target' in sample)));

const exactResponse = {
  rules: [request.reactions[0], request.reactions[1], request.reactions[3]],
  kd: [0.8, 0.35, 1.7],
  fit_loss: 1e-7,
  reaction_fit: request.reactions.map((rule, index) => ({
    rule,
    active: [0, 1, 3].includes(index),
    kd: request.simulation_kd[index],
  })),
};
const exactCard = architectureDemoCard('branched', exactResponse);
assert.equal(exactCard.exact_support_recovery, true);
assert.equal(exactCard.verdict, 'exact recovery');
assert.equal(exactCard.candidate_rules.length, 5);

const ambiguousRequest = architectureDemoRequest('symmetric-ambiguous');
const ambiguousCard = architectureDemoCard('symmetric-ambiguous', {
  rules: [ambiguousRequest.reactions[1]],
  kd: [1],
  fit_loss: 4.7e-4,
  reaction_fit: ambiguousRequest.reactions.map((rule, index) => ({
    rule,
    active: index === 1,
    kd: index === 1 ? 1 : 1e6,
  })),
});
assert.equal(ambiguousCard.exact_support_recovery, false);
assert.equal(ambiguousCard.identifiable, false);
assert.match(ambiguousCard.evidence_tier, /either branch/);

console.log('architecture discovery demo: 12/12 passed');
