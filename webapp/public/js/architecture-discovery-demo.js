const INACTIVE_KD = 1e6;

function powers(values) {
  return values.map(value => 10 ** value);
}

function threeInputSamples() {
  const levels = powers([-0.75, -0.25, 0.25, 0.75]);
  const samples = [];
  for (const a of levels) for (const b of levels) for (const c of levels) {
    samples.push({ totals: { tA: a, tB: b, tC: c } });
  }
  return samples;
}

function symmetricSamples() {
  return Array.from({ length: 16 }, (_, index) => ({
    totals: { tA: 10 ** (-1.5 + 3 * index / 15), tB: 1, tC: 1 },
  }));
}

const SYMMETRIC_REACTIONS = ['A + B <-> AB', 'A + C <-> AC'];
const SYMMETRIC_KD = [1, 1e12];
const SYMMETRIC_ACTIVE = [true, false];

const PRESETS = {
  branched: {
    label: 'Branched pathway',
    note: 'Three true reactions are hidden inside a five-reaction candidate library.',
    reactions: [
      'A + B <-> AB',
      'AB + B <-> AB2',
      'AB2 + B <-> AB3',
      'A + C <-> AC',
      'AC + C <-> AC2',
    ],
    outputExprs: ['AB + 2*AB2 + 3*AB3', 'AC + 2*AC2'],
    truthKd: [0.8, 0.35, INACTIVE_KD, 1.7, INACTIVE_KD],
    truthActive: [true, true, false, true, false],
    identifiable: true,
    samples: threeInputSamples,
  },
  'symmetric-ambiguous': {
    label: 'Symmetric branches · summed readout',
    note: 'With equal B/C totals, AB + AC cannot tell which symmetric branch is active.',
    reactions: SYMMETRIC_REACTIONS,
    outputExprs: ['AB + AC'],
    truthKd: SYMMETRIC_KD,
    truthActive: SYMMETRIC_ACTIVE,
    identifiable: false,
    samples: symmetricSamples,
  },
  'symmetric-resolved': {
    label: 'Symmetric branches · separate readouts',
    note: 'Observing AB and AC separately breaks the symmetry and identifies the active branch.',
    reactions: SYMMETRIC_REACTIONS,
    outputExprs: ['AB', 'AC'],
    truthKd: SYMMETRIC_KD,
    truthActive: SYMMETRIC_ACTIVE,
    identifiable: true,
    samples: symmetricSamples,
  },
};

function preset(id) {
  const value = PRESETS[id];
  if (!value) throw new Error(`Unknown architecture demo: ${id}`);
  return value;
}

export function architectureDemoOptions() {
  return Object.entries(PRESETS).map(([id, value]) => ({ id, label: value.label }));
}

export function architectureDemoRequest(id, noise = 0) {
  const value = preset(id);
  const noiseValue = Number(noise);
  if (!Number.isFinite(noiseValue) || noiseValue < 0) {
    throw new Error('Simulation noise must be a non-negative number.');
  }
  return {
    reactions: [...value.reactions],
    output_exprs: [...value.outputExprs],
    simulation_kd: [...value.truthKd],
    simulation_noise: noiseValue,
    samples: value.samples(),
  };
}

export function architectureDemoCard(id, response) {
  const value = preset(id);
  const reactionFit = Array.isArray(response?.reaction_fit) ? response.reaction_fit : [];
  const learnedActive = reactionFit.map(fit => fit.active === true);
  const exact = learnedActive.length === value.truthActive.length &&
    learnedActive.every((active, index) => active === value.truthActive[index]);
  const rules = Array.isArray(response?.rules) ? response.rules : [];
  const kd = Array.isArray(response?.kd) ? response.kd : [];
  return {
    family: 'architecture_discovery',
    verdict: value.identifiable === false
      ? 'compatible, non-unique fit'
      : (exact ? 'exact recovery' : 'support mismatch'),
    demo_label: value.label,
    demo_note: value.note,
    n_reactions: rules.length,
    candidate_count: value.reactions.length,
    output_symbol: value.outputExprs.length === 1
      ? value.outputExprs[0]
      : `${value.outputExprs.length} readouts`,
    output_exprs: [...value.outputExprs],
    rules,
    kd,
    fit_loss: response?.fit_loss,
    reaction_fit: reactionFit,
    candidate_rules: [...value.reactions],
    truth_active: [...value.truthActive],
    truth_kd: [...value.truthKd],
    exact_support_recovery: exact,
    identifiable: value.identifiable,
    evidence_tier: value.identifiable === false
      ? 'the observations are symmetric; either branch is compatible'
      : exact
        ? 'hidden support recovered from simulated observations'
        : 'the learned support differs from the hidden simulation',
  };
}
