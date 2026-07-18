export const meta = {
  name: 'gate-b-evidence-refute',
  description: 'Adversarially refute Gate B WAP-firing evidence claims (ADR-0006 criterion-2 candidate run)',
  phases: [
    { title: 'Refute', detail: 'one skeptic per load-bearing Gate B claim, tier-pinned from config' },
  ],
}

// ADR-0006 criterion 2: tiers come from rigor config via args (never hardcoded);
// every worker is pinned via an expression and must return a model receipt.
// args may arrive JSON-encoded depending on the caller -- tolerate both.
const A = typeof args === 'string' ? JSON.parse(args) : args
const tiers = A?.tiers
if (!tiers || !tiers.judgment || !tiers.build || !tiers.cheap) {
  throw new Error('args.tiers must carry {judgment, build, cheap} from rigor config/models.json')
}
const claims = A.claims // [{node, label, tier, claim, recompute}]

const RECEIPT_SCHEMA = {
  type: 'object',
  required: ['refuted', 'evidence', 'model_self_report'],
  properties: {
    refuted: { type: 'boolean', description: 'true if the claim did NOT survive your refutation attempt' },
    evidence: { type: 'string', description: 'what you recomputed/observed from RAW artifacts, with commands and outputs' },
    caveats: { type: 'string', description: 'anything you could not check' },
    model_self_report: {
      type: 'string',
      description: 'REQUIRED RECEIPT: the model you, the worker, are actually running as -- your own model name/id as precisely as you know it. Do not name the model that dispatched you.',
    },
  },
}

phase('Refute')
const verdicts = await parallel(claims.map((c) => () =>
  agent(
    `You are an adversarial skeptic (rigor: refute before trust). Try to BREAK this claim by recomputing from raw artifacts -- never accept the claim's own summary as evidence. Default to refuted=true if you cannot verify.\n\nCLAIM: ${c.claim}\n\nRecompute instructions (run these yourself, compare outputs):\n${c.recompute}\n\nReturn refuted=false ONLY if your own recomputation reproduces the claim. Fill model_self_report with YOUR model identity.`,
    { label: c.label, phase: 'Refute', model: tiers[c.tier], schema: RECEIPT_SCHEMA },
  ).then((v) => ({ node: c.node, label: c.label, requested: tiers[c.tier], ...(v ?? { refuted: null }) }))
))

return { verdicts: verdicts.filter(Boolean) }
