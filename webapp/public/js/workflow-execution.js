import {
  assertExecutionOutcome,
  blockedOutcome,
  failedOutcome,
} from './execution-outcome.js';

const BLOCKING_STATUSES = new Set(['blocked', 'failed', 'cancelled', 'stale']);

function scopedEdges(nodeSet, connections) {
  return (connections || []).filter(edge =>
    nodeSet.has(edge?.fromNode) && nodeSet.has(edge?.toNode));
}

export function connectedComponentNodeIds(seedNodeId, connections) {
  if (typeof seedNodeId !== 'string' || !seedNodeId) return [];
  const adjacency = new Map();
  const add = (from, to) => {
    if (!adjacency.has(from)) adjacency.set(from, new Set());
    adjacency.get(from).add(to);
  };
  for (const edge of connections || []) {
    if (!edge?.fromNode || !edge?.toNode) continue;
    add(edge.fromNode, edge.toNode);
    add(edge.toNode, edge.fromNode);
  }
  const visited = new Set([seedNodeId]);
  const queue = [seedNodeId];
  while (queue.length) {
    const current = queue.shift();
    for (const neighbour of adjacency.get(current) || []) {
      if (visited.has(neighbour)) continue;
      visited.add(neighbour);
      queue.push(neighbour);
    }
  }
  return [...visited].sort();
}

export function allConnectedNodeIds(connections) {
  const ids = new Set();
  for (const edge of connections || []) {
    if (edge?.fromNode) ids.add(edge.fromNode);
    if (edge?.toNode) ids.add(edge.toNode);
  }
  return [...ids].sort();
}

export function planWorkflowExecution({ nodeIds, connections, registry }) {
  const uniqueNodeIds = [...new Set(nodeIds || [])].filter(nodeId => registry?.[nodeId]);
  const nodeSet = new Set(uniqueNodeIds);
  const edges = scopedEdges(nodeSet, connections);
  const indegree = new Map(uniqueNodeIds.map(nodeId => [nodeId, 0]));
  const outgoing = new Map(uniqueNodeIds.map(nodeId => [nodeId, []]));

  for (const edge of edges) {
    indegree.set(edge.toNode, indegree.get(edge.toNode) + 1);
    outgoing.get(edge.fromNode).push(edge.toNode);
  }

  const queue = uniqueNodeIds.filter(nodeId => indegree.get(nodeId) === 0).sort();
  const order = [];
  while (queue.length) {
    const nodeId = queue.shift();
    order.push(nodeId);
    for (const downstreamId of outgoing.get(nodeId).sort()) {
      indegree.set(downstreamId, indegree.get(downstreamId) - 1);
      if (indegree.get(downstreamId) === 0) {
        queue.push(downstreamId);
        queue.sort();
      }
    }
  }

  if (order.length !== uniqueNodeIds.length) {
    const cycleNodes = uniqueNodeIds.filter(nodeId => !order.includes(nodeId)).sort();
    return Object.freeze({
      ok: false,
      code: 'dependency_cycle',
      message: `Workflow contains a dependency cycle: ${cycleNodes.join(', ')}`,
      nodeIds: Object.freeze([...uniqueNodeIds]),
      edges: Object.freeze(edges.map(edge => Object.freeze({ ...edge }))),
      order: Object.freeze([]),
      cycleNodes: Object.freeze(cycleNodes),
    });
  }

  return Object.freeze({
    ok: true,
    code: 'ready',
    message: 'Workflow execution plan is acyclic',
    nodeIds: Object.freeze([...uniqueNodeIds]),
    edges: Object.freeze(edges.map(edge => Object.freeze({ ...edge }))),
    order: Object.freeze(order),
    cycleNodes: Object.freeze([]),
  });
}

function summarize(outcomes) {
  const summary = {
    executed: 0,
    reused: 0,
    blocked: 0,
    failed: 0,
    cancelled: 0,
    stale: 0,
  };
  for (const outcome of Object.values(outcomes)) {
    if (outcome.status === 'succeeded') {
      if (outcome.work === 'executed') summary.executed += 1;
      if (outcome.work === 'reused') summary.reused += 1;
    } else {
      summary[outcome.status] += 1;
    }
  }
  return Object.freeze(summary);
}

function reportStatus(summary) {
  if (summary.failed > 0) return 'failed';
  if (summary.blocked > 0) return 'blocked';
  if (summary.cancelled > 0) return 'cancelled';
  if (summary.stale > 0) return 'stale';
  return 'succeeded';
}

function blockingDependency(nodeId, plan, outcomes) {
  for (const edge of plan.edges) {
    if (edge.toNode !== nodeId) continue;
    const upstream = outcomes[edge.fromNode];
    if (!upstream) continue;
    if (BLOCKING_STATUSES.has(upstream.status)) {
      return { edge, upstream, reason: 'upstream_outcome' };
    }
    const availability = edge.fromPort ? upstream.outputs?.[edge.fromPort] : null;
    if (availability === 'missing' || availability === 'historical') {
      return { edge, upstream, reason: `upstream_output_${availability}` };
    }
  }
  return null;
}

function finalizeReport(plan, outcomes) {
  const summary = summarize(outcomes);
  return Object.freeze({
    status: reportStatus(summary),
    code: plan.code,
    message: plan.message,
    plan,
    outcomes: Object.freeze({ ...outcomes }),
    summary,
  });
}

export async function runWorkflowPlan(plan, { registry, nodeTypes }) {
  if (!plan?.ok) {
    const emptySummary = summarize({});
    return Object.freeze({
      status: 'blocked',
      code: plan?.code || 'invalid_plan',
      message: plan?.message || 'Workflow plan is invalid',
      plan,
      outcomes: Object.freeze({}),
      summary: emptySummary,
    });
  }

  const outcomes = {};
  // Deliberately serial. A future parallel scheduler must be a separate,
  // explicitly reviewed execution policy.
  for (const nodeId of plan.order) {
    const info = registry?.[nodeId];
    const definition = nodeTypes?.[info?.type];
    const descriptor = definition?.execution;

    const dependency = blockingDependency(nodeId, plan, outcomes);
    if (dependency) {
      outcomes[nodeId] = blockedOutcome(nodeId, {
        code: dependency.reason,
        message: `Skipped because upstream node ${dependency.edge.fromNode} is not current and usable`,
        details: { upstreamNodeId: dependency.edge.fromNode },
      });
      continue;
    }

    const mode = descriptor?.mode || 'none';
    const operation = mode === 'prepare' ? definition?.prepare : definition?.execute;
    if (mode === 'none' || mode === 'restore-only') continue;
    if (typeof operation !== 'function') {
      outcomes[nodeId] = failedOutcome(nodeId, {
        code: 'missing_execution_operation',
        message: `${info?.type || 'Unknown node'} declares ${mode} without an operation`,
      });
      continue;
    }

    try {
      const rawOutcome = await operation(nodeId, {
        triggerDownstream: false,
        throwOnFailure: true,
        workflowExecution: true,
      });
      outcomes[nodeId] = assertExecutionOutcome(rawOutcome, {
        nodeId,
        nodeType: info?.type,
      });
    } catch (error) {
      outcomes[nodeId] = failedOutcome(nodeId, {
        code: error?.code === 'execution_contract_violation'
          ? 'execution_contract_violation'
          : 'execution_failed',
        message: error?.message || String(error),
        details: { name: error?.name || 'Error' },
      });
    }
  }

  return finalizeReport(plan, outcomes);
}
