//! Dependency analysis for the generation graph.
//!
//! This module analyzes the graph to determine:
//! - Which nodes have dependents (are consumed by other nodes)
//! - Which nodes are outputs (leaves)
//! - Which subgraphs are independent and can be parallelized
//! - Optimal generation strategy (row-wise vs columnar)

use std::collections::{HashMap, HashSet};

use crate::graph::{Graph, NodeId};

/// Classification of a node based on its position in the graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeClass {
    /// Root node with no dependencies (Source).
    Root,
    /// Interior node with both dependencies and dependents.
    Interior,
    /// Leaf node that is an output with no dependents.
    Leaf,
}

/// Analysis result for a single node.
#[derive(Debug, Clone)]
pub struct NodeInfo {
    /// The node's classification.
    pub class: NodeClass,
    /// Direct dependencies (nodes this node reads from).
    pub deps: Vec<NodeId>,
    /// Direct dependents (nodes that read from this node).
    pub dependents: Vec<NodeId>,
    /// Whether this node is designated as an output.
    pub is_output: bool,
    /// The subgraph ID this node belongs to.
    pub subgraph: usize,
}

/// Strategy for generating a node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GenStrategy {
    /// Generate values one at a time in a row-wise pass.
    /// Required when values are consumed by dependent nodes in the same iteration.
    RowWise,
    /// Generate all values at once (batch/columnar).
    /// Possible when node has no dependents or is in an independent subgraph.
    Columnar,
}

/// Result of analyzing a graph.
#[derive(Debug)]
pub struct GraphAnalysis {
    /// Per-node analysis info.
    pub nodes: HashMap<NodeId, NodeInfo>,
    /// Nodes in topological order (dependencies before dependents).
    pub topo_order: Vec<NodeId>,
    /// Subgraph membership: subgraph_id -> set of node IDs.
    pub subgraphs: Vec<HashSet<NodeId>>,
    /// Generation strategy for each node.
    pub strategies: HashMap<NodeId, GenStrategy>,
}

impl GraphAnalysis {
    /// Analyze a graph and determine optimal generation strategies.
    pub fn analyze(graph: &Graph) -> Self {
        let mut nodes = HashMap::new();
        let outputs: HashSet<NodeId> = graph.outputs().iter().copied().collect();

        // Build adjacency lists (deps and reverse/dependents).
        let mut dependents_map: HashMap<NodeId, Vec<NodeId>> = HashMap::new();

        for (id, node) in graph.iter() {
            let deps = node.dependencies();

            // Register this node as a dependent of each of its deps
            for &dep in &deps {
                dependents_map.entry(dep).or_default().push(id);
            }

            nodes.insert(
                id,
                NodeInfo {
                    class: NodeClass::Root, // Will be updated
                    deps,
                    dependents: Vec::new(), // Will be filled
                    is_output: outputs.contains(&id),
                    subgraph: 0, // Will be assigned
                },
            );
        }

        // Fill in dependents and classify nodes.
        for (id, info) in nodes.iter_mut() {
            info.dependents = dependents_map.get(id).cloned().unwrap_or_default();

            info.class = match (info.deps.is_empty(), info.dependents.is_empty()) {
                (true, _) => NodeClass::Root,
                (false, true) => NodeClass::Leaf,
                (false, false) => NodeClass::Interior,
            };
        }

        // Topological sort using Kahn's algorithm.
        let topo_order = Self::topo_sort(&nodes);

        // Identify connected subgraphs using union-find.
        let subgraphs = Self::find_subgraphs(&nodes, graph.len());

        // Assign subgraph IDs to nodes.
        for (subgraph_id, members) in subgraphs.iter().enumerate() {
            for &node_id in members {
                if let Some(info) = nodes.get_mut(&node_id) {
                    info.subgraph = subgraph_id;
                }
            }
        }

        // Determine generation strategies.
        let strategies = Self::determine_strategies(&nodes, &subgraphs);

        GraphAnalysis {
            nodes,
            topo_order,
            subgraphs,
            strategies,
        }
    }

    /// Topological sort using Kahn's algorithm.
    fn topo_sort(nodes: &HashMap<NodeId, NodeInfo>) -> Vec<NodeId> {
        let mut in_degree: HashMap<NodeId, usize> = HashMap::new();
        let mut queue: Vec<NodeId> = Vec::new();
        let mut result = Vec::new();

        // Calculate in-degrees (number of dependencies).
        for (&id, info) in nodes {
            in_degree.insert(id, info.deps.len());
            if info.deps.is_empty() {
                queue.push(id);
            }
        }

        // Process nodes with no remaining dependencies.
        while let Some(id) = queue.pop() {
            result.push(id);

            if let Some(info) = nodes.get(&id) {
                for &dependent in &info.dependents {
                    if let Some(degree) = in_degree.get_mut(&dependent) {
                        *degree -= 1;
                        if *degree == 0 {
                            queue.push(dependent);
                        }
                    }
                }
            }
        }

        result
    }

    /// Find connected subgraphs using union-find.
    fn find_subgraphs(nodes: &HashMap<NodeId, NodeInfo>, num_nodes: usize) -> Vec<HashSet<NodeId>> {
        // Union-find data structure.
        let mut parent: Vec<usize> = (0..num_nodes).collect();

        fn find(parent: &mut [usize], x: usize) -> usize {
            if parent[x] != x {
                parent[x] = find(parent, parent[x]);
            }
            parent[x]
        }

        fn union(parent: &mut [usize], x: usize, y: usize) {
            let px = find(parent, x);
            let py = find(parent, y);
            if px != py {
                parent[px] = py;
            }
        }

        // Union nodes that share edges.
        for (&id, info) in nodes {
            for &dep in &info.deps {
                union(&mut parent, id.index(), dep.index());
            }
        }

        // Group nodes by their root.
        let mut groups: HashMap<usize, HashSet<NodeId>> = HashMap::new();
        for &id in nodes.keys() {
            let root = find(&mut parent, id.index());
            groups.entry(root).or_default().insert(id);
        }

        groups.into_values().collect()
    }

    /// Determine generation strategy for each node.
    ///
    /// Rules:
    /// 1. If a node has dependents (interior node), it must be row-wise
    ///    because its value is consumed in the same iteration.
    /// 2. If a node feeds an interior node, it must also be row-wise.
    /// 3. Independent leaf-only chains can be columnar.
    fn determine_strategies(
        nodes: &HashMap<NodeId, NodeInfo>,
        subgraphs: &[HashSet<NodeId>],
    ) -> HashMap<NodeId, GenStrategy> {
        let mut strategies = HashMap::new();

        // For each subgraph, determine if it has any dependencies.
        // A subgraph needs row-wise if any node in it has dependencies,
        // because those dependencies need to be computed in the same iteration.
        let mut subgraph_needs_rowwise: Vec<bool> = vec![false; subgraphs.len()];
        for info in nodes.values() {
            // If this node has any dependencies, the subgraph needs row-wise.
            if !info.deps.is_empty() {
                subgraph_needs_rowwise[info.subgraph] = true;
            }
        }

        // Assign strategies based on subgraph analysis.
        for (id, info) in nodes {
            let strategy = if subgraph_needs_rowwise[info.subgraph] {
                // Subgraph has dependencies that require row-wise generation.
                GenStrategy::RowWise
            } else {
                // Independent subgraph with no interior dependencies.
                GenStrategy::Columnar
            };
            strategies.insert(*id, strategy);
        }

        strategies
    }

    /// Get nodes that can be generated in parallel (different subgraphs).
    pub fn parallel_groups(&self) -> Vec<Vec<NodeId>> {
        self.subgraphs
            .iter()
            .map(|sg| sg.iter().copied().collect())
            .collect()
    }

    /// Get nodes that must be generated row-wise.
    pub fn row_wise_nodes(&self) -> Vec<NodeId> {
        self.strategies
            .iter()
            .filter(|(_, s)| **s == GenStrategy::RowWise)
            .map(|(id, _)| *id)
            .collect()
    }

    /// Get nodes that can be generated columnar.
    pub fn columnar_nodes(&self) -> Vec<NodeId> {
        self.strategies
            .iter()
            .filter(|(_, s)| **s == GenStrategy::Columnar)
            .map(|(id, _)| *id)
            .collect()
    }

    /// Check if a node has any dependents.
    pub fn has_dependents(&self, id: NodeId) -> bool {
        self.nodes
            .get(&id)
            .is_some_and(|info| !info.dependents.is_empty())
    }

    /// Check if a node is an output.
    pub fn is_output(&self, id: NodeId) -> bool {
        self.nodes.get(&id).is_some_and(|info| info.is_output)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Constant, Graph, SFloat64, SInt64};

    #[test]
    fn test_independent_sources() {
        // Two independent sources - should be columnar and parallelizable.
        let mut graph = Graph::new();
        let a = graph.source::<SInt64, _>(Constant::new(1i64));
        let b = graph.source::<SInt64, _>(Constant::new(2i64));
        graph.output_col(a);
        graph.output_col(b);

        let analysis = GraphAnalysis::analyze(&graph);

        // Both should be roots and outputs.
        assert_eq!(analysis.nodes[&a.id()].class, NodeClass::Root);
        assert_eq!(analysis.nodes[&b.id()].class, NodeClass::Root);
        assert!(analysis.nodes[&a.id()].is_output);
        assert!(analysis.nodes[&b.id()].is_output);

        // Both should be columnar (independent, no dependents).
        assert_eq!(analysis.strategies[&a.id()], GenStrategy::Columnar);
        assert_eq!(analysis.strategies[&b.id()], GenStrategy::Columnar);

        // Should be in separate subgraphs (can parallelize).
        assert_ne!(
            analysis.nodes[&a.id()].subgraph,
            analysis.nodes[&b.id()].subgraph
        );
    }

    #[test]
    fn test_dependent_chain() {
        // a -> b (map) -> c (map)
        // b is interior, so chain must be row-wise.
        let mut graph = Graph::new();
        let a = graph.source::<SInt64, _>(Constant::new(1i64));
        let b = graph.map::<SInt64, SInt64, _>(a, |x| x * 2);
        let c = graph.map::<SInt64, SInt64, _>(b, |x| x + 1);
        graph.output_col(c);

        let analysis = GraphAnalysis::analyze(&graph);

        // Classifications.
        assert_eq!(analysis.nodes[&a.id()].class, NodeClass::Root);
        assert_eq!(analysis.nodes[&b.id()].class, NodeClass::Interior);
        assert_eq!(analysis.nodes[&c.id()].class, NodeClass::Leaf);

        // All in same subgraph.
        assert_eq!(
            analysis.nodes[&a.id()].subgraph,
            analysis.nodes[&b.id()].subgraph
        );
        assert_eq!(
            analysis.nodes[&b.id()].subgraph,
            analysis.nodes[&c.id()].subgraph
        );

        // All must be row-wise due to interior node.
        assert_eq!(analysis.strategies[&a.id()], GenStrategy::RowWise);
        assert_eq!(analysis.strategies[&b.id()], GenStrategy::RowWise);
        assert_eq!(analysis.strategies[&c.id()], GenStrategy::RowWise);

        // Topo order: a before b before c.
        let topo = &analysis.topo_order;
        let pos_a = topo.iter().position(|&x| x == a.id()).unwrap();
        let pos_b = topo.iter().position(|&x| x == b.id()).unwrap();
        let pos_c = topo.iter().position(|&x| x == c.id()).unwrap();
        assert!(pos_a < pos_b);
        assert!(pos_b < pos_c);
    }

    #[test]
    fn test_mixed_graph() {
        // a, b -> c (map2), d independent
        // Subgraph {a, b, c} is row-wise, {d} is columnar.
        let mut graph = Graph::new();
        let a = graph.source::<SInt64, _>(Constant::new(1i64));
        let b = graph.source::<SFloat64, _>(Constant::new(2.0f64));
        let c = graph.map2::<SInt64, SFloat64, SFloat64, _>(a, b, |x, y| x as f64 + y);
        let d = graph.source::<SInt64, _>(Constant::new(3i64));
        graph.output_col(c);
        graph.output_col(d);

        let analysis = GraphAnalysis::analyze(&graph);

        // a and b are roots with dependents, c is leaf.
        assert_eq!(analysis.nodes[&a.id()].class, NodeClass::Root);
        assert_eq!(analysis.nodes[&b.id()].class, NodeClass::Root);
        assert_eq!(analysis.nodes[&c.id()].class, NodeClass::Leaf);
        assert_eq!(analysis.nodes[&d.id()].class, NodeClass::Root);

        // {a, b, c} in one subgraph, {d} in another.
        assert_eq!(
            analysis.nodes[&a.id()].subgraph,
            analysis.nodes[&c.id()].subgraph
        );
        assert_ne!(
            analysis.nodes[&a.id()].subgraph,
            analysis.nodes[&d.id()].subgraph
        );

        // {a, b, c} row-wise (has interior deps), {d} columnar.
        assert_eq!(analysis.strategies[&a.id()], GenStrategy::RowWise);
        assert_eq!(analysis.strategies[&b.id()], GenStrategy::RowWise);
        assert_eq!(analysis.strategies[&c.id()], GenStrategy::RowWise);
        assert_eq!(analysis.strategies[&d.id()], GenStrategy::Columnar);
    }
}
