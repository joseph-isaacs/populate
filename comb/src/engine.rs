//! Generation engine - executes the graph to produce values.
//!
//! The engine analyzes the graph and chooses optimal generation strategies:
//! - Row-wise for dependent subgraphs (values consumed in same iteration)
//! - Columnar for independent subgraphs (can batch/parallelize)

#![allow(dead_code)]
#![allow(clippy::type_complexity)]
#![allow(clippy::collapsible_if)]
#![allow(clippy::collapsible_match)]

use std::collections::HashMap;

use rand::RngCore;

use crate::analysis::{GenStrategy, GraphAnalysis};
use crate::graph::{Graph, Node, NodeId};
use crate::value::{ErasedValue, FromErased};

/// The generation engine that executes graphs.
pub struct Engine<'a> {
    graph: &'a mut Graph,
    analysis: GraphAnalysis,
}

impl<'a> Engine<'a> {
    /// Create a new engine for a graph.
    pub fn new(graph: &'a mut Graph) -> Self {
        let analysis = GraphAnalysis::analyze(graph);
        Self { graph, analysis }
    }

    /// Generate n rows into the provided sinks.
    ///
    /// Sinks are provided as a map from NodeId to a mutable sink callback.
    /// Only output nodes will have their values pushed to sinks.
    pub fn generate<R: RngCore>(
        &mut self,
        n: usize,
        rng: &mut R,
        sinks: &mut HashMap<NodeId, Box<dyn FnMut(ErasedValue)>>,
    ) {
        // Partition nodes by strategy.
        let row_wise: Vec<NodeId> = self
            .analysis
            .topo_order
            .iter()
            .filter(|id| self.analysis.strategies.get(id) == Some(&GenStrategy::RowWise))
            .copied()
            .collect();

        let columnar: Vec<NodeId> = self
            .analysis
            .topo_order
            .iter()
            .filter(|id| self.analysis.strategies.get(id) == Some(&GenStrategy::Columnar))
            .copied()
            .collect();

        // Generate columnar nodes first (they're independent).
        for _ in 0..n {
            for &node_id in &columnar {
                if let Some(sink) = sinks.get_mut(&node_id) {
                    if let Some(node) = self.graph.get_mut(node_id) {
                        if let Node::Source { generator } = node {
                            let value = generator.gen_erased(rng);
                            sink(value);
                        }
                    }
                }
            }
        }

        // Reset generators for row-wise pass.
        for &node_id in &row_wise {
            if let Some(node) = self.graph.get_mut(node_id) {
                if let Node::Source { generator } = node {
                    generator.reset();
                }
            }
        }

        // Generate row-wise nodes.
        for _ in 0..n {
            // Values computed in this iteration, keyed by NodeId.
            let mut row_values: HashMap<NodeId, ErasedValue> = HashMap::new();

            // Process in topo order.
            for &node_id in &row_wise {
                let value = self.generate_node(node_id, rng, &row_values);

                // Push to sink if this is an output.
                if self.analysis.is_output(node_id) {
                    if let Some(sink) = sinks.get_mut(&node_id) {
                        sink(value.clone());
                    }
                }

                // Store for dependent nodes.
                if self.analysis.has_dependents(node_id) {
                    row_values.insert(node_id, value);
                }
            }
        }
    }

    /// Generate a single value for a node.
    fn generate_node<R: RngCore>(
        &mut self,
        node_id: NodeId,
        rng: &mut R,
        row_values: &HashMap<NodeId, ErasedValue>,
    ) -> ErasedValue {
        // We need to be careful here because we're borrowing self.graph mutably.
        // Clone what we need from the node first.
        let node = self.graph.get_mut(node_id).unwrap();

        match node {
            Node::Source { generator } => generator.gen_erased(rng),

            Node::Map {
                input,
                func,
                output_type: _,
            } => {
                let input_value = row_values.get(input).cloned().unwrap_or_else(|| {
                    panic!("Missing input value for node {:?}", input);
                });
                func(input_value)
            }

            Node::Map2 {
                input1,
                input2,
                func,
                output_type: _,
            } => {
                let v1 = row_values.get(input1).cloned().unwrap_or_else(|| {
                    panic!("Missing input1 value for node {:?}", input1);
                });
                let v2 = row_values.get(input2).cloned().unwrap_or_else(|| {
                    panic!("Missing input2 value for node {:?}", input2);
                });
                func(v1, v2)
            }

            Node::MapN {
                inputs,
                func,
                output_type: _,
            } => {
                let values: Vec<ErasedValue> = inputs
                    .iter()
                    .map(|id| {
                        row_values.get(id).cloned().unwrap_or_else(|| {
                            panic!("Missing input value for node {:?}", id);
                        })
                    })
                    .collect();
                func(values)
            }

            Node::Filter { input, pred } => {
                let input_value = row_values.get(input).cloned().unwrap_or_else(|| {
                    panic!("Missing input value for filter node {:?}", input);
                });
                if pred(&input_value) {
                    ErasedValue::Option(Some(Box::new(input_value)))
                } else {
                    ErasedValue::Option(None)
                }
            }

            Node::FlatMap {
                input,
                func,
                element_type: _,
            } => {
                let input_value = row_values.get(input).cloned().unwrap_or_else(|| {
                    panic!("Missing input value for flatmap node {:?}", input);
                });
                ErasedValue::List(func(input_value))
            }
        }
    }

    /// Get the analysis results.
    pub fn analysis(&self) -> &GraphAnalysis {
        &self.analysis
    }
}

/// Builder for collecting generated data into typed vectors.
pub struct Collector<'a> {
    graph: &'a mut Graph,
}

impl<'a> Collector<'a> {
    pub fn new(graph: &'a mut Graph) -> Self {
        Self { graph }
    }

    /// Collect n rows from a single output column into a Vec.
    pub fn collect_vec<S, R>(&mut self, col: crate::Col<S>, n: usize, rng: &mut R) -> Vec<S::Value>
    where
        S: crate::Schema + 'static,
        S::Value: crate::value::FromErased,
        R: RngCore,
    {
        self.graph.output_col(col);
        let mut engine = Engine::new(self.graph);

        let mut result: Vec<S::Value> = Vec::with_capacity(n);
        let mut sinks: HashMap<NodeId, Box<dyn FnMut(ErasedValue)>> = HashMap::new();

        // Create a sink that converts erased values to typed and collects them.
        let result_ptr = &mut result as *mut Vec<S::Value>;
        sinks.insert(
            col.id(),
            Box::new(move |v| {
                if let Some(typed) = S::Value::from_erased(v) {
                    // SAFETY: We know result_ptr is valid for the duration of generate().
                    unsafe { (*result_ptr).push(typed) };
                }
            }),
        );

        engine.generate(n, rng, &mut sinks);
        result
    }

    /// Collect n rows from two output columns into a tuple of Vecs.
    pub fn collect_vecs2<S1, S2, R>(
        &mut self,
        col1: crate::Col<S1>,
        col2: crate::Col<S2>,
        n: usize,
        rng: &mut R,
    ) -> (Vec<S1::Value>, Vec<S2::Value>)
    where
        S1: crate::Schema + 'static,
        S2: crate::Schema + 'static,
        S1::Value: crate::value::FromErased,
        S2::Value: crate::value::FromErased,
        R: RngCore,
    {
        self.graph.output_col(col1);
        self.graph.output_col(col2);
        let mut engine = Engine::new(self.graph);

        let mut result1: Vec<S1::Value> = Vec::with_capacity(n);
        let mut result2: Vec<S2::Value> = Vec::with_capacity(n);
        let mut sinks: HashMap<NodeId, Box<dyn FnMut(ErasedValue)>> = HashMap::new();

        let result1_ptr = &mut result1 as *mut Vec<S1::Value>;
        let result2_ptr = &mut result2 as *mut Vec<S2::Value>;

        sinks.insert(
            col1.id(),
            Box::new(move |v| {
                if let Some(typed) = S1::Value::from_erased(v) {
                    unsafe { (*result1_ptr).push(typed) };
                }
            }),
        );

        sinks.insert(
            col2.id(),
            Box::new(move |v| {
                if let Some(typed) = S2::Value::from_erased(v) {
                    unsafe { (*result2_ptr).push(typed) };
                }
            }),
        );

        engine.generate(n, rng, &mut sinks);
        (result1, result2)
    }

    /// Collect n rows from three output columns into a tuple of Vecs.
    pub fn collect_vecs3<S1, S2, S3, R>(
        &mut self,
        col1: crate::Col<S1>,
        col2: crate::Col<S2>,
        col3: crate::Col<S3>,
        n: usize,
        rng: &mut R,
    ) -> (Vec<S1::Value>, Vec<S2::Value>, Vec<S3::Value>)
    where
        S1: crate::Schema + 'static,
        S2: crate::Schema + 'static,
        S3: crate::Schema + 'static,
        S1::Value: crate::value::FromErased,
        S2::Value: crate::value::FromErased,
        S3::Value: crate::value::FromErased,
        R: RngCore,
    {
        self.graph.output_col(col1);
        self.graph.output_col(col2);
        self.graph.output_col(col3);
        let mut engine = Engine::new(self.graph);

        let mut result1: Vec<S1::Value> = Vec::with_capacity(n);
        let mut result2: Vec<S2::Value> = Vec::with_capacity(n);
        let mut result3: Vec<S3::Value> = Vec::with_capacity(n);
        let mut sinks: HashMap<NodeId, Box<dyn FnMut(ErasedValue)>> = HashMap::new();

        let result1_ptr = &mut result1 as *mut Vec<S1::Value>;
        let result2_ptr = &mut result2 as *mut Vec<S2::Value>;
        let result3_ptr = &mut result3 as *mut Vec<S3::Value>;

        sinks.insert(
            col1.id(),
            Box::new(move |v| {
                if let Some(typed) = S1::Value::from_erased(v) {
                    unsafe { (*result1_ptr).push(typed) };
                }
            }),
        );

        sinks.insert(
            col2.id(),
            Box::new(move |v| {
                if let Some(typed) = S2::Value::from_erased(v) {
                    unsafe { (*result2_ptr).push(typed) };
                }
            }),
        );

        sinks.insert(
            col3.id(),
            Box::new(move |v| {
                if let Some(typed) = S3::Value::from_erased(v) {
                    unsafe { (*result3_ptr).push(typed) };
                }
            }),
        );

        engine.generate(n, rng, &mut sinks);
        (result1, result2, result3)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Constant, Graph, Inc, SFloat64, SInt64};

    #[test]
    fn test_generate_single_source() {
        let mut graph = Graph::new();
        let a = graph.source::<SInt64, _>(Constant::new(42i64));
        graph.output_col(a);

        let mut collector = Collector::new(&mut graph);
        let mut rng = rand::rng();
        let result = collector.collect_vec(a, 5, &mut rng);

        assert_eq!(result, vec![42, 42, 42, 42, 42]);
    }

    #[test]
    fn test_generate_inc() {
        let mut graph = Graph::new();
        let a = graph.source::<SInt64, _>(Inc::from(0));
        graph.output_col(a);

        let mut collector = Collector::new(&mut graph);
        let mut rng = rand::rng();
        let result = collector.collect_vec(a, 5, &mut rng);

        assert_eq!(result, vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn test_generate_map() {
        let mut graph = Graph::new();
        let a = graph.source::<SInt64, _>(Inc::from(0));
        let b: crate::Col<SInt64> = graph.map(a, |x| x * 2);
        graph.output_col(b);

        let mut collector = Collector::new(&mut graph);
        let mut rng = rand::rng();
        let result = collector.collect_vec(b, 5, &mut rng);

        assert_eq!(result, vec![0, 2, 4, 6, 8]);
    }

    #[test]
    fn test_generate_map2() {
        let mut graph = Graph::new();
        let a = graph.source::<SInt64, _>(Inc::from(1));
        let b = graph.source::<SFloat64, _>(Constant::new(0.5f64));
        let c: crate::Col<SFloat64> = graph.map2(a, b, |x, y| x as f64 * y);
        graph.output_col(c);

        let mut collector = Collector::new(&mut graph);
        let mut rng = rand::rng();
        let result = collector.collect_vec(c, 4, &mut rng);

        assert_eq!(result, vec![0.5, 1.0, 1.5, 2.0]);
    }

    #[test]
    fn test_generate_multiple_outputs() {
        let mut graph = Graph::new();
        let a = graph.source::<SInt64, _>(Inc::from(0));
        let b: crate::Col<SInt64> = graph.map(a, |x| x * 10);

        let mut collector = Collector::new(&mut graph);
        let mut rng = rand::rng();
        let (result_a, result_b) = collector.collect_vecs2(a, b, 3, &mut rng);

        assert_eq!(result_a, vec![0, 1, 2]);
        assert_eq!(result_b, vec![0, 10, 20]);
    }

    #[test]
    fn test_generate_independent_columnar() {
        // Two independent sources should use columnar strategy.
        let mut graph = Graph::new();
        let a = graph.source::<SInt64, _>(Inc::from(0));
        let b = graph.source::<SInt64, _>(Inc::from(100));

        let analysis = GraphAnalysis::analyze(&graph);

        // Both should be columnar (independent).
        assert_eq!(
            analysis.strategies.get(&a.id()),
            Some(&GenStrategy::Columnar)
        );
        assert_eq!(
            analysis.strategies.get(&b.id()),
            Some(&GenStrategy::Columnar)
        );

        // Different subgraphs.
        assert_ne!(
            analysis.nodes[&a.id()].subgraph,
            analysis.nodes[&b.id()].subgraph
        );
    }
}
