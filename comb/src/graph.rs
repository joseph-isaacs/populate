//! Graph model - explicit representation of the generation DAG.
//!
//! The graph stores nodes in an arena and uses NodeIds as handles.
//! This enables dependency analysis and optimization strategies.

use std::marker::PhantomData;

use crate::Schema;
use crate::generator::BoxedGen;
use crate::value::{ErasedValue, FromErased, IntoErased, TypeTag};

/// Unique identifier for a node in the graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NodeId(pub(crate) usize);

impl NodeId {
    pub fn index(self) -> usize {
        self.0
    }
}

/// A typed column handle - lightweight reference into the graph.
///
/// `Col<S>` is Copy and just points to a node in the arena.
/// The phantom type ensures type safety at compile time.
#[derive(Debug)]
pub struct Col<S: Schema> {
    pub(crate) id: NodeId,
    _marker: PhantomData<S>,
}

impl<S: Schema> Col<S> {
    pub(crate) fn new(id: NodeId) -> Self {
        Self {
            id,
            _marker: PhantomData,
        }
    }

    /// Get the underlying node ID.
    pub fn id(&self) -> NodeId {
        self.id
    }

    /// Erase the type, returning just the NodeId and TypeTag.
    pub fn erase(&self) -> (NodeId, TypeTag) {
        (self.id, S::type_tag())
    }
}

impl<S: Schema> Clone for Col<S> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<S: Schema> Copy for Col<S> {}

/// Function type for Map operations.
pub type MapFn = Box<dyn Fn(ErasedValue) -> ErasedValue + Send + Sync>;

/// Function type for Map2 operations.
pub type Map2Fn = Box<dyn Fn(ErasedValue, ErasedValue) -> ErasedValue + Send + Sync>;

/// Function type for MapN operations.
pub type MapNFn = Box<dyn Fn(Vec<ErasedValue>) -> ErasedValue + Send + Sync>;

/// Function type for Filter predicates.
pub type FilterPred = Box<dyn Fn(&ErasedValue) -> bool + Send + Sync>;

/// Function type for FlatMap operations (produces a list).
pub type FlatMapFn = Box<dyn Fn(ErasedValue) -> Vec<ErasedValue> + Send + Sync>;

/// A node in the generation graph.
pub enum Node {
    /// Source node - root generator with no dependencies.
    Source { generator: BoxedGen },

    /// Map node - transforms one input.
    Map {
        input: NodeId,
        func: MapFn,
        output_type: TypeTag,
    },

    /// Map2 node - combines two inputs.
    Map2 {
        input1: NodeId,
        input2: NodeId,
        func: Map2Fn,
        output_type: TypeTag,
    },

    /// MapN node - combines N inputs.
    MapN {
        inputs: Vec<NodeId>,
        func: MapNFn,
        output_type: TypeTag,
    },

    /// Filter node - conditional generation (produces Option).
    Filter { input: NodeId, pred: FilterPred },

    /// FlatMap node - expands input to list.
    FlatMap {
        input: NodeId,
        func: FlatMapFn,
        element_type: TypeTag,
    },
}

impl Node {
    /// Get the type tag for values produced by this node.
    pub fn type_tag(&self) -> TypeTag {
        match self {
            Node::Source { generator } => generator.type_tag(),
            Node::Map { output_type, .. } => output_type.clone(),
            Node::Map2 { output_type, .. } => output_type.clone(),
            Node::MapN { output_type, .. } => output_type.clone(),
            Node::Filter { input: _, pred: _ } => {
                // Filter produces Option of input type
                // We'd need to look up the input type, so we'll handle this in Graph
                TypeTag::Bool // Placeholder - real impl uses graph lookup
            }
            Node::FlatMap { element_type, .. } => TypeTag::List(Box::new(element_type.clone())),
        }
    }

    /// Get the direct dependencies of this node.
    pub fn dependencies(&self) -> Vec<NodeId> {
        match self {
            Node::Source { .. } => vec![],
            Node::Map { input, .. } => vec![*input],
            Node::Map2 { input1, input2, .. } => vec![*input1, *input2],
            Node::MapN { inputs, .. } => inputs.clone(),
            Node::Filter { input, .. } => vec![*input],
            Node::FlatMap { input, .. } => vec![*input],
        }
    }
}

/// The generation graph - holds all nodes in an arena.
pub struct Graph {
    /// Arena of all nodes.
    nodes: Vec<Node>,

    /// Which nodes are designated as outputs.
    outputs: Vec<NodeId>,
}

impl Graph {
    /// Create a new empty graph.
    pub fn new() -> Self {
        Self {
            nodes: Vec::new(),
            outputs: Vec::new(),
        }
    }

    /// Add a source node with a typed generator.
    pub fn source<S, G>(&mut self, generator: G) -> Col<S>
    where
        S: Schema + Send + Sync + 'static,
        G: crate::Gen<S> + Clone + Send + Sync + 'static,
        S::Value: IntoErased,
    {
        let id = NodeId(self.nodes.len());
        self.nodes.push(Node::Source {
            generator: BoxedGen::new::<S, G>(generator),
        });
        Col::new(id)
    }

    /// Add a map node that transforms one column.
    pub fn map<A, B, F>(&mut self, input: Col<A>, f: F) -> Col<B>
    where
        A: Schema + 'static,
        B: Schema + 'static,
        A::Value: crate::value::IntoErased + crate::value::FromErased,
        B::Value: crate::value::IntoErased,
        F: Fn(A::Value) -> B::Value + Send + Sync + 'static,
    {
        let id = NodeId(self.nodes.len());
        let func: MapFn = Box::new(move |v| {
            let typed = A::Value::from_erased(v).expect("type mismatch in map");
            f(typed).into_erased()
        });
        self.nodes.push(Node::Map {
            input: input.id,
            func,
            output_type: B::type_tag(),
        });
        Col::new(id)
    }

    /// Add a map2 node that combines two columns.
    pub fn map2<A, B, C, F>(&mut self, input1: Col<A>, input2: Col<B>, f: F) -> Col<C>
    where
        A: Schema + 'static,
        B: Schema + 'static,
        C: Schema + 'static,
        A::Value: crate::value::IntoErased + crate::value::FromErased,
        B::Value: crate::value::IntoErased + crate::value::FromErased,
        C::Value: crate::value::IntoErased,
        F: Fn(A::Value, B::Value) -> C::Value + Send + Sync + 'static,
    {
        let id = NodeId(self.nodes.len());
        let func: Map2Fn = Box::new(move |v1, v2| {
            let a = A::Value::from_erased(v1).expect("type mismatch in map2 arg1");
            let b = B::Value::from_erased(v2).expect("type mismatch in map2 arg2");
            f(a, b).into_erased()
        });
        self.nodes.push(Node::Map2 {
            input1: input1.id,
            input2: input2.id,
            func,
            output_type: C::type_tag(),
        });
        Col::new(id)
    }

    /// Add a zip node that combines two columns into a tuple.
    pub fn zip<A, B>(&mut self, col_a: Col<A>, col_b: Col<B>) -> Col<crate::STuple2<A, B>>
    where
        A: Schema + 'static,
        B: Schema + 'static,
        A::Value: crate::value::IntoErased + crate::value::FromErased + Clone,
        B::Value: crate::value::IntoErased + crate::value::FromErased + Clone,
    {
        self.map2(col_a, col_b, |a, b| (a, b))
    }

    /// Mark columns as outputs.
    pub fn output(&mut self, cols: impl IntoIterator<Item = NodeId>) {
        self.outputs.extend(cols);
    }

    /// Mark a single column as output.
    pub fn output_col<S: Schema>(&mut self, col: Col<S>) {
        self.outputs.push(col.id);
    }

    /// Get the number of nodes.
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Check if the graph is empty.
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// Get a reference to a node by ID.
    pub fn get(&self, id: NodeId) -> Option<&Node> {
        self.nodes.get(id.0)
    }

    /// Get a mutable reference to a node by ID.
    pub fn get_mut(&mut self, id: NodeId) -> Option<&mut Node> {
        self.nodes.get_mut(id.0)
    }

    /// Get all output node IDs.
    pub fn outputs(&self) -> &[NodeId] {
        &self.outputs
    }

    /// Get the type tag for a node.
    pub fn type_tag(&self, id: NodeId) -> Option<TypeTag> {
        self.get(id).map(|node| match node {
            Node::Filter { input, .. } => {
                // Filter wraps input type in Option
                let input_type = self.type_tag(*input).unwrap_or(TypeTag::Bool);
                TypeTag::Option(Box::new(input_type))
            }
            _ => node.type_tag(),
        })
    }

    /// Iterate over all nodes with their IDs.
    pub fn iter(&self) -> impl Iterator<Item = (NodeId, &Node)> {
        self.nodes.iter().enumerate().map(|(i, n)| (NodeId(i), n))
    }
}

impl Default for Graph {
    fn default() -> Self {
        Self::new()
    }
}

/// IntoErased implementation for tuples.
impl<A, B> crate::value::IntoErased for (A, B)
where
    A: crate::value::IntoErased,
    B: crate::value::IntoErased,
{
    fn into_erased(self) -> ErasedValue {
        ErasedValue::Struct(vec![self.0.into_erased(), self.1.into_erased()])
    }
}

impl<A, B, C> crate::value::IntoErased for (A, B, C)
where
    A: crate::value::IntoErased,
    B: crate::value::IntoErased,
    C: crate::value::IntoErased,
{
    fn into_erased(self) -> ErasedValue {
        ErasedValue::Struct(vec![
            self.0.into_erased(),
            self.1.into_erased(),
            self.2.into_erased(),
        ])
    }
}

/// FromErased implementation for tuples.
impl<A, B> crate::value::FromErased for (A, B)
where
    A: crate::value::FromErased,
    B: crate::value::FromErased,
{
    fn from_erased(value: ErasedValue) -> Option<Self> {
        match value {
            ErasedValue::Struct(mut fields) if fields.len() == 2 => {
                let b = fields.pop()?;
                let a = fields.pop()?;
                Some((A::from_erased(a)?, B::from_erased(b)?))
            }
            _ => None,
        }
    }
}

impl<A, B, C> crate::value::FromErased for (A, B, C)
where
    A: crate::value::FromErased,
    B: crate::value::FromErased,
    C: crate::value::FromErased,
{
    fn from_erased(value: ErasedValue) -> Option<Self> {
        match value {
            ErasedValue::Struct(mut fields) if fields.len() == 3 => {
                let c = fields.pop()?;
                let b = fields.pop()?;
                let a = fields.pop()?;
                Some((A::from_erased(a)?, B::from_erased(b)?, C::from_erased(c)?))
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Constant, SFloat64, SInt64};

    #[test]
    fn test_graph_source() {
        let mut graph = Graph::new();
        let col: Col<SInt64> = graph.source(Constant::new(42i64));
        assert_eq!(col.id().index(), 0);
        assert_eq!(graph.len(), 1);
    }

    #[test]
    fn test_graph_map() {
        let mut graph = Graph::new();
        let a: Col<SInt64> = graph.source(Constant::new(10i64));
        let b: Col<SInt64> = graph.map(a, |x| x * 2);

        assert_eq!(graph.len(), 2);
        assert_eq!(b.id().index(), 1);

        let deps = graph.get(b.id()).unwrap().dependencies();
        assert_eq!(deps, vec![a.id()]);
    }

    #[test]
    fn test_graph_map2() {
        let mut graph = Graph::new();
        let a: Col<SInt64> = graph.source(Constant::new(10i64));
        let b: Col<SFloat64> = graph.source(Constant::new(2.5f64));
        let c: Col<SFloat64> = graph.map2(a, b, |x, y| x as f64 * y);

        assert_eq!(graph.len(), 3);
        let deps = graph.get(c.id()).unwrap().dependencies();
        assert_eq!(deps, vec![a.id(), b.id()]);
    }

    #[test]
    fn test_col_erase() {
        let mut graph = Graph::new();
        let col: Col<SInt64> = graph.source(Constant::new(42i64));
        let (id, tag) = col.erase();
        assert_eq!(id, col.id());
        assert_eq!(tag, TypeTag::Int64);
    }
}
