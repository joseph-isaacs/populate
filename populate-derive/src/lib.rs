use proc_macro::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Fields, parse_macro_input};

/// Derive macro for generating column schema information.
///
/// Generates:
/// - `FIELD_NAMES`: array of field name strings
/// - `Row`: tuple type matching field types
/// - `collect_columns`: method for column-by-column extraction
///
/// # Example
///
/// ```ignore
/// #[derive(ColumnSchema)]
/// struct Part {
///     key: i64,
///     name: String,
///     size: i64,
/// }
///
/// // Generates impl with:
/// // - Part::FIELD_NAMES = ["key", "name", "size"]
/// // - Part::Row = (i64, String, i64)
/// ```
#[proc_macro_derive(ColumnSchema)]
pub fn derive_column_schema(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    let fields = match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(fields) => &fields.named,
            _ => panic!("ColumnSchema only supports structs with named fields"),
        },
        _ => panic!("ColumnSchema only supports structs"),
    };

    let field_names: Vec<_> = fields
        .iter()
        .map(|f| f.ident.as_ref().unwrap().to_string())
        .collect();

    let field_types: Vec<_> = fields.iter().map(|f| &f.ty).collect();
    let field_idents: Vec<_> = fields.iter().map(|f| f.ident.as_ref().unwrap()).collect();
    let field_count = fields.len();

    // Generate tuple field accessors (0, 1, 2, ...)
    let indices: Vec<syn::Index> = (0..field_count).map(syn::Index::from).collect();

    // Generate a type alias name like "PartRow" from "Part"
    let row_type_name = syn::Ident::new(&format!("{}Row", name), name.span());

    let expanded = quote! {
        /// Tuple type matching the struct fields, for use with Distribution.
        pub type #row_type_name = (#(#field_types),*);

        impl #name {
            /// Field names as static strings for schema generation.
            pub const FIELD_NAMES: &'static [&'static str] = &[#(#field_names),*];

            /// Number of fields/columns.
            pub const FIELD_COUNT: usize = #field_count;
        }

        impl populate_core::ColumnSchema for #name {
            type Row = #row_type_name;

            fn field_names() -> &'static [&'static str] {
                Self::FIELD_NAMES
            }

            fn field_count() -> usize {
                Self::FIELD_COUNT
            }
        }

        impl From<#row_type_name> for #name {
            fn from(row: #row_type_name) -> Self {
                Self {
                    #(#field_idents: row.#indices),*
                }
            }
        }
    };

    TokenStream::from(expanded)
}
