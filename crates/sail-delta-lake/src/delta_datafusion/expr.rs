use datafusion::common::DFSchema;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::Expr;
use datafusion::sql::planner::{ContextProvider, SqlToRel};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion::sql::sqlparser::tokenizer::Tokenizer;
use deltalake::errors::{DeltaResult, DeltaTableError};
use deltalake::kernel::Add;
use deltalake::logstore::{LogStore, LogStoreRef};
use object_store::ObjectMeta;
use serde::{Deserialize, Serialize};
use url::Url;

use crate::delta_datafusion::DeltaContextProvider;

/// Parse a string predicate into a DataFusion `Expr`
pub fn parse_predicate_expression(
    schema: &DFSchema,
    expr: impl AsRef<str>,
    state: &SessionState,
) -> DeltaResult<Expr> {
    let dialect = &GenericDialect {};
    let mut tokenizer = Tokenizer::new(dialect, expr.as_ref());
    let tokens = tokenizer
        .tokenize()
        .map_err(|err| DeltaTableError::Generic(format!("Failed to tokenize expression: {err}")))?;

    let sql = Parser::new(dialect)
        .with_tokens(tokens)
        .parse_expr()
        .map_err(|err| DeltaTableError::Generic(format!("Failed to parse expression: {err}")))?;

    let context_provider = DeltaContextProvider::new(state);
    let sql_to_rel = SqlToRel::new(&context_provider);

    sql_to_rel
        .sql_to_expr(sql, schema, &mut Default::default())
        .map_err(|err| {
            DeltaTableError::Generic(format!("Failed to convert SQL to expression: {err}"))
        })
}
