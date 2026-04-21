use super::ast::{ParseTree, Rule};
use crate::errors::{Entity, SbroadError};
use crate::executor::engine::helpers::normalize_name_from_sql;
use crate::{CopyFormat, CopyFrom, CopyOptions, CopyStatement, CopyTableTarget, CopyTo};
use pest::iterators::Pair;
use pest::Parser;
use smol_str::{format_smolstr, SmolStr};

#[derive(Debug)]
pub enum ParsedCommand {
    Sql,
    Copy(CopyStatement),
}

pub fn parse_command(query: &str) -> Result<ParsedCommand, SbroadError> {
    let top_pair = parse_top_level_command(query)?;

    match top_pair.as_rule() {
        Rule::Copy => parse_copy(top_pair).map(ParsedCommand::Copy),
        _ => Ok(ParsedCommand::Sql),
    }
}

fn parse_top_level_command<'query>(query: &'query str) -> Result<Pair<'query, Rule>, SbroadError> {
    let mut command_pair = ParseTree::parse(Rule::Command, query)
        .map_err(|e| SbroadError::ParsingError(Entity::Rule, format_smolstr!("{e}")))?;
    Ok(command_pair
        .next()
        .expect("Query expected as a first parsing tree child."))
}

fn parse_copy(pair: Pair<'_, Rule>) -> Result<CopyStatement, SbroadError> {
    debug_assert_eq!(pair.as_rule(), Rule::Copy);

    let mut target = None;
    let mut columns = Vec::new();
    let mut direction = None;
    let mut endpoint = None;
    let mut options = CopyOptions::default();

    for child in pair.into_inner() {
        match child.as_rule() {
            Rule::CopyTableName => target = Some(parse_table_name(child.as_str())?),
            Rule::Identifier => columns.push(normalize_name_from_sql(child.as_str())),
            Rule::CopyDirection => {
                direction = Some(match child.as_str().to_ascii_lowercase().as_str() {
                    "from" => ParsedDirection::From,
                    "to" => ParsedDirection::To,
                    unexpected => {
                        return Err(SbroadError::ParsingError(
                            Entity::Query,
                            format_smolstr!("unsupported COPY direction: {unexpected}"),
                        ));
                    }
                });
            }
            Rule::CopyTarget => {
                endpoint = Some(match child.as_str().to_ascii_lowercase().as_str() {
                    "stdin" => ParsedEndpoint::Stdin,
                    "stdout" => ParsedEndpoint::Stdout,
                    unexpected => {
                        return Err(SbroadError::ParsingError(
                            Entity::Query,
                            format_smolstr!("unsupported COPY target: {unexpected}"),
                        ));
                    }
                });
            }
            Rule::CopyWithOptions => parse_with_options(&mut options, child)?,
            Rule::CopyLegacyDelimiter => options.delimiter = Some(parse_option_value(child)),
            Rule::CopyLegacyNull => options.null_string = Some(parse_option_value(child)),
            _ => {}
        }
    }

    let (schema_name, table_name) = target.ok_or_else(|| {
        SbroadError::Invalid(Entity::Query, Some("COPY table name is missing".into()))
    })?;
    let table = CopyTableTarget {
        schema_name,
        table_name,
        columns,
    };

    let direction = direction.ok_or_else(|| {
        SbroadError::Invalid(Entity::Query, Some("COPY direction is missing".into()))
    })?;
    let endpoint = endpoint.ok_or_else(|| {
        SbroadError::Invalid(Entity::Query, Some("COPY target is missing".into()))
    })?;

    match (direction, endpoint) {
        (ParsedDirection::From, ParsedEndpoint::Stdin) => {
            Ok(CopyStatement::From(CopyFrom { table, options }))
        }
        (ParsedDirection::To, ParsedEndpoint::Stdout) => {
            Ok(CopyStatement::To(CopyTo { table, options }))
        }
        (ParsedDirection::From, ParsedEndpoint::Stdout) => Err(SbroadError::Invalid(
            Entity::Query,
            Some("COPY FROM STDOUT is invalid".into()),
        )),
        (ParsedDirection::To, ParsedEndpoint::Stdin) => Err(SbroadError::Invalid(
            Entity::Query,
            Some("COPY TO STDIN is invalid".into()),
        )),
    }
}

fn parse_with_options(options: &mut CopyOptions, pair: Pair<'_, Rule>) -> Result<(), SbroadError> {
    for option in pair.into_inner() {
        match option.as_rule() {
            Rule::CopyFormatOption => {
                options.format = parse_copy_format(&parse_option_value(option))?;
            }
            Rule::CopyDelimiterOption => options.delimiter = Some(parse_option_value(option)),
            Rule::CopyNullOption => options.null_string = Some(parse_option_value(option)),
            Rule::CopyHeaderOption => {
                options.header = option
                    .into_inner()
                    .next()
                    .map(|value| value.as_str().eq_ignore_ascii_case("true"))
                    .unwrap_or(true);
            }
            Rule::CopyBatchSizeOption => {
                let raw = option
                    .into_inner()
                    .next()
                    .expect("COPY batch_size must have a value")
                    .as_str();
                let batch_size = raw.parse::<usize>().map_err(|e| {
                    SbroadError::Invalid(
                        Entity::Query,
                        Some(format_smolstr!("invalid COPY batch_size {raw}: {e}")),
                    )
                })?;
                options.batch_size = Some(batch_size);
            }
            _ => {}
        }
    }

    Ok(())
}

fn parse_copy_format(raw: &str) -> Result<CopyFormat, SbroadError> {
    match raw.to_ascii_lowercase().as_str() {
        "text" => Ok(CopyFormat::Text),
        "csv" => Ok(CopyFormat::Csv),
        "binary" => Ok(CopyFormat::Binary),
        unsupported => Err(SbroadError::Invalid(
            Entity::Query,
            Some(format_smolstr!("unsupported COPY format: {unsupported}")),
        )),
    }
}

fn parse_option_value(option: Pair<'_, Rule>) -> String {
    let value = option
        .into_inner()
        .next()
        .expect("COPY option must have a value");

    match value.as_rule() {
        Rule::Identifier => value.as_str().to_string(),
        Rule::SingleQuotedString => unquote_single_quoted(value.as_str()),
        _ => unreachable!("unexpected COPY option value rule"),
    }
}

fn unquote_single_quoted(raw: &str) -> String {
    raw[1..raw.len() - 1].replace("''", "'")
}

fn parse_table_name(raw: &str) -> Result<(Option<SmolStr>, SmolStr), SbroadError> {
    Ok(match split_top_level_dot(raw) {
        Some(dot_idx) => (
            Some(normalize_name_from_sql(&raw[..dot_idx])),
            normalize_name_from_sql(&raw[dot_idx + 1..]),
        ),
        None => (None, normalize_name_from_sql(raw)),
    })
}

fn split_top_level_dot(name: &str) -> Option<usize> {
    let bytes = name.as_bytes();
    let mut idx = 0usize;
    let mut in_quotes = false;

    while idx < bytes.len() {
        match bytes[idx] {
            b'"' => {
                if in_quotes && bytes.get(idx + 1) == Some(&b'"') {
                    idx += 2;
                    continue;
                }
                in_quotes = !in_quotes;
            }
            b'.' if !in_quotes => return Some(idx),
            _ => {}
        }
        idx += 1;
    }

    None
}

#[derive(Clone, Copy)]
enum ParsedDirection {
    From,
    To,
}

#[derive(Clone, Copy)]
enum ParsedEndpoint {
    Stdin,
    Stdout,
}

#[cfg(test)]
mod tests {
    use super::{parse_command, ParsedCommand};
    use crate::{CopyFormat, CopyFrom, CopyOptions, CopyStatement, CopyTableTarget};

    fn parse_copy(query: &str) -> CopyStatement {
        match parse_command(query).expect("parse command") {
            ParsedCommand::Copy(copy) => copy,
            ParsedCommand::Sql => panic!("expected COPY command"),
        }
    }

    #[test]
    fn parses_copy_from_stdin() {
        let parsed = parse_copy(
            r#"COPY "t" ("id", "value") FROM STDIN WITH (DELIMITER '|', NULL 'nil', HEADER true)"#,
        );

        assert_eq!(
            parsed,
            CopyStatement::From(CopyFrom {
                table: CopyTableTarget {
                    schema_name: None,
                    table_name: "t".into(),
                    columns: vec!["id".into(), "value".into()],
                },
                options: CopyOptions {
                    format: CopyFormat::Text,
                    delimiter: Some("|".into()),
                    null_string: Some("nil".into()),
                    header: true,
                    batch_size: None,
                },
            })
        );
    }

    #[test]
    fn parses_copy_batch_size_option() {
        let parsed = parse_copy(r#"COPY "t" FROM STDIN WITH (BATCH_SIZE = 2)"#);

        assert_eq!(
            parsed,
            CopyStatement::From(CopyFrom {
                table: CopyTableTarget {
                    schema_name: None,
                    table_name: "t".into(),
                    columns: vec![],
                },
                options: CopyOptions {
                    batch_size: Some(2),
                    ..CopyOptions::default()
                },
            })
        );
    }

    #[test]
    fn classifies_non_copy_as_sql() {
        assert!(matches!(
            parse_command("SELECT 1").unwrap(),
            ParsedCommand::Sql
        ));
    }

    #[test]
    fn normalizes_schema_qualified_copy_target() {
        let parsed = parse_copy(r#"COPY public."Mixed Table" ("Mixed Column") FROM STDIN"#);

        assert_eq!(
            parsed,
            CopyStatement::From(CopyFrom {
                table: CopyTableTarget {
                    schema_name: Some("public".into()),
                    table_name: "Mixed Table".into(),
                    columns: vec!["Mixed Column".into()],
                },
                options: CopyOptions::default(),
            })
        );
    }
}
